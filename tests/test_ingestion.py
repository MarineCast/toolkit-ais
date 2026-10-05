"""Synthetic local files only; native candidates are not qualified vessel activity."""

import csv
import json
import tracemalloc
from dataclasses import asdict, replace
from datetime import timedelta

import pyarrow.parquet as pq
import pytest
import zstandard as zstd
from test_foundation import CURRENT_HEADERS, LEGACY_HEADERS, START, row

from ais_toolkit.cli import main
from ais_toolkit.ingestion import (
    IngestionConfig,
    LocalInput,
    SpatialFilter,
    digest,
    ingest,
    load_job,
    verified_receipt,
)
from ais_toolkit.sources import SourcePartition


def config(**changes):
    base = IngestionConfig(
        "synthetic-day",
        START,
        START + timedelta(days=1),
        START - timedelta(minutes=1),
        START + timedelta(days=1, minutes=1),
        SpatialFilter("synthetic-bounds", 19, 9, 21, 11, 0.1),
        "reject",
        7,
        20000,
        10000000,
        10000,
        1024,
        8192,
        "e" * 40,
    )
    return replace(base, **changes)


def local(tmp_path, name, rows, compressed=False, era="2025+"):
    path = tmp_path / f"{name}.csv"
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(
            stream, fieldnames=CURRENT_HEADERS if era == "2025+" else LEGACY_HEADERS
        )
        writer.writeheader()
        writer.writerows(rows)
    if compressed:
        packed = path.with_suffix(".csv.zst")
        packed.write_bytes(zstd.ZstdCompressor(write_checksum=True).compress(path.read_bytes()))
        path = packed
    receipt = SourcePartition(
        "noaa_marinecadastre",
        name,
        digest(path),
        START,
        START - timedelta(minutes=2),
        START + timedelta(days=2),
        era,
        True,
        "synthetic rights fixture",
        "synthetic support fixture",
        "available",
    )
    return LocalInput(path, receipt)


def table(directory, name):
    return pq.read_table(directory / f"{name}.parquet").to_pylist()


def test_cross_file_duplicate_conflict_exact_time_and_input_order(tmp_path):
    a = local(
        tmp_path,
        "a",
        [
            row(),
            row(base_date_time="2025-01-01T00:00:30Z"),
            row(base_date_time="2025-01-01T00:01:00Z"),
        ],
    )
    b = local(
        tmp_path,
        "b",
        [
            row(),
            row(base_date_time="2025-01-01T00:01:00Z", longitude="20.2"),
            row(latitude="91"),
            row(base_date_time="2025-01-01T00:02:00Z", longitude="25"),
        ],
        compressed=True,
    )
    first = ingest([a, b], config(), tmp_path / "one")
    second = ingest([b, a], config(), tmp_path / "two")
    report = verified_receipt(first)
    assert report == verified_receipt(second)
    assert report["counts"] == {"accepted": 2, "duplicate": 1, "filtered": 1, "rejected": 3}
    assert report["input_rows"] == 7
    assert len(table(first, "positions")) == 2  # Same-minute distinct seconds are distinct reports.
    assert len(table(first, "audit")) == 7
    assert table(first, "state")[0]["barrier_seen"]
    assert any(
        "simultaneous_identity_position_conflict" in r["reasons"] for r in table(first, "audit")
    )
    assert all(
        r["source_sha256"] in (a.receipt.sha256, b.receipt.sha256) for r in table(first, "audit")
    )
    assert ingest([b, a], config(), tmp_path / "one") == first  # Verified retry.
    (first / "positions.parquet").write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="checksum mismatch"):
        ingest([a, b], config(), tmp_path / "one")


def test_midnight_halos_and_carried_rejection_barriers(tmp_path):
    before = row(base_date_time="2024-12-31T23:59:30Z")  # Wrong era: barrier, never invented fix.
    a = local(
        tmp_path,
        "day-one",
        [
            before,
            row(),
            row(base_date_time="2025-01-01T23:59:30Z"),
            row(base_date_time="2025-01-02T00:00:00Z"),
        ],
    )
    first = ingest([a], config(), tmp_path / "first")
    assert len(table(first, "positions")) == 2
    assert len(table(first, "halo")) == 1
    assert table(first, "state")[0]["barrier_seen"]
    b = local(
        tmp_path,
        "day-two",
        [row(base_date_time="2025-01-02T00:00:00Z"), row(base_date_time="2025-01-02T00:00:00Z")],
        compressed=True,
    )
    next_config = config(
        partition_id="synthetic-next-day",
        core_start=START + timedelta(days=1),
        core_end=START + timedelta(days=2),
        halo_start=START + timedelta(days=1, minutes=-1),
        halo_end=START + timedelta(days=2, minutes=1),
    )
    second = ingest([b], next_config, tmp_path / "second", first)
    assert len(table(second, "positions")) == 1
    assert table(second, "state")[0]["barrier_seen"]
    assert json.loads(table(second, "state")[0]["last_point_json"])["timestamp"].startswith(
        "2025-01-02"
    )
    assert verified_receipt(second)["carried_run_key"] == first.name
    assert ingest([b], next_config, tmp_path / "second", first) == second
    with pytest.raises(ValueError, match="adjacent"):
        ingest([b], config(), tmp_path / "bad-carry", first)


def test_era_boundary_and_mixed_schema_quarantine(tmp_path):
    old = local(
        tmp_path, "legacy", [row("2018-2024", BaseDateTime="2024-12-31T23:59:30Z")], era="2018-2024"
    )
    current = local(tmp_path, "current", [row()], compressed=True)
    result = ingest([current, old], config(), tmp_path / "done")
    assert len(table(result, "positions")) == 1 and len(table(result, "halo")) == 1
    assert {r["source_era"] for r in table(result, "audit")} == {"2018-2024", "2025+"}
    wrong = replace(current, receipt=replace(current.receipt, source_era="2018-2024"))
    with pytest.raises(ValueError, match="mixed/unknown schema"):
        ingest([wrong], config(), tmp_path / "wrong")


@pytest.mark.parametrize("kind", ("truncated", "extra", "width", "quote", "empty", "cap"))
def test_failed_partitions_never_have_completion_marker(tmp_path, kind):
    item = local(tmp_path, "bad", [row()], compressed=kind in ("truncated", "extra"))
    if kind == "truncated":
        item.path.write_bytes(item.path.read_bytes()[:-3])
    elif kind == "extra":
        item.path.write_bytes(item.path.read_bytes() + b"extra")
    elif kind == "width":
        item.path.write_text(",".join(CURRENT_HEADERS) + "\n1,2\n")
    elif kind == "quote":
        item.path.write_text(",".join(CURRENT_HEADERS) + '\n"unclosed\n')
    elif kind == "empty":
        item.path.write_text(",".join(CURRENT_HEADERS) + "\n")
    item = replace(item, receipt=replace(item.receipt, sha256=digest(item.path)))
    chosen = config(max_decoded_bytes=10) if kind == "cap" else config()
    output = tmp_path / "outputs"
    with pytest.raises((ValueError, csv.Error, zstd.ZstdError)):
        ingest([item], chosen, output)
    assert not list(output.glob("*/complete.json"))
    failures = list(output.glob(".staging-*/failure.json"))
    assert len(failures) == 1 and json.loads(failures[0].read_text())["status"] == "failed"


def test_source_bounds_filter_buffer_caps_and_receipt_gate(tmp_path):
    item = local(
        tmp_path,
        "bounds",
        [
            row(longitude="21.05"),
            row(base_date_time="2025-01-01T00:00:15Z", longitude="21.2"),
            row(base_date_time="2025-01-01T00:00:30Z"),
        ],
    )
    item = replace(item, receipt=replace(item.receipt, end=START + timedelta(seconds=20)))
    result = ingest([item], config(), tmp_path / "result")
    assert verified_receipt(result)["counts"] == {"accepted": 1, "filtered": 1, "rejected": 1}
    with pytest.raises(ValueError, match="record cap"):
        ingest([item], config(max_records=1), tmp_path / "too-many")
    with pytest.raises(ValueError, match="receipt checksum"):
        ingest(
            [replace(item, receipt=replace(item.receipt, sha256="f" * 64))],
            config(),
            tmp_path / "hash",
        )
    with pytest.raises(ValueError, match="provider unavailable"):
        ingest(
            [replace(item, receipt=replace(item.receipt, provider="canada_configurable"))],
            config(),
            tmp_path / "ca",
        )


def test_conflict_outside_spatial_filter_still_blocks_inside_report(tmp_path):
    item = local(tmp_path, "conflict", [row(), row(longitude="25")])
    result = ingest([item], config(), tmp_path / "done")
    assert verified_receipt(result)["counts"] == {"rejected": 2}
    assert not table(result, "positions")
    assert table(result, "state")[0]["barrier_seen"]


def test_multiline_provenance_and_configured_record_length_cap(tmp_path):
    item = local(tmp_path, "lines", [row(vessel_name="SYNTHETIC\nNAME"), row(latitude="91")])
    result = ingest([item], config(), tmp_path / "done")
    starts = sorted(r["row_start"] for r in table(result, "audit"))
    assert starts == [2, 4]
    assert any(r["row_start"] == 2 and r["row_end"] == 3 for r in table(result, "audit"))
    with pytest.raises(ValueError, match="character cap"):
        ingest([item], config(max_record_characters=20), tmp_path / "small")


def test_bounded_batches_and_measured_python_allocation(tmp_path):
    rows = (row(base_date_time=(START + timedelta(seconds=i)).isoformat()) for i in range(3000))
    item = local(tmp_path, "many", rows)
    tracemalloc.start()
    result = ingest([item], config(batch_rows=17), tmp_path / "done")
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    report = verified_receipt(result)
    assert report["input_rows"] == 3000 and report["peak_writer_rows"] <= 17
    assert report["peak_arrow_table_bytes"] < 100000
    assert report["sqlite_spill_bytes"] > 100000
    assert peak < 8000000  # Measured Python allocations; excludes native caches/process RSS.
    assert (
        sum(
            pq.ParquetFile(result / "positions.parquet").metadata.row_group(i).num_rows
            for i in range(pq.ParquetFile(result / "positions.parquet").num_row_groups)
        )
        == 3000
    )


def test_cli_explicit_job_paths_strict_config_and_idempotence(tmp_path, capsys):
    item = local(tmp_path, "cli", [row()], compressed=True)
    values = asdict(config())
    values = {k: v.isoformat() if hasattr(v, "isoformat") else v for k, v in values.items()}
    receipt = {
        k: v.isoformat() if hasattr(v, "isoformat") else v for k, v in asdict(item.receipt).items()
    }
    path = tmp_path / "job.json"
    path.write_text(
        json.dumps({"config": values, "inputs": [{"path": item.path.name, "receipt": receipt}]})
    )
    loaded, configured = load_job(path)
    assert loaded[0].path == item.path and configured == config()
    assert main(["ingest-local", str(path), str(tmp_path / "out")]) == 0
    first = json.loads(capsys.readouterr().out)["native_directory"]
    assert main(["ingest-local", str(path), str(tmp_path / "out")]) == 0
    assert json.loads(capsys.readouterr().out)["native_directory"] == first
    path.write_text('{"config": {}, "config": {}, "inputs": []}')
    assert main(["ingest-local", str(path), str(tmp_path / "bad")]) == 2
