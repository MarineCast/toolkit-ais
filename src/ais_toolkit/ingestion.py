"""Offline local ingestion with disk-spilled reconciliation; no track inference."""

import csv
import hashlib
import json
import os
import sqlite3
import tempfile
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterator

import pyarrow as pa
import pyarrow.parquet as pq
import zstandard as zstd

from .adapters import ADAPTER_VERSION, DICTIONARY_VERSION, FIELD_MAPS, MarineCadastreAdapter
from .contracts import Provenance, utc
from .processing import ProcessingManifest
from .sources import SourcePartition

METHOD = "offline-ingestion/0.3"
SCHEMA_VERSION = "ais-ingestion/0.1"


def canonical(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def fingerprint(value) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def strict_json(path: Path):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON key")
            result[key] = value
        return result

    return json.loads(
        path.read_text(),
        object_pairs_hook=unique,
        parse_constant=lambda _: (_ for _ in ()).throw(ValueError("nonfinite JSON")),
    )


@dataclass(frozen=True)
class SpatialFilter:
    """Explicit buffered rectangular candidate filter in degrees, not water/H3 geometry."""

    domain_reference: str
    west: float
    south: float
    east: float
    north: float
    buffer_degrees: float

    def __post_init__(self):
        if not self.domain_reference or not -180 <= self.west < self.east <= 180:
            raise ValueError("explicit domain reference and increasing longitude bounds required")
        if not -90 <= self.south < self.north <= 90 or not 0 <= self.buffer_degrees <= 10:
            raise ValueError("invalid latitude bounds/buffer; antimeridian wrapping unsupported")

    def contains(self, lon, lat):
        b = self.buffer_degrees
        return self.west - b <= lon <= self.east + b and self.south - b <= lat <= self.north + b


@dataclass(frozen=True)
class IngestionConfig:
    partition_id: str
    core_start: datetime
    core_end: datetime
    halo_start: datetime
    halo_end: datetime
    spatial: SpatialFilter
    naive_time_policy: str
    batch_rows: int
    max_records: int
    max_decoded_bytes: int
    max_record_characters: int
    sqlite_cache_kib: int
    zstd_window_kib: int
    producer_git_sha: str

    def __post_init__(self):
        for key in ("core_start", "core_end", "halo_start", "halo_end"):
            object.__setattr__(self, key, utc(getattr(self, key)))
        if not self.halo_start <= self.core_start < self.core_end <= self.halo_end:
            raise ValueError("halo must contain positive core window")
        if self.naive_time_policy not in ("reject", "dictionary_utc"):
            raise ValueError("explicit timestamp policy required")
        for key in (
            "batch_rows",
            "max_records",
            "max_decoded_bytes",
            "max_record_characters",
            "sqlite_cache_kib",
            "zstd_window_kib",
        ):
            if not isinstance(getattr(self, key), int) or getattr(self, key) <= 0:
                raise ValueError(f"positive integer {key} required")
        if self.sqlite_cache_kib > 65536 or self.zstd_window_kib > 65536:
            raise ValueError("foundation memory caches/windows limited to 64 MiB each")

    @property
    def policy_hash(self):
        return fingerprint(
            {"spatial": asdict(self.spatial), "time": self.naive_time_policy, "method": METHOD}
        )


@dataclass(frozen=True)
class LocalInput:
    path: Path
    receipt: SourcePartition


def materialize(item: LocalInput, stage: Path, config: IngestionConfig) -> Path:
    """Bounded decode to spill disk; single standard zstd frame, checked through EOF.

    Small compressed chunks bound decompressobj's collected output. Decoder window
    and total decoded bytes are separately capped. Concatenated/skippable frames are
    rejected rather than silently ignored. No national performance claim.
    """
    target = stage / f"{fingerprint(item.receipt.asset_id)}.csv"
    total = 0
    read_hash = hashlib.sha256()
    # The supported C/CFFI implementations pass this value directly to
    # ZSTD_DCtx_setMaxWindowSize in bytes, despite the upstream KiB docstring.
    # Keep our configuration in KiB and the effective bound at <=64 MiB.
    decoder = zstd.ZstdDecompressor(max_window_size=config.zstd_window_kib * 1024).decompressobj()
    compressed = item.path.name.endswith(".csv.zst")
    if not compressed and item.path.suffix != ".csv":
        raise ValueError("local input must be CSV or single-frame CSV.zst")
    with item.path.open("rb") as source, target.open("xb") as output:
        while data := source.read(64 if compressed else 65536):
            read_hash.update(data)
            if compressed:
                if decoder.eof:
                    raise ValueError("extra data after compressed frame")
                data = decoder.decompress(data)
                if decoder.unused_data:
                    raise ValueError("extra/concatenated compressed frames unsupported")
            total += len(data)
            if total > config.max_decoded_bytes:
                raise ValueError("decoded byte cap exceeded")
            output.write(data)
    if compressed and not decoder.eof:
        raise ValueError("incomplete/truncated compressed partition")
    if read_hash.hexdigest() != item.receipt.sha256 or digest(item.path) != item.receipt.sha256:
        raise ValueError("source changed during inspection")
    return target


def records(
    path: Path, limit: int, adapter: MarineCadastreAdapter
) -> Iterator[tuple[int, int, dict[str, str]]]:
    """Read full CSV framing; project retained columns only after schema validation.

    Physical line and complete record limits bound CSV buffering, including multiline
    records; no claim of avoiding the parsing of unused CSV bytes.
    """
    with path.open(newline="", encoding="utf-8") as stream:
        record_chars = 0

        def lines():
            nonlocal record_chars
            while line := stream.readline(limit + 1):
                record_chars += len(line)
                if record_chars > limit:
                    raise ValueError("record character cap exceeded")
                yield line

        reader = csv.reader(lines(), strict=True)
        headers = next(reader, [])
        adapter.validate_headers(headers)
        record_chars = 0
        while True:
            start = reader.line_num + 1
            values = next(reader, None)
            record_chars = 0
            if values is None:
                break
            if not values:
                continue
            if len(values) != len(headers):
                raise ValueError(f"malformed CSV width at physical line {start}")
            yield start, reader.line_num, dict(zip(headers, values, strict=True))


def retained_signature(row: dict) -> str:
    return fingerprint(
        {
            k: row[k]
            for k in (
                "longitude",
                "latitude",
                "sog_knots",
                "raw_vessel_code",
                "equipment_class",
                "imo",
                "quality_flags",
            )
        }
    )


SCHEMA = pa.schema(
    [
        ("record_key", pa.string()),
        ("provider", pa.string()),
        ("asset_id", pa.string()),
        ("row_start", pa.int64()),
        ("row_end", pa.int64()),
        ("source_sha256", pa.string()),
        ("source_era", pa.string()),
        ("mmsi", pa.string()),
        ("timestamp", pa.timestamp("us", tz="UTC")),
        ("longitude", pa.float64()),
        ("latitude", pa.float64()),
        ("sog_knots", pa.float64()),
        ("raw_vessel_code", pa.string()),
        ("equipment_class", pa.string()),
        ("imo", pa.string()),
        ("quality_flags", pa.string()),
        ("raw_row_sha256", pa.string()),
        ("outcome", pa.string()),
        ("reasons", pa.string()),
        ("scope", pa.string()),
        ("identity_status", pa.string()),
    ]
)
STATE_SCHEMA = pa.schema(
    [("mmsi", pa.string()), ("last_point_json", pa.string()), ("barrier_seen", pa.bool_())]
)
SCHEMA = SCHEMA.with_metadata(
    {
        b"ais_schema_version": SCHEMA_VERSION.encode(),
        b"adapter_version": ADAPTER_VERSION.encode(),
        b"dictionary_version": DICTIONARY_VERSION.encode(),
    }
)
STATE_SCHEMA = STATE_SCHEMA.with_metadata({b"ais_schema_version": SCHEMA_VERSION.encode()})


class BatchWriter:
    def __init__(self, path, schema, batch_rows):
        self.writer = pq.ParquetWriter(path, schema, compression="zstd")
        self.schema, self.batch_rows, self.pending, self.peak = schema, batch_rows, [], 0
        self.closed = False
        self.peak_table_bytes = 0

    def append(self, row):
        self.pending.append(row)
        self.peak = max(self.peak, len(self.pending))
        if len(self.pending) == self.batch_rows:
            self.flush()

    def flush(self):
        if self.pending:
            table = pa.Table.from_pylist(self.pending, schema=self.schema)
            self.peak_table_bytes = max(self.peak_table_bytes, table.nbytes)
            self.writer.write_table(table)
            self.pending.clear()

    def close(self):
        if not self.closed:
            self.flush()
            self.writer.close()
            self.closed = True


def verified_receipt(
    directory: Path,
    *,
    method=METHOD,
    schema_version=SCHEMA_VERSION,
    artifact_names=("positions.parquet", "halo.parquet", "audit.parquet", "state.parquet"),
) -> dict:
    if directory.name.startswith(".staging-") or (directory / "failure.json").exists():
        raise ValueError("failed/unpublished ingestion directory")
    receipt = strict_json(directory / "complete.json")
    if (
        receipt["status"] != "complete"
        or receipt["method"] != method
        or receipt["schema_version"] != schema_version
    ):
        raise ValueError("incomplete/incompatible ingestion receipt")
    if directory.name != receipt["run_key"]:
        raise ValueError("unpublished ingestion directory identity")
    for name, expected in receipt["artifacts"].items():
        if name not in artifact_names:
            raise ValueError("unexpected artifact name")
        if digest(directory / name) != expected:
            raise ValueError("completed artifact checksum mismatch")
    if set(receipt["artifacts"]) != set(artifact_names):
        raise ValueError("incomplete artifact receipt")
    return receipt


def publish_result(stage: Path, final: Path, result: dict) -> None:
    """Closed/checksummed artifacts become visible inside the private output root by rename."""
    (stage / "complete.json").write_text(canonical(result))
    os.chmod(stage / "complete.json", 0o600)
    os.rename(stage, final)


def mark_failed(stage: Path, error: Exception, method: str) -> None:
    (stage / "complete.json").unlink(missing_ok=True)
    (stage / "failure.json").write_text(
        canonical(
            {
                "status": "failed",
                "method": method,
                "error_type": type(error).__name__,
                "reason": str(error),
            }
        )
    )
    os.chmod(stage / "failure.json", 0o600)


def ingest(
    inputs: list[LocalInput],
    config: IngestionConfig,
    output_root: Path,
    carried_directory: Path | None = None,
) -> Path:
    """Emit immutable native candidates/audit/state atomically; never tracks or H3.

    Python holds only one raw CSV record, one projected position and bounded writer
    batches. SQLite temp_store=FILE handles sorting/groups on spill disk.
    """
    if not inputs or len({i.receipt.asset_id for i in inputs}) != len(inputs):
        raise ValueError("nonempty inputs with unique source asset IDs required")
    for item in inputs:
        item.receipt.require_normalization()
        if digest(item.path) != item.receipt.sha256:
            raise ValueError("source receipt checksum mismatch")
    previous = verified_receipt(carried_directory) if carried_directory else None
    if previous and (
        previous["policy_hash"] != config.policy_hash
        or datetime.fromisoformat(previous["core_end"]) != config.core_start
    ):
        raise ValueError("carried state requires identical policy and adjacent core windows")
    carry_hash = digest(carried_directory / "complete.json") if carried_directory else None
    effective = asdict(config)
    effective = {k: v.isoformat() if isinstance(v, datetime) else v for k, v in effective.items()}
    manifest = ProcessingManifest(
        config.partition_id,
        tuple(i.receipt.sha256 for i in inputs),
        fingerprint(effective),
        METHOD,
        SCHEMA_VERSION,
        config.core_start,
        config.core_end,
        config.halo_start,
        config.halo_end,
        carry_hash,
        config.producer_git_sha,
    )
    # Receipts (including asset identities/rights/coverage) also affect reuse, not only bytes.
    receipts = [
        {k: v.isoformat() if isinstance(v, datetime) else v for k, v in asdict(i.receipt).items()}
        for i in sorted(inputs, key=lambda i: i.receipt.asset_id)
    ]
    key = fingerprint({"processing": manifest.idempotency_key, "receipts": receipts})
    output_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    final = output_root / key
    if final.exists():
        saved = verified_receipt(final)
        if saved["run_key"] != key:
            raise ValueError("existing output identity mismatch")
        return final
    stage = Path(tempfile.mkdtemp(prefix=".staging-", dir=output_root))
    os.chmod(stage, 0o700)
    db = sqlite3.connect(stage / "spill.sqlite")
    writers = {}
    try:
        db.execute(f"PRAGMA cache_size=-{config.sqlite_cache_kib}")
        db.execute("PRAGMA temp_store=FILE")
        db.execute(
            "CREATE TABLE records (key TEXT PRIMARY KEY,mmsi TEXT,time TEXT,"
            "signature TEXT,outcome TEXT,scope TEXT,payload TEXT)"
        )
        db.execute("CREATE TABLE state (mmsi TEXT PRIMARY KEY,point TEXT,barrier INTEGER)")
        db.execute(
            "CREATE TABLE carried (mmsi TEXT PRIMARY KEY,time TEXT,signature TEXT,record_key TEXT)"
        )
        if previous:
            for batch in pq.ParquetFile(carried_directory / "state.parquet").iter_batches(
                batch_size=config.batch_rows
            ):
                for entry in batch.to_pylist():
                    db.execute(
                        "INSERT INTO state VALUES (?,?,?)",
                        (entry["mmsi"], entry["last_point_json"], entry["barrier_seen"]),
                    )
                    if entry["last_point_json"]:
                        point = json.loads(entry["last_point_json"])
                        db.execute(
                            "INSERT INTO carried VALUES (?,?,?,?)",
                            (
                                entry["mmsi"],
                                point["timestamp"],
                                retained_signature(point),
                                point["record_key"],
                            ),
                        )
        total = 0
        for item in sorted(inputs, key=lambda i: i.receipt.asset_id):
            adapter = MarineCadastreAdapter(item.receipt.source_era, config.naive_time_policy)
            path = materialize(item, stage, config)
            rows = records(path, config.max_record_characters, adapter)
            source_rows = 0
            for start, end, raw in rows:
                total += 1
                source_rows += 1
                if total > config.max_records:
                    raise ValueError("input record cap exceeded")
                provenance = Provenance(
                    item.receipt.provider,
                    item.receipt.source_era,
                    item.receipt.asset_id,
                    start,
                    DICTIONARY_VERSION,
                    ADAPTER_VERSION,
                    item.receipt.sha256,
                )
                parsed = adapter.parse(raw, provenance)
                position = parsed.position
                reason = list(parsed.rejection_reasons)
                if position and not item.receipt.start <= position.timestamp < item.receipt.end:
                    reason.append("outside_declared_source_window")
                fields = FIELD_MAPS[item.receipt.source_era]
                mmsi = position.mmsi if position else raw[fields["mmsi"]].strip()
                valid_identity = len(mmsi) == 9 and mmsi.isascii() and mmsi.isdigit()
                row = {
                    "record_key": f"{item.receipt.asset_id}:{start:020d}",
                    "provider": item.receipt.provider,
                    "asset_id": item.receipt.asset_id,
                    "row_start": start,
                    "row_end": end,
                    "source_sha256": item.receipt.sha256,
                    "source_era": item.receipt.source_era,
                    "mmsi": mmsi,
                    "timestamp": position.timestamp.isoformat(timespec="microseconds")
                    if position
                    else None,
                    "longitude": position.longitude if position else None,
                    "latitude": position.latitude if position else None,
                    "sog_knots": position.sog_knots if position else None,
                    "raw_vessel_code": position.raw_vessel_code if position else None,
                    "equipment_class": position.equipment_class if position else None,
                    "imo": position.imo if position else None,
                    "quality_flags": canonical(position.quality_flags if position else ()),
                    "raw_row_sha256": fingerprint(raw),
                    "reasons": canonical(reason),
                    "identity_status": "source_mmsi_unresolved",
                }
                scope = "unknown"
                outcome = "rejected" if reason else "candidate"
                if not reason:
                    inside = config.halo_start <= position.timestamp < config.halo_end
                    scope = (
                        "core"
                        if config.core_start <= position.timestamp < config.core_end
                        else "halo"
                    )
                    if not inside or not config.spatial.contains(
                        position.longitude, position.latitude
                    ):
                        scope, outcome = "outside", "filtered"
                        row["reasons"] = canonical(["outside_candidate_filter"])
                signature = retained_signature(row)
                row.update(outcome=outcome, scope=scope)
                db.execute(
                    "INSERT INTO records VALUES (?,?,?,?,?,?,?)",
                    (
                        row["record_key"],
                        mmsi if valid_identity else None,
                        row["timestamp"],
                        signature,
                        outcome,
                        scope,
                        canonical(row),
                    ),
                )
            if not source_rows:
                raise ValueError("empty source partition is unavailable, not observed zero")
            db.commit()
        db.execute("CREATE INDEX event_order ON records(mmsi,time,key)")
        db.execute(
            "CREATE TABLE groups AS SELECT mmsi,time,count(DISTINCT signature) variants,"
            "min(key) keeper FROM records WHERE outcome IN ('candidate','filtered') GROUP BY mmsi,time"
        )
        db.execute("CREATE INDEX group_order ON groups(mmsi,time)")
        for name in ("audit", "positions", "halo"):
            writers[name] = BatchWriter(stage / f"{name}.parquet", SCHEMA, config.batch_rows)
        counts = Counter()
        cursor = db.execute(
            "SELECT r.payload,g.variants,g.keeper,c.record_key FROM records r LEFT JOIN groups g "
            "ON r.mmsi=g.mmsi AND r.time=g.time LEFT JOIN carried c "
            "ON r.mmsi=c.mmsi AND r.time=c.time AND r.signature<>c.signature "
            "ORDER BY r.mmsi,r.time,r.key"
        )
        while batch := cursor.fetchmany(config.batch_rows):
            for payload, variants, keeper, carried_conflict in batch:
                row = json.loads(payload)
                if row["outcome"] in ("candidate", "filtered") and (
                    variants > 1 or carried_conflict
                ):
                    row["outcome"] = "rejected"
                    row["reasons"] = canonical(
                        json.loads(row["reasons"])
                        + ["simultaneous_identity_position_conflict"]
                        + (
                            [f"carried_endpoint_conflict:{carried_conflict}"]
                            if carried_conflict
                            else []
                        )
                    )
                elif row["outcome"] == "candidate":
                    row["outcome"] = "accepted" if row["record_key"] == keeper else "duplicate"
                counts[row["outcome"]] += 1
                when = datetime.fromisoformat(row["timestamp"]) if row["timestamp"] else None
                mmsi = row["mmsi"]
                if len(mmsi) == 9 and mmsi.isascii() and mmsi.isdigit():
                    db.execute("INSERT OR IGNORE INTO state VALUES (?,NULL,0)", (mmsi,))
                    if carried_conflict:
                        # Preserve the conflict barrier, but retire the contradictory endpoint.
                        db.execute("UPDATE state SET point=NULL,barrier=1 WHERE mmsi=?", (mmsi,))
                    if row["outcome"] == "rejected":
                        db.execute("UPDATE state SET barrier=1 WHERE mmsi=?", (mmsi,))
                    elif row["outcome"] == "accepted" and when < config.core_end:
                        old = db.execute(
                            "SELECT point FROM state WHERE mmsi=?", (mmsi,)
                        ).fetchone()[0]
                        if not old or json.loads(old)["timestamp"] < row["timestamp"]:
                            db.execute(
                                "UPDATE state SET point=? WHERE mmsi=?", (canonical(row), mmsi)
                            )
                row["timestamp"] = when
                writers["audit"].append(row)
                if row["outcome"] == "accepted":
                    writers["positions" if row["scope"] == "core" else "halo"].append(row)
        writers["state"] = BatchWriter(stage / "state.parquet", STATE_SCHEMA, config.batch_rows)
        cursor = db.execute("SELECT mmsi,point,barrier FROM state ORDER BY mmsi")
        while batch := cursor.fetchmany(config.batch_rows):
            for mmsi, point, barrier in batch:
                writers["state"].append(
                    {"mmsi": mmsi, "last_point_json": point, "barrier_seen": bool(barrier)}
                )
        for writer in writers.values():
            writer.close()
        if sum(counts.values()) != total:
            raise ValueError("row reconciliation failed")
        db.commit()
        db.close()
        for file in stage.iterdir():
            os.chmod(file, 0o600)
        result = {
            "status": "complete",
            "method": METHOD,
            "schema_version": SCHEMA_VERSION,
            "producer_git_sha": config.producer_git_sha,
            "configuration": effective,
            "adapter_version": ADAPTER_VERSION,
            "dictionary_version": DICTIONARY_VERSION,
            "run_key": key,
            "processing_key": manifest.idempotency_key,
            "policy_hash": config.policy_hash,
            "core_start": config.core_start.isoformat(),
            "core_end": config.core_end.isoformat(),
            "halo_start": config.halo_start.isoformat(),
            "halo_end": config.halo_end.isoformat(),
            "sources": receipts,
            "carried_receipt_sha256": carry_hash,
            "carried_run_key": previous["run_key"] if previous else None,
            "counts": dict(sorted(counts.items())),
            "input_rows": total,
            "peak_writer_rows": max(w.peak for w in writers.values()),
            "peak_arrow_table_bytes": max(w.peak_table_bytes for w in writers.values()),
            "sqlite_spill_bytes": (stage / "spill.sqlite").stat().st_size,
            "sqlite_cache_kib": config.sqlite_cache_kib,
            "artifacts": {f"{name}.parquet": digest(stage / f"{name}.parquet") for name in writers},
            "limitations": "native received-AIS candidates; unresolved vessel identity; "
            "barriers retained conservatively; no tracks/H3/receiver/fleet completeness",
        }
        publish_result(stage, final, result)
        return final
    except Exception as error:
        db.close()
        for writer in writers.values():
            try:
                writer.close()
            except Exception:
                pass  # Failed staging artifacts are never published/reused.
        mark_failed(stage, error, METHOD)
        raise


def load_job(path: Path) -> tuple[list[LocalInput], IngestionConfig]:
    """Strict, explicit local job configuration; input paths relative to job file."""
    if path.stat().st_size > 1048576:
        raise ValueError("job configuration exceeds 1 MiB")
    job = strict_json(path)
    if set(job) != {"config", "inputs"}:
        raise ValueError("job requires exactly config and inputs")
    values = job["config"]
    for key in ("core_start", "core_end", "halo_start", "halo_end"):
        values[key] = datetime.fromisoformat(values[key])
    values["spatial"] = SpatialFilter(**values["spatial"])
    config = IngestionConfig(**values)
    inputs = []
    for entry in job["inputs"]:
        if set(entry) != {"path", "receipt"}:
            raise ValueError("local input requires exactly path and receipt")
        receipt = entry["receipt"]
        for key in ("start", "end", "acquired_at"):
            if receipt[key] is not None:
                receipt[key] = datetime.fromisoformat(receipt[key])
        inputs.append(
            LocalInput((path.parent / entry["path"]).resolve(), SourcePartition(**receipt))
        )
    return inputs, config
