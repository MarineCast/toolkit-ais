"""Entirely synthetic/offline; no real vessel identities or private AIS records."""

import csv
import json
import subprocess
import sys
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from ais_toolkit.adapters import (
    ADAPTER_VERSION,
    CURRENT_HEADERS,
    DICTIONARY_VERSION,
    LEGACY_HEADERS,
    MarineCadastreAdapter,
    classify_vessel,
)
from ais_toolkit.cli import main
from ais_toolkit.contracts import (
    Coverage,
    DailyExport,
    LocalWindow,
    MetricStatus,
    MetricValue,
    ParseResult,
    ProductConfig,
    Provenance,
    validate_export_batch,
)
from ais_toolkit.metrics import METRICS
from ais_toolkit.processing import ProcessingManifest
from ais_toolkit.sources import SourcePartition
from ais_toolkit.tracks import (
    AcceptedInterval,
    Contribution,
    TrackConfig,
    assess_interval,
    reconcile_messages,
    validate_conservation,
)

FIXTURES = Path(__file__).parent / "fixtures"
START = datetime(2025, 1, 1, tzinfo=timezone.utc)


def source(era="2025+", row=2):
    return Provenance(
        "noaa_marinecadastre", era, "synthetic", row, DICTIONARY_VERSION, ADAPTER_VERSION
    )


def row(era="2025+", **changes):
    headers = CURRENT_HEADERS if era == "2025+" else LEGACY_HEADERS
    result = dict.fromkeys(headers, "")
    if era == "2025+":
        result.update(
            mmsi="000000001",
            base_date_time="2025-01-01T00:00:00Z",
            latitude="10",
            longitude="20",
            sog="0",
            vessel_type="999",
            transceiver="B",
        )
    else:
        result.update(
            MMSI="000000001",
            BaseDateTime="2020-01-01T00:00:00Z",
            LAT="10",
            LON="20",
            SOG="0",
            VesselType="70",
            TransceiverClass="A",
        )
    result.update(changes)
    return result


def parsed(**changes):
    return MarineCadastreAdapter("2025+").parse(row(**changes), source())


@pytest.mark.parametrize("era", ("2018-2024", "2025+"))
def test_era_headers_order_and_coordinates(era):
    values = dict(reversed(list(row(era).items())))
    result = MarineCadastreAdapter(era).parse(values, source(era))
    assert result.position.longitude == 20
    assert result.position.latitude == 10
    assert result.position.timestamp.utcoffset() == timedelta(0)
    assert result.position.raw_vessel_code != result.position.equipment_class


def test_schema_duplicate_mixed_and_unknown():
    adapter = MarineCadastreAdapter("2025+")
    for headers in (
        (*CURRENT_HEADERS, "mmsi"),
        LEGACY_HEADERS,
        (*CURRENT_HEADERS[:-1], "TransceiverClass"),
    ):
        with pytest.raises(ValueError):
            adapter.validate_headers(headers)


def test_timestamp_naive_policy_and_utc_conversion():
    values = row(base_date_time="2025-01-01T00:00:00")
    assert MarineCadastreAdapter("2025+").parse(values, source()).position is None
    accepted = MarineCadastreAdapter("2025+", "dictionary_utc").parse(values, source()).position
    assert accepted.timestamp == START
    assert "source_dictionary_utc_assumed" in accepted.quality_flags
    assert parsed(base_date_time="2025-01-01T01:00:00+01:00").position.timestamp == START


@pytest.mark.parametrize(
    "era,year,key", (("2018-2024", "2020", "BaseDateTime"), ("2025+", "2025", "base_date_time"))
)
@pytest.mark.parametrize("suffix", ("", "T12", "T12:30", "T12:30Z", "T12:30:00.1234567"))
def test_incomplete_or_unsupported_timestamp_precision_rejected(era, year, key, suffix):
    values = row(era, **{key: f"{year}-01-01{suffix}"})
    result = MarineCadastreAdapter(era, "dictionary_utc").parse(values, source(era))
    assert result.position is None
    assert "invalid_or_naive_timestamp" in result.rejection_reasons


@pytest.mark.parametrize(
    "era,year,key", (("2018-2024", "2020", "BaseDateTime"), ("2025+", "2025", "base_date_time"))
)
@pytest.mark.parametrize("offset", ("", "Z", "+01:00"))
def test_full_source_timestamp_precision_and_supported_fractions(era, year, key, offset):
    values = row(era, **{key: f"{year}-01-01T01:02:03.123456{offset}"})
    result = MarineCadastreAdapter(era, "dictionary_utc").parse(values, source(era))
    assert result.position.timestamp.microsecond == 123456
    assert result.position.timestamp.second == 3
    assert result.position.timestamp.minute == 2
    assert ("source_dictionary_utc_assumed" in result.position.quality_flags) == (offset == "")


@pytest.mark.parametrize(
    "changes",
    (
        {"latitude": "91"},
        {"longitude": "181"},
        {"latitude": "NaN"},
        {"longitude": "inf"},
        {"latitude": ""},
        {"latitude": "90"},
        {"base_date_time": "2024-12-31T00:00:00Z"},
        {"mmsi": "1"},
    ),
)
def test_invalid_positions_are_rejected(changes):
    result = parsed(**changes)
    assert result.position is None and result.rejection_reasons


@pytest.mark.parametrize("speed", ("102.3", "-1", "NaN", "inf", "", "null", "bad"))
def test_speed_missing_sentinel_invalid_not_zero(speed):
    position = parsed(sog=speed).position
    assert position.sog_knots is None and position.quality_flags
    assert parsed(sog="0").position.sog_knots == 0


def test_unknown_class_equipment_and_identity_provenance():
    position = parsed(transceiver="future").position
    classification = classify_vessel(position.raw_vessel_code, {"70": "cargo"}, "reviewed-v1")
    assert (classification.category, classification.raw_code) == ("unknown", "999")
    assert position.equipment_class == "future"
    with pytest.raises(ValueError, match="provenance"):
        MarineCadastreAdapter("2025+").parse(row(), replace(source(), source_era="2018-2024"))


def test_duplicate_indexes_are_irrelevant_and_conflicts_retained():
    p = parsed().position
    copy = replace(p, provenance=source(row=3))
    groups = reconcile_messages([copy, p])
    assert groups[0].outcome == "duplicate" and len(groups[0].messages) == 2
    conflict = replace(copy, longitude=21)
    groups = reconcile_messages([conflict, p])
    assert groups[0].outcome == "conflict" and len(groups[0].messages) == 2
    assert groups == reconcile_messages([p, conflict])


def test_interval_barriers_order_identity_gap_and_stationary_support():
    a = parsed()
    b = parsed(base_date_time="2025-01-01T00:01:00Z")
    config = TrackConfig(120)
    decision = assess_interval(a, b, config)
    assert decision.outcome == "pending_scientific_validation"
    assert decision.supported_seconds == 60 and decision.stationary_candidate
    assert "nonpositive_or_unordered_time" in assess_interval(b, a, config).reasons
    assert "nonpositive_or_unordered_time" in assess_interval(a, a, config).reasons
    assert "excessive_gap" in assess_interval(a, b, TrackConfig(30)).reasons
    assert "identity_conflict" in assess_interval(a, b, config, identity_conflict=True).reasons
    bad = ParseResult(source(row=3), None, ("invalid_lat",))
    assert "invalid_sample_barrier" in assess_interval(a, bad, config).reasons
    c = replace(b, position=replace(b.position, imo="SYNTHETIC-OTHER"))
    d = replace(a, position=replace(a.position, imo="SYNTHETIC-ONE"))
    assert "identity_conflict" in assess_interval(d, c, config).reasons
    with pytest.raises(ValueError):
        TrackConfig(120, bridge_invalid_samples=True)


def test_stationary_time_and_distance_conservation_independent_fixture():
    # A 60-second stationary interval split at 30 seconds: 1/60 vessel-hour, zero km.
    interval = AcceptedInterval(
        "i",
        "synthetic-identity",
        START,
        START + timedelta(seconds=60),
        0,
        (source(), source(row=3)),
        "synthetic-v1",
        ("fixture",),
    )
    parts = [
        Contribution(
            "i",
            "synthetic-identity",
            "synthetic-cell",
            START,
            START + timedelta(seconds=30),
            0,
            30,
            0,
            "synthetic",
        ),
        Contribution(
            "i",
            "synthetic-identity",
            "synthetic-cell",
            START + timedelta(seconds=30),
            START + timedelta(seconds=60),
            0,
            0,
            None,
            "synthetic",
        ),
    ]
    validate_conservation(interval, parts, seconds_tolerance=0, km_tolerance=0)
    assert sum((p.end - p.start).total_seconds() for p in parts) / 3600 == 1 / 60
    with pytest.raises(ValueError, match="overlap"):
        validate_conservation(interval, [parts[0], parts[0]], seconds_tolerance=0, km_tolerance=0)
    with pytest.raises(ValueError, match="distance"):
        validate_conservation(
            interval,
            [replace(parts[0], distance_km=1), parts[1]],
            seconds_tolerance=0,
            km_tolerance=0,
        )


def test_sog_integral_units_and_unknown_speed_exclusion():
    # Three one-minute pieces: 10 knots, 20 knots, then unknown speed.
    # Supported mean = (10 + 20) / 2 = 15 knots, with 2/60 hour speed support.
    parts = [
        Contribution(
            "i",
            "synthetic",
            "cell",
            START + timedelta(seconds=60 * index),
            START + timedelta(seconds=60 * (index + 1)),
            0,
            0 if speed is None else 60,
            None if speed is None else speed * 60,
            "synthetic",
        )
        for index, speed in enumerate((10, 20, None))
    ]
    knot_seconds = sum(p.reported_sog_integral_knot_seconds or 0 for p in parts)
    supported_seconds = sum(p.speed_supported_seconds for p in parts)
    assert knot_seconds == 1800
    assert supported_seconds == 120
    assert knot_seconds / supported_seconds == 15
    # Convert BOTH quantities when presenting an hours-based companion.
    knot_hours, supported_hours = knot_seconds / 3600, supported_seconds / 3600
    assert knot_hours == 0.5
    assert supported_hours == pytest.approx(1 / 30)
    assert knot_hours / supported_hours == 15
    metric = next(m for m in METRICS if m.name == "reported_sog_time_weighted_knots")
    assert metric.unit == "knots"
    assert "knot-seconds" in metric.definition and "seconds" in metric.denominator
    assert "zero supported seconds yields null" in metric.reduction


def coverage():
    return Coverage(
        "accepted interval vs requested synthetic time; not fleet completeness",
        60,
        86400,
        "seconds",
        ("synthetic",),
    )


def test_per_metric_status_and_null_rules():
    MetricValue(0, MetricStatus.OBSERVED, "synthetic stationary estimate", coverage())
    MetricValue(None, MetricStatus.UNAVAILABLE, "unimplemented", coverage(), "pending engine")
    MetricValue(1, MetricStatus.PARTIAL, "synthetic estimate", coverage(), "gaps")
    for status, value in (
        (MetricStatus.UNKNOWN, 0),
        (MetricStatus.UNAVAILABLE, 0),
        (MetricStatus.OBSERVED, None),
        (MetricStatus.OBSERVED, float("nan")),
    ):
        with pytest.raises(ValueError):
            MetricValue(value, status, "synthetic", coverage())


def test_export_daily_keys_mixed_schema_and_unresolved_config():
    config = ProductConfig("synthetic-domain", "synthetic-v1", 6)
    export = DailyExport(
        "synthetic-cell",
        date(2025, 1, 1),
        START,
        START + timedelta(days=1),
        "synthetic",
        config,
        ("fixture",),
        "unimplemented",
        {
            "vessel_hours": MetricValue(
                None, MetricStatus.UNAVAILABLE, "unimplemented", coverage(), "pending"
            )
        },
    )
    validate_export_batch([export])
    with pytest.raises(ValueError, match="duplicate"):
        validate_export_batch([export, export])
    with pytest.raises(ValueError, match="one spatial"):
        validate_export_batch(
            [export, replace(export, cell="other", config=replace(config, h3_resolution=7))]
        )
    with pytest.raises(ValueError, match="canonical"):
        replace(export, window_end=START + timedelta(hours=23))
    with pytest.raises(ValueError, match="unresolved"):
        replace(export, config=ProductConfig())


@pytest.mark.parametrize("day,hours", ((date(2025, 3, 9), 23), (date(2025, 11, 2), 25)))
def test_local_companion_dst_is_separate(day, hours):
    # Explicit fixture zone/version; no default regional clock is inferred.
    zone = ZoneInfo("America/Los_Angeles")
    start = datetime.combine(day, datetime.min.time(), zone)
    end = datetime.combine(day + timedelta(days=1), datetime.min.time(), zone)
    window = LocalWindow(
        day,
        zone.key,
        "fixture-host-tzdb",
        "local-day",
        start.astimezone(timezone.utc),
        end.astimezone(timezone.utc),
    )
    assert window.duration_seconds == hours * 3600


def test_source_gates_canadian_and_catalog_only():
    p = SourcePartition(
        "canada_configurable",
        "synthetic",
        None,
        None,
        START,
        START + timedelta(days=1),
        "unknown",
        False,
        None,
        None,
        "unavailable",
    )
    with pytest.raises(ValueError, match="provider unavailable"):
        p.require_normalization()
    p = replace(p, provider="noaa_marinecadastre", source_era="2025+", availability="catalogued")
    with pytest.raises(ValueError, match="unavailable"):
        p.require_normalization()
    p = replace(
        p,
        sha256="a" * 64,
        acquired_at=START,
        availability="available",
        schema_verified=True,
        rights_evidence="synthetic",
        coverage_evidence="synthetic",
    )
    p.require_normalization()


def test_incremental_identity_state_halos_and_repeatability():
    manifest = ProcessingManifest(
        "synthetic",
        ("a" * 64, "b" * 64),
        "c" * 64,
        "v1",
        "ais-native/0.1",
        START,
        START + timedelta(days=1),
        START - timedelta(minutes=1),
        START + timedelta(days=1, minutes=1),
        None,
        "e" * 40,
    )
    assert (
        manifest.idempotency_key
        == replace(manifest, source_checksums=("b" * 64, "a" * 64)).idempotency_key
    )
    assert (
        manifest.idempotency_key != replace(manifest, carried_state_sha256="d" * 64).idempotency_key
    )
    assert manifest.idempotency_key != replace(manifest, producer_git_sha="f" * 40).idempotency_key
    with pytest.raises(ValueError, match="halo"):
        replace(manifest, halo_start=START + timedelta(seconds=1))


@pytest.mark.parametrize(
    "filename,era", (("legacy_synthetic.csv", "2018-2024"), ("current_synthetic.csv", "2025+"))
)
def test_cli_integration_fixtures(filename, era, capsys):
    assert main(["validate-csv", str(FIXTURES / filename), "--era", era]) == 0
    assert '"accepted_rows": 2' in capsys.readouterr().out


def test_cli_quarantines_malformed_empty_and_row_cap(tmp_path, capsys):
    malformed = tmp_path / "malformed.csv"
    malformed.write_text(",".join(CURRENT_HEADERS) + "\n1,2\n")
    assert main(["validate-csv", str(malformed), "--era", "2025+"]) == 2
    malformed.write_text(",".join(CURRENT_HEADERS) + "\n")
    assert main(["validate-csv", str(malformed), "--era", "2025+"]) == 1
    assert (
        main(
            [
                "validate-csv",
                str(FIXTURES / "current_synthetic.csv"),
                "--era",
                "2025+",
                "--max-rows",
                "1",
            ]
        )
        == 2
    )
    assert "no partial-success" in capsys.readouterr().out


@pytest.mark.parametrize("blank_between,second_line", ((False, 4), (True, 5)))
def test_cli_physical_record_start_lines_and_record_based_cap(
    tmp_path, monkeypatch, capsys, blank_between, second_line
):
    fixture = tmp_path / "multiline.csv"
    with fixture.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=CURRENT_HEADERS)
        writer.writeheader()
        writer.writerow(row(vessel_name="SYNTHETIC\nNAME"))
        if blank_between:
            stream.write("\n")
        writer.writerow(row(latitude="91"))
    seen = []
    original = MarineCadastreAdapter.parse

    def capture(adapter, values, provenance):
        seen.append(provenance.row_number)
        return original(adapter, values, provenance)

    monkeypatch.setattr(MarineCadastreAdapter, "parse", capture)
    assert main(["validate-csv", str(fixture), "--era", "2025+", "--max-rows", "2"]) == 1
    assert seen == [2, second_line]
    report = json.loads(capsys.readouterr().out)
    assert report["accepted_rows"] == 1
    assert report["rejected_rows"] == [{"row_number": second_line, "reasons": ["invalid_lat"]}]


def test_fixture_parse_and_module_entrypoint():
    values = list(csv.DictReader(StringIO((FIXTURES / "current_synthetic.csv").read_text())))
    results = [
        MarineCadastreAdapter("2025+").parse(v, source(row=i)) for i, v in enumerate(values, 2)
    ]
    assert results[0].position.sog_knots == 0 and results[1].position.sog_knots is None
    subprocess.run(
        [sys.executable, "-m", "ais_toolkit.cli", "inspect-metrics"],
        check=True,
        capture_output=True,
    )
