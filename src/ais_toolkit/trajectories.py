"""Offline conservative estimated trajectories and direct cell contributions, not census."""

import json
import os
import sqlite3
import tempfile
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from math import isclose, isfinite
from pathlib import Path

import h3
import pyarrow as pa
import pyarrow.parquet as pq
import shapely
from shapely import Point

from .contracts import utc
from .geometry import LocalGeometry, LocalProjection
from .ingestion import (
    SCHEMA,
    BatchWriter,
    canonical,
    digest,
    fingerprint,
    mark_failed,
    publish_result,
    retained_signature,
    strict_json,
    verified_receipt,
)

METHOD = "estimated-local-trajectories/0.1"
SCHEMA_VERSION = "ais-contributions/0.1"
ARTIFACTS = ("intervals.parquet", "contributions.parquet", "points.parquet", "state.parquet")
SOFTWARE = {
    "h3": h3.__version__,
    "h3_core": h3.versions()["c"],
    "shapely": shapely.__version__,
    "geos": shapely.geos_version_string,
    "pyarrow": pa.__version__,
}


@dataclass(frozen=True)
class TrajectoryConfig:
    max_gap_seconds: float
    max_implied_speed_knots: float
    material_land_crossing_m: float
    shoreline_uncertainty_m: float
    center_longitude: float
    center_latitude: float
    projection_radius_m: float
    max_relative_metric_error: float
    numerical_tolerance_m: float
    seconds_tolerance: float
    km_tolerance: float
    resolution: int
    batch_rows: int
    max_records: int
    max_cells: int
    max_candidates: int
    max_geometry_vertices: int
    max_contributions: int
    sqlite_cache_kib: int
    producer_git_sha: str

    def __post_init__(self):
        for name in (
            "max_gap_seconds",
            "max_implied_speed_knots",
            "material_land_crossing_m",
            "shoreline_uncertainty_m",
            "numerical_tolerance_m",
            "seconds_tolerance",
            "km_tolerance",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isfinite(value) or value < 0:
                raise ValueError("finite nonnegative thresholds required")
        if not 0 < self.max_gap_seconds <= 1800 or not 0 < self.max_implied_speed_knots <= 102.2:
            raise ValueError("explicit gap (at most 30 minutes) and AIS speed thresholds required")
        if not self.material_land_crossing_m > 0 or not 0 < self.numerical_tolerance_m <= 0.01:
            raise ValueError("positive land threshold and numerical tolerance <= 1 cm required")
        if self.seconds_tolerance < 1e-5 or self.km_tolerance < 1e-8:
            raise ValueError("conservation tolerances must cover timestamp/float precision")
        if self.resolution not in (6, 7):
            raise ValueError("direct R6 or R7 required")
        limits = {
            "batch_rows": 10000,
            "max_records": 10000000,
            "max_cells": 5000,
            "max_candidates": 512,
            "max_geometry_vertices": 100000,
            "max_contributions": 10000000,
            "sqlite_cache_kib": 65536,
        }
        for name, maximum in limits.items():
            value = getattr(self, name)
            if type(value) is not int or not 0 < value <= maximum:
                raise ValueError(f"explicit positive {name} cap <= {maximum} required")
        if (
            not isinstance(self.producer_git_sha, str)
            or len(self.producer_git_sha) != 40
            or any(c not in "0123456789abcdef" for c in self.producer_git_sha)
        ):
            raise ValueError("full producer Git SHA required")
        LocalProjection(
            self.center_longitude,
            self.center_latitude,
            self.projection_radius_m,
            self.max_relative_metric_error,
        )


def verified_track_receipt(directory):
    return verified_receipt(
        directory, method=METHOD, schema_version=SCHEMA_VERSION, artifact_names=ARTIFACTS
    )


def timestamp(row):
    return utc(datetime.fromisoformat(row["timestamp"])) if row and row["timestamp"] else None


def wire(row):
    return {
        k: v.isoformat(timespec="microseconds") if isinstance(v, datetime) else v
        for k, v in row.items()
    }


def episode(row):
    return fingerprint({"source_mmsi": row["mmsi"], "episode_start": row["record_key"]})


INTERVAL_SCHEMA = pa.schema(
    [
        ("interval_id", pa.string()),
        ("mmsi", pa.string()),
        ("episode_id", pa.string()),
        ("identity_status", pa.string()),
        ("left_json", pa.string()),
        ("right_json", pa.string()),
        ("start", pa.timestamp("us", tz="UTC")),
        ("end", pa.timestamp("us", tz="UTC")),
        ("outcome", pa.string()),
        ("reasons", pa.string()),
        ("estimated_geometry_wkt", pa.string()),
        ("distance_km", pa.float64()),
        ("owned_seconds", pa.float64()),
        ("inside_seconds", pa.float64()),
        ("outside_seconds", pa.float64()),
        ("inside_km", pa.float64()),
        ("outside_km", pa.float64()),
        ("numerical_allocation_error_bound_m", pa.float64()),
        ("class_status", pa.string()),
    ]
)
CONTRIBUTION_SCHEMA = pa.schema(
    [
        ("interval_id", pa.string()),
        ("mmsi", pa.string()),
        ("episode_id", pa.string()),
        ("identity_status", pa.string()),
        ("cell", pa.string()),
        ("resolution", pa.int8()),
        ("utc_date", pa.date32()),
        ("start", pa.timestamp("us", tz="UTC")),
        ("end", pa.timestamp("us", tz="UTC")),
        ("seconds", pa.float64()),
        ("vessel_hours", pa.float64()),
        ("distance_km", pa.float64()),
        ("reported_sog_integral_knot_seconds", pa.float64()),
        ("speed_supported_seconds", pa.float64()),
        ("reported_sog_status", pa.string()),
        ("raw_vessel_code", pa.string()),
        ("equipment_class", pa.string()),
        ("class_status", pa.string()),
        ("left_record_key", pa.string()),
        ("right_record_key", pa.string()),
        ("method", pa.string()),
    ]
)
POINT_SCHEMA = SCHEMA.append(pa.field("cell", pa.string())).append(
    pa.field("cell_status", pa.string())
)
TRACK_STATE_SCHEMA = pa.schema(
    [
        ("mmsi", pa.string()),
        ("last_event_json", pa.string()),
        ("episode_id", pa.string()),
        ("barrier_seen", pa.bool_()),
    ]
)


_METADATA = {b"ais_schema_version": SCHEMA_VERSION.encode(), b"method": METHOD.encode()}
INTERVAL_SCHEMA = INTERVAL_SCHEMA.with_metadata(_METADATA)
CONTRIBUTION_SCHEMA = CONTRIBUTION_SCHEMA.with_metadata(_METADATA)
POINT_SCHEMA = POINT_SCHEMA.with_metadata(_METADATA)
TRACK_STATE_SCHEMA = TRACK_STATE_SCHEMA.with_metadata(_METADATA)


def allocate(left, right, geometry, core_start, core_end, interval_id, episode_id):
    """Full core-owned partition of accepted interval; includes outside AOI and UTC days."""
    start, end = timestamp(left), timestamp(right)
    duration = (end - start).total_seconds()
    route = geometry.route(left, right)
    parts = geometry.spatial_parts(route)
    bounds = [max(start, core_start), min(end, core_end)]
    midnight = datetime.combine(bounds[0].date(), datetime.min.time(), timezone.utc) + timedelta(
        days=1
    )
    while midnight < bounds[1]:
        bounds.append(midnight)
        midnight += timedelta(days=1)
    time_cuts = sorted((value - start).total_seconds() / duration for value in bounds)
    rows = []
    stable_class = all(left[k] == right[k] for k in ("raw_vessel_code", "equipment_class"))
    speed_known = left["sog_knots"] is not None and right["sog_knots"] is not None
    for lo, hi, cell in parts:
        for a, b in zip(time_cuts, time_cuts[1:]):
            u, v = max(lo, a), min(hi, b)
            if v <= u:
                continue
            begin, finish = (
                start + timedelta(seconds=u * duration),
                start + timedelta(seconds=v * duration),
            )
            seconds = (finish - begin).total_seconds()
            if (
                seconds <= 0
            ):  # Below timestamp precision; bounded by the reported numerical envelope.
                continue
            integral = None
            if speed_known:
                # Explicit linear interpolation of reported endpoint SOG, separate from implied speed.
                integral = seconds * (
                    left["sog_knots"] + (right["sog_knots"] - left["sog_knots"]) * (u + v) / 2
                )
            rows.append(
                {
                    "interval_id": interval_id,
                    "mmsi": left["mmsi"],
                    "episode_id": episode_id,
                    "identity_status": "source_mmsi_episode_unresolved",
                    "cell": cell,
                    "resolution": geometry.config.resolution,
                    "utc_date": begin.date(),
                    "start": begin,
                    "end": finish,
                    "seconds": seconds,
                    "vessel_hours": seconds / 3600,
                    "distance_km": route.length / 1000 * (v - u),
                    "reported_sog_integral_knot_seconds": integral,
                    "speed_supported_seconds": seconds if speed_known else 0.0,
                    "reported_sog_status": "supported_estimate" if speed_known else "unknown",
                    "raw_vessel_code": left["raw_vessel_code"] if stable_class else None,
                    "equipment_class": left["equipment_class"] if stable_class else None,
                    "class_status": "stable_raw_attributes"
                    if stable_class
                    else "endpoint_class_change_unknown",
                    "left_record_key": left["record_key"],
                    "right_record_key": right["record_key"],
                    "method": METHOD,
                }
            )
    return rows, len(parts) * 2 * geometry.config.numerical_tolerance_m if route.length else 0.0


def interval(left, right, config, geometry, core_start, core_end, episode_id, *, blocked=False):
    start, end = timestamp(left), timestamp(right)
    identifier = fingerprint(
        {"left": left["record_key"], "right": right["record_key"], "method": METHOD}
    )
    reasons = []
    if blocked:
        reasons.append("conservative_carry_or_unknown_time_barrier")
    if left["outcome"] != "accepted" or right["outcome"] != "accepted":
        reasons.append("original_adjacency_barrier")
    if left["mmsi"] != right["mmsi"] or (
        left["imo"] and right["imo"] and left["imo"] != right["imo"]
    ):
        reasons.append("identity_episode_conflict")
    duration = (end - start).total_seconds() if start and end else None
    if duration is None or duration <= 0:
        reasons.append("unknown_or_nonpositive_time")
    elif duration > config.max_gap_seconds:
        reasons.append("excessive_gap")
    route = None
    if not reasons:
        try:
            route = geometry.route(left, right)
            speed = route.length / duration / (1852 / 3600)
            if (
                speed * (1 + geometry.projection.metric_error_bound)
                > config.max_implied_speed_knots
            ):
                reasons.append("implied_speed_jump_or_threshold_uncertainty")
            screened = geometry.screen(route)
            if screened:
                reasons.append(screened)
        except ValueError:
            reasons.append("unsupported_projection_geometry")
    row = {
        "interval_id": identifier,
        "mmsi": right["mmsi"],
        "episode_id": episode_id,
        "identity_status": "source_mmsi_episode_unresolved",
        "left_json": canonical(left),
        "right_json": canonical(right),
        "start": start,
        "end": end,
        "outcome": "rejected" if reasons else "accepted_estimate",
        "reasons": canonical(sorted(set(reasons))),
        "estimated_geometry_wkt": route.wkt if route is not None else None,
        "distance_km": None,
        "owned_seconds": None,
        "inside_seconds": None,
        "outside_seconds": None,
        "inside_km": None,
        "outside_km": None,
        "numerical_allocation_error_bound_m": None,
        "class_status": "stable_raw_attributes"
        if all(left[k] == right[k] for k in ("raw_vessel_code", "equipment_class"))
        else "endpoint_class_change_unknown",
    }
    contributions = []
    if not reasons:
        contributions, error_m = allocate(
            left, right, geometry, core_start, core_end, identifier, episode_id
        )
        owned = (min(end, core_end) - max(start, core_start)).total_seconds()
        distance = route.length / 1000 * owned / duration
        if not isclose(
            sum(p["seconds"] for p in contributions),
            owned,
            abs_tol=config.seconds_tolerance,
            rel_tol=0,
        ):
            raise ValueError("allocation time conservation failed")
        if not isclose(
            sum(p["distance_km"] for p in contributions),
            distance,
            abs_tol=config.km_tolerance,
            rel_tol=0,
        ):
            raise ValueError("allocation distance conservation failed")
        row.update(
            distance_km=distance,
            owned_seconds=owned,
            inside_seconds=sum(p["seconds"] for p in contributions if p["cell"]),
            outside_seconds=sum(p["seconds"] for p in contributions if not p["cell"]),
            inside_km=sum(p["distance_km"] for p in contributions if p["cell"]),
            outside_km=sum(p["distance_km"] for p in contributions if not p["cell"]),
            numerical_allocation_error_bound_m=error_m,
        )
    return row, contributions


def process_tracks(
    ingestion_directory: Path,
    config: TrajectoryConfig,
    geometry_document: dict,
    output_root: Path,
    *,
    ingestion_carry_directory: Path | None = None,
    carried_directory: Path | None = None,
) -> Path:
    """Process one verified ingestion core; private outputs are capped native evidence."""
    source = verified_receipt(ingestion_directory)
    source_hash = digest(ingestion_directory / "complete.json")
    core_start, core_end = (
        utc(datetime.fromisoformat(source["core_start"])),
        utc(datetime.fromisoformat(source["core_end"])),
    )
    halo_start, halo_end = (
        utc(datetime.fromisoformat(source["halo_start"])),
        utc(datetime.fromisoformat(source["halo_end"])),
    )
    # Incoming audit must retain every intermediate fix. No qualified halo -> no invented support.
    geometry = LocalGeometry(config, geometry_document)
    policy = fingerprint(
        {
            "method": METHOD,
            "schema": SCHEMA_VERSION,
            "config": asdict(config),
            "geometry": geometry_document,
            "software": SOFTWARE,
            "ingestion_policy": source["policy_hash"],
        }
    )
    prior = None
    if source["carried_receipt_sha256"]:
        if ingestion_carry_directory is None:
            raise ValueError("referenced ingestion carry evidence required")
        prior = verified_receipt(ingestion_carry_directory)
        if (
            digest(ingestion_carry_directory / "complete.json") != source["carried_receipt_sha256"]
            or prior["run_key"] != source["carried_run_key"]
        ):
            raise ValueError("ingestion carry receipt identity mismatch")
        if utc(datetime.fromisoformat(prior["core_end"])) != core_start:
            raise ValueError("adjacent ingestion carry required")
    elif ingestion_carry_directory is not None or carried_directory is not None:
        raise ValueError("unreferenced carry evidence forbidden")
    previous = verified_track_receipt(carried_directory) if carried_directory else None
    if previous and (
        previous["policy_hash"] != policy
        or previous["ingestion_receipt_sha256"] != source["carried_receipt_sha256"]
        or utc(datetime.fromisoformat(previous["core_end"])) != core_start
    ):
        raise ValueError(
            "compatible method/config/source and adjacent trajectory checkpoint required"
        )
    previous_hash = digest(carried_directory / "complete.json") if carried_directory else None
    key = fingerprint(
        {"ingestion": source_hash, "policy": policy, "carried_trajectory": previous_hash}
    )
    output_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    final = output_root / key
    if final.exists():
        saved = verified_track_receipt(final)
        if saved["run_key"] != key:
            raise ValueError("existing trajectory output identity mismatch")
        return final
    stage = Path(tempfile.mkdtemp(prefix=".staging-", dir=output_root))
    os.chmod(stage, 0o700)
    db = sqlite3.connect(stage / "spill.sqlite")
    writers = {}
    try:
        db.execute(f"PRAGMA cache_size=-{config.sqlite_cache_kib}")
        db.execute("PRAGMA temp_store=FILE")
        db.execute("CREATE TABLE events (key TEXT PRIMARY KEY,mmsi TEXT,time TEXT,payload TEXT)")
        db.execute(
            "CREATE TABLE carry (mmsi TEXT PRIMARY KEY,point TEXT,barrier INTEGER,episode TEXT)"
        )
        db.execute(
            "CREATE TABLE checkpoint (mmsi TEXT PRIMARY KEY,point TEXT,barrier INTEGER,episode TEXT)"
        )
        carry_rows = 0
        if prior:
            for batch in pq.ParquetFile(ingestion_carry_directory / "state.parquet").iter_batches(
                batch_size=config.batch_rows
            ):
                for row in batch.to_pylist():
                    carry_rows += 1
                    if carry_rows > config.max_records:
                        raise ValueError("ingestion carry row cap exceeded")
                    point = row["last_point_json"]
                    db.execute(
                        "INSERT INTO carry VALUES (?,?,?,?)",
                        (
                            row["mmsi"],
                            point,
                            row["barrier_seen"],
                            episode(json.loads(point)) if point else None,
                        ),
                    )
            if previous:
                carry_rows = 0
                for batch in pq.ParquetFile(carried_directory / "state.parquet").iter_batches(
                    batch_size=config.batch_rows
                ):
                    for row in batch.to_pylist():
                        carry_rows += 1
                        if carry_rows > config.max_records:
                            raise ValueError("trajectory carry row cap exceeded")
                        # Carry cannot clear the authoritative ingestion barrier.
                        db.execute(
                            "UPDATE carry SET episode=?,barrier=barrier OR ? WHERE mmsi=?",
                            (row["episode_id"], row["barrier_seen"], row["mmsi"]),
                        )
        total = 0
        for batch in pq.ParquetFile(ingestion_directory / "audit.parquet").iter_batches(
            batch_size=config.batch_rows
        ):
            for incoming in batch.to_pylist():
                total += 1
                if total > config.max_records:
                    raise ValueError("trajectory input row cap exceeded")
                row = wire(incoming)
                if row["outcome"] not in ("accepted", "duplicate", "rejected", "filtered"):
                    raise ValueError("unrecognized ingestion audit outcome")
                mmsi = row["mmsi"]
                if len(mmsi) != 9 or not mmsi.isascii() or not mmsi.isdigit():
                    continue  # No claimed identity/interval; original evidence remains in input audit.
                when = timestamp(row)
                if row["outcome"] == "duplicate" or (when and not halo_start <= when < halo_end):
                    continue
                db.execute(
                    "INSERT INTO events VALUES (?,?,?,?)",
                    (row["record_key"], mmsi, row["timestamp"], canonical(row)),
                )
        if total != source["input_rows"]:
            raise ValueError("ingestion audit row reconciliation failed")
        db.execute("CREATE INDEX event_order ON events(mmsi,time,key)")
        db.execute("CREATE TABLE quarantine AS SELECT DISTINCT mmsi FROM events WHERE time IS NULL")
        db.execute("CREATE INDEX unknown_identity ON quarantine(mmsi)")
        db.execute("INSERT INTO checkpoint SELECT * FROM carry")
        writers = {
            "intervals": BatchWriter(
                stage / "intervals.parquet", INTERVAL_SCHEMA, config.batch_rows
            ),
            "contributions": BatchWriter(
                stage / "contributions.parquet", CONTRIBUTION_SCHEMA, config.batch_rows
            ),
            "points": BatchWriter(stage / "points.parquet", POINT_SCHEMA, config.batch_rows),
            "state": BatchWriter(stage / "state.parquet", TRACK_STATE_SCHEMA, config.batch_rows),
        }
        counts = Counter()
        last_mmsi, last, current_episode, blocked_carry, unknown_time = (
            None,
            None,
            None,
            False,
            False,
        )
        count_parts = 0
        cursor = db.execute("SELECT mmsi,payload FROM events ORDER BY mmsi,time,key")
        while batch := cursor.fetchmany(config.batch_rows):
            for mmsi, payload in batch:
                row = json.loads(payload)
                when = timestamp(row)
                if mmsi != last_mmsi:
                    carry = db.execute(
                        "SELECT point,barrier,episode FROM carry WHERE mmsi=?", (mmsi,)
                    ).fetchone()
                    last = json.loads(carry[0]) if carry and carry[0] else None
                    current_episode = carry[2] if carry else None
                    blocked_carry = bool(carry and carry[1])
                    unknown_time = bool(
                        db.execute("SELECT 1 FROM quarantine WHERE mmsi=?", (mmsi,)).fetchone()
                    )
                    last_mmsi = mmsi
                if last and timestamp(last) and when and when <= timestamp(last):
                    if (
                        when == timestamp(last)
                        and row["outcome"] == "accepted"
                        and retained_signature(last) == retained_signature(row)
                    ):
                        # A repeated carried endpoint is supplied evidence, not a zero-duration leg.
                        last = row
                        continue
                    if when < timestamp(last):
                        # Earlier halo evidence is independently ordered; retire stale carry context.
                        last, current_episode = None, None
                if row["outcome"] == "accepted" and row["scope"] == "core":
                    observed = dict(row)
                    try:
                        route = Point(
                            geometry.projection.forward(row["longitude"], row["latitude"])
                        )
                        observed["cell"] = geometry.spatial_parts(route)[0][2]
                        observed["cell_status"] = (
                            "observed_point" if observed["cell"] else "outside_aoi"
                        )
                    except ValueError:
                        observed["cell"], observed["cell_status"] = None, "unsupported_geometry"
                    observed["timestamp"] = when
                    writers["points"].append(observed)
                    counts["observed_endpoints"] += 1
                if current_episode is None:
                    current_episode = episode(row)
                interval_rejected = False
                if last:
                    start = timestamp(last)
                    intersects_core = (
                        start is None or when is None or (start < core_end and when > core_start)
                    )
                    if intersects_core:
                        decision, contributions = interval(
                            last,
                            row,
                            config,
                            geometry,
                            core_start,
                            core_end,
                            current_episode,
                            blocked=unknown_time
                            or (blocked_carry and (start is None or start < core_start)),
                        )
                        writers["intervals"].append(decision)
                        counts[decision["outcome"]] += 1
                        for part in contributions:
                            count_parts += 1
                            if count_parts > config.max_contributions:
                                raise ValueError("trajectory contribution cap exceeded")
                            writers["contributions"].append(part)
                        interval_rejected = decision["outcome"] == "rejected"
                        if interval_rejected:
                            current_episode = episode(row)
                if row["outcome"] != "accepted":
                    current_episode = episode(row)
                if when is None or when < core_end:
                    old = db.execute(
                        "SELECT barrier FROM checkpoint WHERE mmsi=?", (mmsi,)
                    ).fetchone()
                    barrier = (
                        unknown_time
                        or interval_rejected
                        or row["outcome"] != "accepted"
                        or bool(old and old[0])
                    )
                    db.execute(
                        "INSERT OR REPLACE INTO checkpoint VALUES (?,?,?,?)",
                        (mmsi, canonical(row), barrier, current_episode),
                    )
                last = row
        cursor = db.execute("SELECT mmsi,point,barrier,episode FROM checkpoint ORDER BY mmsi")
        while batch := cursor.fetchmany(config.batch_rows):
            for mmsi, point, barrier, identity_episode in batch:
                writers["state"].append(
                    {
                        "mmsi": mmsi,
                        "last_event_json": point,
                        "episode_id": identity_episode,
                        "barrier_seen": bool(barrier),
                    }
                )
        for writer in writers.values():
            writer.close()
        db.commit()
        db.close()
        for path in stage.iterdir():
            os.chmod(path, 0o600)
        result = {
            "status": "complete",
            "method": METHOD,
            "schema_version": SCHEMA_VERSION,
            "run_key": key,
            "policy_hash": policy,
            "core_start": core_start.isoformat(),
            "core_end": core_end.isoformat(),
            "configuration": asdict(config),
            "geometry_sha256": fingerprint(geometry_document),
            "domain_version": geometry_document["domain_version"],
            "water_mask_version": geometry_document["water_mask_version"],
            "software": SOFTWARE,
            "ingestion_receipt_sha256": source_hash,
            "ingestion_run_key": source["run_key"],
            "carried_ingestion_sha256": source["carried_receipt_sha256"],
            "carried_trajectory_sha256": previous_hash,
            "metric_relative_error_bound": geometry.projection.metric_error_bound,
            "counts": dict(sorted(counts.items())),
            "input_audit_rows": total,
            "contribution_rows": count_parts,
            "peak_writer_rows": max(w.peak for w in writers.values()),
            "peak_arrow_table_bytes": max(w.peak_table_bytes for w in writers.values()),
            "sqlite_spill_bytes": (stage / "spill.sqlite").stat().st_size,
            "artifacts": {f"{name}.parquet": digest(stage / f"{name}.parquet") for name in writers},
            "limitations": "synthetic-tested local estimates; scientific qualification pending; unresolved MMSI episodes; no fleet completeness/true activity/noise/whale inference",
        }
        # Detect ordinary mutation of source artifacts during processing, not just on entry.
        if (
            verified_receipt(ingestion_directory) != source
            or digest(ingestion_directory / "complete.json") != source_hash
        ):
            raise ValueError("ingestion changed during trajectory processing")
        if prior and (
            verified_receipt(ingestion_carry_directory) != prior
            or digest(ingestion_carry_directory / "complete.json")
            != source["carried_receipt_sha256"]
        ):
            raise ValueError("ingestion carry changed during trajectory processing")
        if previous and (
            verified_track_receipt(carried_directory) != previous
            or digest(carried_directory / "complete.json") != previous_hash
        ):
            raise ValueError("trajectory carry changed during processing")
        publish_result(stage, final, result)
        return final
    except Exception as error:
        db.close()
        for writer in writers.values():
            try:
                writer.close()
            except Exception:
                pass
        mark_failed(stage, error, METHOD)
        raise


def load_track_job(path):
    if path.stat().st_size > 1048576:
        raise ValueError("trajectory job exceeds 1 MiB")
    job = strict_json(path)
    if set(job) != {
        "config",
        "geometry",
        "ingestion_directory",
        "ingestion_carry_directory",
        "carried_directory",
    }:
        raise ValueError("explicit config/geometry/ingestion/carry fields required")
    config = TrajectoryConfig(**job["config"])

    def local_path(name):
        return (path.parent / job[name]).resolve() if job[name] is not None else None

    source = local_path("ingestion_directory")
    if source is None:
        raise ValueError("verified ingestion directory required")
    return (
        source,
        config,
        job["geometry"],
        local_path("ingestion_carry_directory"),
        local_path("carried_directory"),
    )
