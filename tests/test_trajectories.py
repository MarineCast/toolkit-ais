"""Fabricated local geometry/messages only; no scientific interpolation validation."""

import json
from dataclasses import replace
from datetime import timedelta
from math import acos, cos, hypot, sin, sqrt

import h3
import pytest
from test_foundation import START, row
from test_ingestion import config as ingestion_config
from test_ingestion import local, table

from ais_toolkit.cli import main
from ais_toolkit.geometry import RADIUS_M, LocalGeometry, LocalProjection
from ais_toolkit.ingestion import ingest
from ais_toolkit.trajectories import (
    TrajectoryConfig,
    process_tracks,
    verified_track_receipt,
)


def config(**changes):
    # Test choices only; the public config has no gap/coast/geography defaults.
    base = TrajectoryConfig(
        600,
        90,
        5,
        2,
        20,
        10,
        50000,
        0.007,
        0.001,
        1e-5,
        1e-8,
        7,
        5,
        20000,
        1000,
        100,
        1000,
        20000,
        1024,
        "e" * 40,
    )
    return replace(base, **changes)


def ring(projection, corners):
    points = [list(projection.inverse(x, y)) for x, y in corners]
    return {"type": "Polygon", "coordinates": [points + [points[0]]]}


def document(cfg, *, aoi=None, land=None):
    projection = LocalProjection(
        cfg.center_longitude,
        cfg.center_latitude,
        cfg.projection_radius_m,
        cfg.max_relative_metric_error,
    )
    cells = sorted(h3.grid_disk(h3.latlng_to_cell(10, 20, cfg.resolution), 4))
    return {
        "domain_version": "fabricated-rectangle/1",
        "water_mask_version": "fabricated-coast/1",
        "aoi": ring(
            projection, aoi or [(-2000, -2000), (2000, -2000), (2000, 2000), (-2000, 2000)]
        ),
        "land": ring(projection, land) if land else {"type": "MultiPolygon", "coordinates": []},
        "mask_support": ring(
            projection, [(-10000, -10000), (10000, -10000), (10000, 10000), (-10000, 10000)]
        ),
        "cells": cells,
    }


def sample(cfg, x, y, seconds, **changes):
    lon, lat = LocalProjection(
        cfg.center_longitude,
        cfg.center_latitude,
        cfg.projection_radius_m,
        cfg.max_relative_metric_error,
    ).inverse(x, y)
    return row(
        longitude=str(lon),
        latitude=str(lat),
        base_date_time=(START + timedelta(seconds=seconds)).isoformat(),
        **changes,
    )


def run(tmp_path, rows, cfg=None, geometry=None, name="run"):
    cfg = cfg or config()
    source = ingest(
        [local(tmp_path, name, rows)], ingestion_config(), tmp_path / (name + "-ingestion")
    )
    output = process_tracks(source, cfg, geometry or document(cfg), tmp_path / (name + "-tracks"))
    return source, output


def test_straight_route_conserves_time_distance_inside_and_outside_endpoints(tmp_path):
    cfg = config()
    _, output = run(
        tmp_path, [sample(cfg, -4000, 0, 0, sog="2"), sample(cfg, 4000, 0, 600, sog="6")]
    )
    ledger = table(output, "intervals")[0]
    assert ledger["outcome"] == "accepted_estimate"
    assert ledger["distance_km"] == pytest.approx(8)
    assert ledger["inside_km"] == pytest.approx(4)
    assert ledger["outside_km"] == pytest.approx(4)
    assert ledger["inside_seconds"] == pytest.approx(300)
    assert ledger["outside_seconds"] == pytest.approx(300)
    parts = table(output, "contributions")
    assert sum(r["seconds"] for r in parts) == pytest.approx(600)
    assert sum(r["vessel_hours"] for r in parts) == pytest.approx(1 / 6)
    assert sum(r["reported_sog_integral_knot_seconds"] for r in parts) == pytest.approx(
        2400, abs=1e-4
    )
    assert all(r["identity_status"] == "source_mmsi_episode_unresolved" for r in parts)
    assert len(table(output, "points")) == 2  # Outside AOI endpoints still observed.
    assert all(r["cell"] is None for r in table(output, "points"))


@pytest.mark.parametrize("speed", ("0", "", "102.3"))
def test_stationary_time_null_speed_and_observed_endpoints(tmp_path, speed):
    cfg = config()
    _, output = run(tmp_path, [sample(cfg, 0, 0, 0, sog=speed), sample(cfg, 0, 0, 60, sog=speed)])
    parts = table(output, "contributions")
    assert len(parts) == 1 and parts[0]["seconds"] == 60 and parts[0]["distance_km"] == 0
    assert parts[0]["speed_supported_seconds"] == (60 if speed == "0" else 0)
    assert parts[0]["reported_sog_integral_knot_seconds"] == (0 if speed == "0" else None)
    assert parts[0]["reported_sog_status"] == ("supported_estimate" if speed == "0" else "unknown")
    assert len(table(output, "points")) == 2


@pytest.mark.parametrize("gap", (120, 300, 600, 1800))
def test_gap_scenarios_no_extrapolation(tmp_path, gap):
    cfg = config(max_gap_seconds=gap)
    _, output = run(
        tmp_path,
        [sample(cfg, 0, 0, 0), sample(cfg, 0, 0, gap), sample(cfg, 0, 0, 2 * gap + 1)],
        cfg,
    )
    ledger = table(output, "intervals")
    assert [r["outcome"] for r in ledger] == ["accepted_estimate", "rejected"]
    assert "excessive_gap" in ledger[1]["reasons"]
    assert ledger[1]["owned_seconds"] is None
    assert sum(r["seconds"] for r in table(output, "contributions")) == gap


def test_rejected_midpoint_unknown_time_and_identity_episodes(tmp_path):
    cfg = config()
    _, output = run(
        tmp_path,
        [
            sample(cfg, 0, 0, 0),
            sample(cfg, 0, 0, 30),
            sample(cfg, 50, 0, 30),
            sample(cfg, 100, 0, 60, imo="IMO1234567"),
            sample(cfg, 200, 0, 120, imo="IMO1234567"),
            sample(cfg, 300, 0, 180, imo="IMO7654321"),
        ],
    )
    ledger = table(output, "intervals")
    assert len(ledger) == 5
    assert [r["outcome"] for r in ledger] == [
        "rejected",
        "rejected",
        "rejected",
        "accepted_estimate",
        "rejected",
    ]
    assert "identity_episode_conflict" in ledger[-1]["reasons"]
    assert len(table(output, "points")) == 4
    _, unknown = run(
        tmp_path,
        [
            sample(cfg, 0, 0, 0),
            row(base_date_time="bad"),
            sample(cfg, 100, 0, 60),
            sample(cfg, 200, 0, 120),
        ],
        name="unknown",
    )
    assert not table(unknown, "contributions")
    assert all(r["outcome"] == "rejected" for r in table(unknown, "intervals"))


@pytest.mark.parametrize(
    "land,reason",
    [
        ([(-10, -200), (10, -200), (10, 200), (-10, 200)], "material_land_crossing"),
        ([(-10, 1), (10, 1), (10, 200), (-10, 200)], "shoreline_uncertainty"),
    ],
)
def test_island_land_vs_shoreline_uncertainty_never_redistributes_time(tmp_path, land, reason):
    cfg = config()
    _, output = run(
        tmp_path, [sample(cfg, -100, 0, 0), sample(cfg, 100, 0, 60)], cfg, document(cfg, land=land)
    )
    assert reason in table(output, "intervals")[0]["reasons"]
    assert not table(output, "contributions")
    assert len(table(output, "points")) == 2


def test_implied_speed_and_unsupported_projection_mask(tmp_path):
    cfg = config(max_implied_speed_knots=1)
    _, output = run(tmp_path, [sample(cfg, 0, 0, 0), sample(cfg, 100, 0, 1)], cfg)
    assert "implied_speed_jump" in table(output, "intervals")[0]["reasons"]
    _, far = run(
        tmp_path,
        [row(), row(longitude="20.8", base_date_time=(START + timedelta(seconds=60)).isoformat())],
        name="far",
    )
    assert "unsupported_projection_geometry" in table(far, "intervals")[0]["reasons"]
    geometry = document(config())
    geometry["mask_support"] = geometry["aoi"]
    _, unsupported = run(
        tmp_path,
        [sample(config(), -4000, 0, 0), sample(config(), 0, 0, 600)],
        geometry=geometry,
        name="mask",
    )
    assert "water_mask_unavailable" in table(unsupported, "intervals")[0]["reasons"]


def test_direct_resolution_geometry_and_independent_convex_clip_reference(tmp_path):
    # Independent half-plane clipping of a straight route against actual cell vertices.
    def clip(vertices, a, b):
        area = sum(
            x * y2 - x2 * y for (x, y), (x2, y2) in zip(vertices, vertices[1:] + vertices[:1])
        )
        sign = 1 if area > 0 else -1
        lo, hi = 0.0, 1.0
        for p, q in zip(vertices, vertices[1:] + vertices[:1]):
            ex, ey = q[0] - p[0], q[1] - p[1]
            base = sign * (ex * (a[1] - p[1]) - ey * (a[0] - p[0]))
            slope = sign * (ex * (b[1] - a[1]) - ey * (b[0] - a[0]))
            if abs(slope) < 1e-12:
                if base < 0:
                    return 0
            elif slope > 0:
                lo = max(lo, -base / slope)
            else:
                hi = min(hi, -base / slope)
        return max(0, hi - lo)

    totals = []
    for resolution in (6, 7):
        cfg = config(resolution=resolution)
        geometry = document(cfg)
        model = LocalGeometry(cfg, geometry)
        a, b = (-1800, -1200), (1800, 1200)
        _, output = run(
            tmp_path,
            [sample(cfg, *a, 0), sample(cfg, *b, 600)],
            cfg,
            geometry,
            name=f"r{resolution}",
        )
        parts = table(output, "contributions")
        by_cell = {}
        for part in parts:
            by_cell[part["cell"]] = by_cell.get(part["cell"], 0) + part["seconds"]
            assert h3.get_resolution(part["cell"]) == resolution
        for cell, polygon in zip(model.cells, model.polygons):
            expected = clip(list(polygon.exterior.coords)[:-1], a, b) * 600
            assert by_cell.get(cell, 0) == pytest.approx(expected, abs=3e-6)
        totals.append(sum(p["distance_km"] for p in parts))
    assert totals[0] == pytest.approx(totals[1])


def test_shared_edge_corner_and_stationary_deterministic_ownership(tmp_path):
    cfg = config()
    model = LocalGeometry(cfg, document(cfg))
    # Use actual shared projected H3 boundary, with explicitly expanded AOI covering it.
    common = None
    pair = None
    for i, p in enumerate(model.polygons):
        for j in range(i + 1, len(model.polygons)):
            edge = p.boundary.intersection(model.polygons[j].boundary)
            if (
                edge.geom_type == "LineString"
                and edge.length > 100
                and hypot(edge.centroid.x, edge.centroid.y) < 2500
            ):
                common, pair = edge, (model.cells[i], model.cells[j])
                break
        if common is not None:
            break
    assert common is not None
    geometry = document(cfg, aoi=[(-4000, -4000), (4000, -4000), (4000, 4000), (-4000, 4000)])
    start, end = common.interpolate(0.2, normalized=True), common.interpolate(0.8, normalized=True)
    _, output = run(
        tmp_path, [sample(cfg, start.x, start.y, 0), sample(cfg, end.x, end.y, 600)], cfg, geometry
    )
    parts = table(output, "contributions")
    assert sum(p["seconds"] for p in parts) == pytest.approx(600)
    assert {p["cell"] for p in parts} == {min(pair)}
    vertex = common.coords[0]
    _, stationary = run(
        tmp_path, [sample(cfg, *vertex, 0), sample(cfg, *vertex, 60)], cfg, geometry, name="corner"
    )
    assert sum(p["seconds"] for p in table(stationary, "contributions")) == 60
    assert len(table(stationary, "contributions")) == 1


def test_local_projection_bound_independent_spherical_reference_convergence():
    projection = LocalProjection(20, 10, 50000, 0.007)
    a, b = projection.inverse(-20000, -10000), projection.inverse(22000, 17000)
    reference = h3.great_circle_distance((a[1], a[0]), (b[1], b[0]), unit="m")
    planar = hypot(42000, 27000)
    assert abs(planar / reference - 1) <= projection.metric_error_bound

    # Independent 3-D spherical vectors compared to chord integration at two densities.
    def vector(lon, lat):
        from math import radians

        lon, lat = map(radians, (lon, lat))
        return (cos(lat) * cos(lon), cos(lat) * sin(lon), sin(lat))

    u, v = vector(*a), vector(*b)
    angle = acos(sum(x * y for x, y in zip(u, v)))

    def length(n):
        points = [
            tuple(
                (sin((1 - i / n) * angle) * x + sin(i / n * angle) * y) / sin(angle)
                for x, y in zip(u, v)
            )
            for i in range(n + 1)
        ]
        return sum(
            RADIUS_M * sqrt(sum((x - y) ** 2 for x, y in zip(p, q)))
            for p, q in zip(points, points[1:])
        )

    errors = [abs(length(n) - reference) for n in (32, 64)]
    assert errors[1] < errors[0] / 3
    assert errors[1] <= reference * angle**2 / (24 * 64**2) + 1e-6  # sphere chord sagitta bound.
    assert projection.forward(*projection.inverse(1234, 5678)) == pytest.approx(
        (1234, 5678), abs=1e-8
    )
    with pytest.raises(ValueError, match="metric error"):
        LocalProjection(20, 10, 50000, 1e-4)
    with pytest.raises(ValueError, match="antimeridian/polar"):
        LocalProjection(179, 10, 50000, 0.007)


def test_midnight_incremental_carry_ownership_and_compatibility(tmp_path):
    cfg = config()
    rows = [sample(cfg, -100, 0, 86370), sample(cfg, 100, 0, 86430), sample(cfg, 200, 0, 86460)]
    item = local(tmp_path, "two-days", rows)
    first_source = ingest([item], ingestion_config(), tmp_path / "source-one")
    first = process_tracks(first_source, cfg, document(cfg), tmp_path / "one")
    next_input = ingestion_config(
        partition_id="next",
        core_start=START + timedelta(days=1),
        core_end=START + timedelta(days=2),
        halo_start=START + timedelta(days=1, minutes=-1),
        halo_end=START + timedelta(days=2, minutes=1),
    )
    second_source = ingest([item], next_input, tmp_path / "source-two", first_source)
    with pytest.raises(ValueError, match="carry evidence required"):
        process_tracks(second_source, cfg, document(cfg), tmp_path / "missing")
    second = process_tracks(
        second_source,
        cfg,
        document(cfg),
        tmp_path / "two",
        ingestion_carry_directory=first_source,
        carried_directory=first,
    )
    assert sum(p["seconds"] for p in table(first, "contributions")) == 30
    assert sum(p["seconds"] for p in table(second, "contributions")) == 60
    assert all(p["end"] <= START + timedelta(days=1) for p in table(first, "contributions"))
    assert all(p["start"] >= START + timedelta(days=1) for p in table(second, "contributions"))
    assert len(table(first, "points")) == 1 and len(table(second, "points")) == 2
    assert table(first, "intervals")[0]["episode_id"] == table(second, "intervals")[0]["episode_id"]
    assert (
        process_tracks(
            second_source,
            cfg,
            document(cfg),
            tmp_path / "two",
            ingestion_carry_directory=first_source,
            carried_directory=first,
        )
        == second
    )
    with pytest.raises(ValueError, match="compatible"):
        process_tracks(
            second_source,
            replace(cfg, max_gap_seconds=300),
            document(cfg),
            tmp_path / "bad-policy",
            ingestion_carry_directory=first_source,
            carried_directory=first,
        )
    # A single two-day core gives the same additive support and UTC-day ownership.
    wide_input = ingestion_config(
        core_end=START + timedelta(days=2), halo_end=START + timedelta(days=2, minutes=1)
    )
    wide_source = ingest([item], wide_input, tmp_path / "source-wide")
    wide = process_tracks(wide_source, cfg, document(cfg), tmp_path / "wide")
    assert sum(p["seconds"] for p in table(wide, "contributions")) == 90
    assert {p["utc_date"] for p in table(wide, "contributions")} == {
        START.date(),
        (START + timedelta(days=1)).date(),
    }
    assert sum(
        p["distance_km"] for p in table(first, "contributions") + table(second, "contributions")
    ) == pytest.approx(sum(p["distance_km"] for p in table(wide, "contributions")))


def test_conflicting_carry_cannot_bridge_later_core_fix(tmp_path):
    cfg = config()
    prior = local(tmp_path, "prior", [sample(cfg, 0, 0, 86370)])
    source_one = ingest([prior], ingestion_config(), tmp_path / "input-one")
    one = process_tracks(source_one, cfg, document(cfg), tmp_path / "one")
    new = local(
        tmp_path,
        "new",
        [sample(cfg, 20, 0, 86370), sample(cfg, 100, 0, 86400), sample(cfg, 200, 0, 86460)],
    )
    next_config = ingestion_config(
        partition_id="next",
        core_start=START + timedelta(days=1),
        core_end=START + timedelta(days=2),
        halo_start=START + timedelta(days=1, minutes=-1),
        halo_end=START + timedelta(days=2, minutes=1),
    )
    source_two = ingest([new], next_config, tmp_path / "input-two", source_one)
    two = process_tracks(
        source_two,
        cfg,
        document(cfg),
        tmp_path / "two",
        ingestion_carry_directory=source_one,
        carried_directory=one,
    )
    # No contribution connects the conflict to the clean first core point.
    parts = table(two, "contributions")
    assert sum(p["seconds"] for p in parts) == 60
    assert all(p["start"] >= START + timedelta(days=1) for p in parts)
    assert table(two, "state")[0]["barrier_seen"]
    assert table(two, "intervals")[-1]["outcome"] == "accepted_estimate"


@pytest.mark.parametrize("winner", (False, True))
def test_publication_failure_retry_checksum_and_caps(tmp_path, monkeypatch, winner):
    import shutil

    import ais_toolkit.ingestion as ingestion

    cfg = config()
    item = local(tmp_path, "publish", [sample(cfg, 0, 0, 0), sample(cfg, 100, 0, 60)])
    source = ingest([item], ingestion_config(), tmp_path / "input")
    out = tmp_path / "output"

    def fail(stage, final):
        if winner:
            shutil.copytree(stage, final)
        raise OSError("synthetic publication error")

    with monkeypatch.context() as patch:
        patch.setattr(ingestion.os, "rename", fail)
        with pytest.raises(OSError, match="publication error"):
            process_tracks(source, cfg, document(cfg), out)
    stage = next(out.glob(".staging-*"))
    assert not (stage / "complete.json").exists()
    assert (stage / "failure.json").exists()
    with pytest.raises(ValueError, match="failed/unpublished"):
        verified_track_receipt(stage)
    good = process_tracks(source, cfg, document(cfg), out)
    assert verified_track_receipt(good)["counts"]["accepted_estimate"] == 1
    assert process_tracks(source, cfg, document(cfg), out) == good
    (good / "contributions.parquet").write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="checksum mismatch"):
        process_tracks(source, cfg, document(cfg), out)
    with pytest.raises(ValueError, match="row cap"):
        process_tracks(source, replace(cfg, max_records=1), document(cfg), tmp_path / "capped")
    assert not list((tmp_path / "capped").glob("*/complete.json"))


def test_duplicate_order_class_changes_and_cli_strict_config(tmp_path, capsys):
    from dataclasses import asdict

    from ais_toolkit.trajectories import load_track_job

    cfg = config()
    a = local(tmp_path, "a", [sample(cfg, 0, 0, 0), sample(cfg, 100, 0, 60, vessel_type="70")])
    b = local(tmp_path, "b", [sample(cfg, 0, 0, 0)])
    one = ingest([a, b], ingestion_config(), tmp_path / "input-one")
    two = ingest([b, a], ingestion_config(), tmp_path / "input-two")
    first = process_tracks(one, cfg, document(cfg), tmp_path / "one")
    second = process_tracks(two, cfg, document(cfg), tmp_path / "two")
    assert verified_track_receipt(first) == verified_track_receipt(second)
    assert len(table(first, "points")) == 2
    assert all(
        p["raw_vessel_code"] is None and p["class_status"] == "endpoint_class_change_unknown"
        for p in table(first, "contributions")
    )
    job = tmp_path / "job.json"
    job.write_text(
        json.dumps(
            {
                "config": asdict(cfg),
                "geometry": document(cfg),
                "ingestion_directory": str(one),
                "ingestion_carry_directory": None,
                "carried_directory": None,
            }
        )
    )
    assert load_track_job(job)[0] == one
    assert main(["estimate-local", str(job), str(tmp_path / "cli")]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "complete"
    assert main(["estimate-local", str(job), str(tmp_path / "cli")]) == 0
    capsys.readouterr()
    job.write_text('{"config":{},"config":{}}')
    assert main(["estimate-local", str(job), str(tmp_path / "bad")]) == 2


def test_geometry_and_resource_gates(tmp_path):
    cfg = config()
    doc = document(cfg)
    doc["cells"] = doc["cells"][:1]
    with pytest.raises(ValueError, match="does not cover"):
        LocalGeometry(cfg, doc)
    doc = document(cfg)
    doc["cells"][0] = h3.cell_to_parent(doc["cells"][0], 6)
    with pytest.raises(ValueError, match="direct R6/R7"):
        LocalGeometry(cfg, doc)
    with pytest.raises(ValueError, match="vertex cap"):
        LocalGeometry(replace(cfg, max_geometry_vertices=3), document(cfg))
    with pytest.raises(ValueError, match="candidate cap"):
        run(
            tmp_path,
            [sample(cfg, -1800, 0, 0), sample(cfg, 1800, 0, 600)],
            replace(cfg, max_candidates=1),
        )


def test_stationary_land_corner_route_and_writer_batch_bounds(tmp_path):
    import pyarrow.parquet as pq

    cfg = config()
    geometry = document(cfg, land=[(-20, -20), (20, -20), (20, 20), (-20, 20)])
    _, inland = run(tmp_path, [sample(cfg, 0, 0, 0), sample(cfg, 0, 0, 60)], cfg, geometry)
    assert "material_land_crossing" in table(inland, "intervals")[0]["reasons"]
    assert not table(inland, "contributions")
    model = LocalGeometry(cfg, document(cfg))
    polygon = min(model.polygons, key=lambda p: hypot(p.centroid.x, p.centroid.y))
    vertex = polygon.exterior.coords[0]
    _, corner = run(
        tmp_path,
        [
            sample(cfg, vertex[0] - 300, vertex[1] - 400, 0),
            sample(cfg, vertex[0] + 300, vertex[1] + 400, 600),
        ],
        name="moving-corner",
    )
    assert sum(p["seconds"] for p in table(corner, "contributions")) == pytest.approx(600)
    assert sum(p["distance_km"] for p in table(corner, "contributions")) == pytest.approx(1)
    _, many = run(tmp_path, [sample(cfg, 0, 0, i * 30) for i in range(100)], name="many")
    assert verified_track_receipt(many)["peak_writer_rows"] <= cfg.batch_rows
    for name in ("points", "intervals", "contributions", "state"):
        file = pq.ParquetFile(many / f"{name}.parquet")
        assert file.schema_arrow.metadata[b"ais_schema_version"] == b"ais-contributions/0.1"
        assert all(
            file.metadata.row_group(i).num_rows <= cfg.batch_rows
            for i in range(file.num_row_groups)
        )


def test_inherited_barrier_after_later_fixes_blocks_next_partition_entry(tmp_path):
    cfg = config()
    first_item = local(
        tmp_path,
        "first",
        [
            sample(cfg, 0, 0, 86370),
            sample(cfg, 20, 0, 86370),
            sample(cfg, 0, 0, 86380),
            sample(cfg, 0, 0, 86390),
        ],
    )
    first_source = ingest([first_item], ingestion_config(), tmp_path / "first-source")
    first = process_tracks(first_source, cfg, document(cfg), tmp_path / "first")
    assert table(first, "state")[0]["barrier_seen"]
    later = local(
        tmp_path,
        "later",
        [sample(cfg, 0, 0, 86390), sample(cfg, 0, 0, 86430), sample(cfg, 0, 0, 86460)],
    )
    next_cfg = ingestion_config(
        partition_id="next",
        core_start=START + timedelta(days=1),
        core_end=START + timedelta(days=2),
        halo_start=START + timedelta(days=1, minutes=-1),
        halo_end=START + timedelta(days=2, minutes=1),
    )
    source = ingest([later], next_cfg, tmp_path / "source", first_source)
    completed = process_tracks(
        source,
        cfg,
        document(cfg),
        tmp_path / "next",
        ingestion_carry_directory=first_source,
        carried_directory=first,
    )
    ledger = table(completed, "intervals")
    assert ledger[0]["outcome"] == "rejected"
    assert "conservative_carry" in ledger[0]["reasons"]
    assert sum(p["seconds"] for p in table(completed, "contributions")) == 30
    assert table(completed, "state")[0]["barrier_seen"]


def test_filtered_fix_remains_adjacency_barrier_and_mutation_quarantines(tmp_path, monkeypatch):
    import ais_toolkit.trajectories as trajectories

    cfg = config()
    source = ingest(
        [
            local(
                tmp_path,
                "filtered",
                [
                    sample(cfg, 0, 0, 0),
                    row(longitude="25", base_date_time=(START + timedelta(seconds=30)).isoformat()),
                    sample(cfg, 100, 0, 60),
                    sample(cfg, 200, 0, 120),
                ],
            )
        ],
        ingestion_config(),
        tmp_path / "input",
    )
    output = process_tracks(source, cfg, document(cfg), tmp_path / "clean")
    assert sum(p["seconds"] for p in table(output, "contributions")) == 60
    assert "original_adjacency_barrier" in table(output, "intervals")[0]["reasons"]
    original = trajectories.interval

    def changed(*args, **kwargs):
        result = original(*args, **kwargs)
        # A byte-only receipt change must be detected despite identical JSON meaning.
        marker = source / "complete.json"
        marker.write_text(marker.read_text() + " ")
        return result

    monkeypatch.setattr(trajectories, "interval", changed)
    with pytest.raises(ValueError, match="changed during"):
        process_tracks(source, cfg, document(cfg), tmp_path / "mutated")
    assert not list((tmp_path / "mutated").glob("*/complete.json"))
    assert list((tmp_path / "mutated").glob(".staging-*/failure.json"))


def test_provenance_versions_and_cell_aliases_are_not_valid_configuration():
    cfg = config()
    doc = document(cfg)
    doc["domain_version"] = True
    with pytest.raises(ValueError, match="evidence versions"):
        LocalGeometry(cfg, doc)
    doc = document(cfg)
    doc["cells"].append(doc["cells"][0].upper())
    with pytest.raises(ValueError, match="canonical"):
        LocalGeometry(cfg, doc)
    with pytest.raises(ValueError, match="finite projection"):
        replace(cfg, center_longitude=True)
    with pytest.raises(ValueError, match="Git SHA"):
        replace(cfg, producer_git_sha=list("e" * 40))
