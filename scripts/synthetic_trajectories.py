"""Create fabricated ingestion and a trajectory job in an explicit NEW local directory."""

import argparse
import csv
import json
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import h3

from ais_toolkit.adapters import CURRENT_HEADERS, FIELD_MAPS
from ais_toolkit.geometry import LocalProjection
from ais_toolkit.ingestion import IngestionConfig, LocalInput, SpatialFilter, digest, ingest
from ais_toolkit.sources import SourcePartition
from ais_toolkit.trajectories import TrajectoryConfig


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("workspace", type=Path)
    args = parser.parse_args()
    args.workspace.mkdir(parents=True, exist_ok=False, mode=0o700)
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    # These are fabricated demonstration choices, not real study geometry or science defaults.
    cfg = TrajectoryConfig(
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
        7,
        1000,
        1000,
        100,
        1000,
        1000,
        1024,
        "e" * 40,
    )
    projection = LocalProjection(20, 10, 50000, 0.007)
    source = args.workspace / "fabricated.csv"
    # Full pinned era field set, including unused columns.

    with source.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=CURRENT_HEADERS)
        writer.writeheader()
        for x, seconds, speed in ((-4000, 0, "2"), (4000, 600, "6"), (4000, 660, "")):
            lon, lat = projection.inverse(x, 0)
            record = dict.fromkeys(CURRENT_HEADERS, "")
            fields = FIELD_MAPS["2025+"]
            record.update(
                {
                    fields["mmsi"]: "000000001",
                    fields["time"]: (start + timedelta(seconds=seconds)).isoformat(),
                    fields["lon"]: str(lon),
                    fields["lat"]: str(lat),
                    fields["sog"]: speed,
                }
            )
            writer.writerow(record)
    receipt = SourcePartition(
        "noaa_marinecadastre",
        "fabricated",
        digest(source),
        start,
        start - timedelta(minutes=1),
        start + timedelta(days=1),
        "2025+",
        True,
        "fabricated source; no provider rights claim",
        "fabricated support only",
        "available",
    )
    ingestion_config = IngestionConfig(
        "fabricated",
        start,
        start + timedelta(days=1),
        start - timedelta(minutes=1),
        start + timedelta(days=1, minutes=1),
        SpatialFilter("fabricated", 19, 9, 21, 11, 0.1),
        "reject",
        7,
        1000,
        100000,
        10000,
        1024,
        8192,
        "e" * 40,
    )
    completed = ingest(
        [LocalInput(source, receipt)], ingestion_config, args.workspace / "ingestion"
    )

    def rectangle(radius):
        coords = [
            list(projection.inverse(x, y))
            for x, y in ((-radius, -radius), (radius, -radius), (radius, radius), (-radius, radius))
        ]
        return {"type": "Polygon", "coordinates": [coords + [coords[0]]]}

    geometry = {
        "domain_version": "fabricated/1",
        "water_mask_version": "fabricated-no-land/1",
        "aoi": rectangle(2000),
        "land": {"type": "MultiPolygon", "coordinates": []},
        "mask_support": rectangle(10000),
        "cells": sorted(h3.grid_disk(h3.latlng_to_cell(10, 20, 7), 4)),
    }
    job = args.workspace / "trajectory-job.json"
    job.write_text(
        json.dumps(
            {
                "config": asdict(cfg),
                "geometry": geometry,
                "ingestion_directory": str(completed.relative_to(args.workspace)),
                "ingestion_carry_directory": None,
                "carried_directory": None,
            },
            indent=2,
        )
    )
    print(job)


if __name__ == "__main__":
    main()
