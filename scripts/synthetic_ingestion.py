"""Write a synthetic local-only job into an explicitly chosen NEW directory."""

import csv
import json
import sys
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import zstandard as zstd

from ais_toolkit.adapters import CURRENT_HEADERS
from ais_toolkit.ingestion import IngestionConfig, SpatialFilter, digest
from ais_toolkit.sources import SourcePartition


def main():
    workspace = Path(sys.argv[1])
    workspace.mkdir(mode=0o700, parents=True, exist_ok=False)
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    csv_path = workspace / "synthetic.csv"
    with csv_path.open("x", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=CURRENT_HEADERS)
        writer.writeheader()
        for seconds in (0, 0, 30):
            values = dict.fromkeys(CURRENT_HEADERS, "")
            values.update(
                mmsi="000000001",
                base_date_time=(start + timedelta(seconds=seconds)).isoformat(),
                longitude="20",
                latitude="10",
                sog="0",
                vessel_name="SYNTHETIC",
                transceiver="A",
            )
            writer.writerow(values)
    path = workspace / "synthetic.csv.zst"
    path.write_bytes(zstd.ZstdCompressor(write_checksum=True).compress(csv_path.read_bytes()))
    receipt = SourcePartition(
        "noaa_marinecadastre",
        "synthetic",
        digest(path),
        start,
        start,
        start + timedelta(days=1),
        "2025+",
        True,
        "fabricated fixture",
        "synthetic support only",
        "available",
    )
    config = IngestionConfig(
        "synthetic-day",
        start,
        start + timedelta(days=1),
        start,
        start + timedelta(days=1, minutes=1),
        SpatialFilter("synthetic-bounds", 19, 9, 21, 11, 0.1),
        "reject",
        7,
        100,
        100000,
        10000,
        1024,
        8192,
        "e" * 40,
    )

    def encode(value):
        return value.isoformat() if isinstance(value, datetime) else value

    job = {
        "config": {k: encode(v) for k, v in asdict(config).items()},
        "inputs": [
            {"path": path.name, "receipt": {k: encode(v) for k, v in asdict(receipt).items()}}
        ],
    }
    (workspace / "job.json").write_text(json.dumps(job, indent=2))
    print("Synthetic files written; ingestion remains an explicit separate command.")


if __name__ == "__main__":
    main()
