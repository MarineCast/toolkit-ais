"""Explicit local inspection and ingestion commands; no acquisition."""

import argparse
import csv
import json
from dataclasses import asdict
from pathlib import Path

from .adapters import ADAPTER_VERSION, DICTIONARY_VERSION, MarineCadastreAdapter
from .contracts import Provenance
from .metrics import METRICS
from .sources import CATALOG
from .tracks import reconcile_messages


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("inspect-catalog", help="show catalog evidence and capability gates")
    commands.add_parser("inspect-metrics", help="show definitions, units and reductions")
    ingestion = commands.add_parser(
        "ingest-local", help="offline local CSV/CSV.zst to private native Parquet"
    )
    ingestion.add_argument("job", type=Path)
    ingestion.add_argument("output", type=Path)
    ingestion.add_argument(
        "--carried", type=Path, help="previous verified adjacent-core ingestion directory"
    )
    tracks = commands.add_parser(
        "estimate-local", help="verified offline ingestion to private local estimated contributions"
    )
    tracks.add_argument("job", type=Path)
    tracks.add_argument("output", type=Path)
    validate = commands.add_parser("validate-csv", help="bounded, offline CSV contract inspection")
    validate.add_argument("path", type=Path)
    validate.add_argument("--era", choices=("2018-2024", "2025+"), required=True)
    validate.add_argument(
        "--naive-time-policy", choices=("reject", "dictionary_utc"), default="reject"
    )
    validate.add_argument("--max-rows", type=int, default=10000)
    args = parser.parse_args(argv)
    if args.command == "estimate-local":
        import sqlite3

        import pyarrow as pa
        from shapely.errors import ShapelyError

        from .trajectories import load_track_job, process_tracks

        try:
            source, config, geometry, ingestion_carry, carried = load_track_job(args.job)
            completed = process_tracks(
                source,
                config,
                geometry,
                args.output,
                ingestion_carry_directory=ingestion_carry,
                carried_directory=carried,
            )
            print(
                json.dumps(
                    {
                        "status": "complete",
                        "native_directory": str(completed),
                        "scope": "private estimated contributions; unresolved identity; no final daily products",
                    }
                )
            )
            return 0
        except (
            OSError,
            ValueError,
            TypeError,
            KeyError,
            sqlite3.Error,
            pa.ArrowException,
            ShapelyError,
        ) as error:
            print(json.dumps({"status": "failed", "reason": str(error)}))
            return 2
    if args.command == "ingest-local":
        import sqlite3

        import zstandard as zstd

        from .ingestion import ingest, load_job

        try:
            inputs, config = load_job(args.job)
            completed = ingest(inputs, config, args.output, args.carried)
            print(
                json.dumps(
                    {
                        "status": "complete",
                        "native_directory": str(completed),
                        "scope": "offline ingestion; no tracks or final H3 products",
                    }
                )
            )
            return 0
        except (
            OSError,
            ValueError,
            TypeError,
            KeyError,
            csv.Error,
            zstd.ZstdError,
            sqlite3.Error,
        ) as error:
            print(json.dumps({"status": "failed", "reason": str(error)}))
            return 2
    if args.command == "inspect-catalog":
        print(json.dumps([asdict(s) for s in CATALOG], indent=2))
        return 0
    if args.command == "inspect-metrics":
        print(json.dumps([asdict(m) for m in METRICS], indent=2))
        return 0
    if args.max_rows < 1:
        parser.error("max-rows must be positive")
    adapter = MarineCadastreAdapter(args.era, args.naive_time_policy)
    positions, rejected = [], []
    try:
        with args.path.open(newline="", encoding="utf-8") as stream:
            reader = csv.reader(stream, strict=True)
            headers = next(reader, [])
            adapter.validate_headers(headers)
            record_count = 0
            while True:
                row_number = reader.line_num + 1
                values = next(reader, None)
                if values is None:
                    break
                if not values:
                    continue  # Blank physical lines are not records, but retain their offsets.
                record_count += 1
                if record_count > args.max_rows:
                    raise ValueError("row cap exceeded: no partial-success report")
                if len(values) != len(headers):
                    raise ValueError(f"malformed CSV row width at physical line {row_number}")
                row = dict(zip(headers, values, strict=True))
                source = Provenance(
                    adapter.provider,
                    args.era,
                    args.path.name,
                    row_number,
                    DICTIONARY_VERSION,
                    ADAPTER_VERSION,
                )
                result = adapter.parse(row, source)
                if result.position is None:
                    rejected.append({"row_number": row_number, "reasons": result.rejection_reasons})
                else:
                    positions.append(result.position)
        groups = reconcile_messages(positions)
        conflicts = sum(g.outcome == "conflict" for g in groups)
        print(
            json.dumps(
                {
                    "scope": "offline contract inspection; no scientific or source qualification",
                    "accepted_rows": len(positions),
                    "rejected_rows": rejected,
                    "duplicate_rows": sum(
                        len(g.messages) - 1 for g in groups if g.outcome == "duplicate"
                    ),
                    "conflict_groups": conflicts,
                    "quality_flag_rows": sum(bool(p.quality_flags) for p in positions),
                    "track_processing": "unimplemented",
                },
                indent=2,
            )
        )
        return 1 if rejected or conflicts or not positions else 0
    except (OSError, ValueError, csv.Error) as error:
        print(json.dumps({"error": str(error), "partition_status": "quarantined"}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
