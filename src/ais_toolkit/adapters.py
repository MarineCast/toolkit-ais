"""Pure header-based NOAA parsers. No network, decompression or source ordering."""

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from math import isfinite
from typing import Mapping, Protocol, Sequence

from .contracts import ParseResult, Position, Provenance, utc

DICTIONARY_VERSION = "NOAA-2026-07-31"
ADAPTER_VERSION = "marinecadastre/0.1"
# Full source event time, to the precision supported by datetime; never fill components.
TIMESTAMP_PATTERN = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]{1,6})?(?:Z|[+-][0-9]{2}:[0-9]{2})?"
)
LEGACY_HEADERS = (
    "MMSI",
    "BaseDateTime",
    "LAT",
    "LON",
    "SOG",
    "COG",
    "Heading",
    "VesselName",
    "IMO",
    "CallSign",
    "VesselType",
    "Status",
    "Length",
    "Width",
    "Draft",
    "Cargo",
    "TransceiverClass",
)
CURRENT_HEADERS = (
    "mmsi",
    "base_date_time",
    "longitude",
    "latitude",
    "sog",
    "cog",
    "heading",
    "vessel_name",
    "imo",
    "call_sign",
    "vessel_type",
    "status",
    "length",
    "width",
    "draft",
    "cargo",
    "transceiver",
)
FIELD_MAPS = {
    "2018-2024": dict(
        zip(
            ("mmsi", "time", "lat", "lon", "sog", "code", "equipment", "imo"),
            ("MMSI", "BaseDateTime", "LAT", "LON", "SOG", "VesselType", "TransceiverClass", "IMO"),
            strict=True,
        )
    ),
    "2025+": dict(
        zip(
            ("mmsi", "time", "lat", "lon", "sog", "code", "equipment", "imo"),
            (
                "mmsi",
                "base_date_time",
                "latitude",
                "longitude",
                "sog",
                "vessel_type",
                "transceiver",
                "imo",
            ),
            strict=True,
        )
    ),
}


class SourceAdapter(Protocol):
    provider: str
    version: str

    def validate_headers(self, headers: Sequence[str]) -> None: ...
    def parse(self, row: Mapping[str, str], provenance: Provenance) -> ParseResult: ...


def optional(value: str) -> str | None:
    return None if value.strip().lower() in ("", "null", "nan", "na") else value.strip()


@dataclass(frozen=True)
class MarineCadastreAdapter:
    era: str
    naive_time_policy: str = "reject"
    provider: str = "noaa_marinecadastre"
    version: str = ADAPTER_VERSION

    def __post_init__(self) -> None:
        if self.era not in FIELD_MAPS:
            raise ValueError("unsupported source era")
        if self.naive_time_policy not in ("reject", "dictionary_utc"):
            raise ValueError("time policy must be reject or dictionary_utc")

    def validate_headers(self, headers: Sequence[str]) -> None:
        expected = LEGACY_HEADERS if self.era == "2018-2024" else CURRENT_HEADERS
        if len(headers) != len(set(headers)):
            raise ValueError("duplicate column names")
        if set(headers) != set(expected):
            raise ValueError(
                "mixed/unknown schema: exact era field set required; order is irrelevant"
            )

    def parse(self, row: Mapping[str, str], provenance: Provenance) -> ParseResult:
        self.validate_headers(tuple(row))
        if (
            provenance.provider,
            provenance.source_era,
            provenance.dictionary_version,
            provenance.adapter_version,
        ) != (self.provider, self.era, DICTIONARY_VERSION, self.version):
            raise ValueError("adapter/source provenance mismatch")
        fields = FIELD_MAPS[self.era]
        get = lambda key: row[fields[key]]  # noqa: E731
        reasons: list[str] = []
        flags: list[str] = []
        mmsi = get("mmsi").strip()
        if len(mmsi) != 9 or not mmsi.isascii() or not mmsi.isdigit():
            reasons.append("invalid_mmsi")
        try:
            time_text = get("time")
            if TIMESTAMP_PATTERN.fullmatch(time_text) is None:
                raise ValueError("full event date/time through seconds required")
            timestamp = datetime.fromisoformat(time_text)
            if timestamp.tzinfo is None:
                if self.naive_time_policy == "reject":
                    raise ValueError("naive")
                timestamp = timestamp.replace(tzinfo=timezone.utc)
                flags.append("source_dictionary_utc_assumed")
            timestamp = utc(timestamp)
            # Do not silently accept records assigned to the wrong source-era partition.
            if (self.era == "2018-2024" and not 2018 <= timestamp.year <= 2024) or (
                self.era == "2025+" and timestamp.year < 2025
            ):
                reasons.append("timestamp_outside_source_era")
        except (ValueError, TypeError):
            timestamp = None
            reasons.append("invalid_or_naive_timestamp")
        coords: dict[str, float] = {}
        for key, limit in (("lat", 90), ("lon", 180)):
            try:
                number = float(get(key))
                if not isfinite(number) or not -limit <= number <= limit:
                    raise ValueError("coordinate")
                # Modern dictionary excludes exact pole/antimeridian values.
                if self.era == "2025+" and abs(number) > limit - 0.00001:
                    raise ValueError("coordinate outside current dictionary")
                coords[key] = number
            except (ValueError, TypeError):
                reasons.append(f"invalid_{key}")
        sog = None
        speed = optional(get("sog"))
        if speed is not None:
            try:
                candidate = float(speed)
                maximum = 99.9 if self.era == "2025+" else 102.2
                if not isfinite(candidate) or not 0 <= candidate <= maximum:
                    raise ValueError("speed")
                sog = candidate
            except ValueError:
                flags.append("invalid_or_sentinel_sog")
        else:
            flags.append("missing_sog")
        equipment = optional(get("equipment"))
        if equipment not in ("A", "B", None):
            flags.append("unknown_equipment_class")
        if reasons:
            return ParseResult(provenance, None, tuple(reasons))
        assert timestamp is not None
        return ParseResult(
            provenance,
            Position(
                mmsi,
                timestamp,
                coords["lon"],
                coords["lat"],
                sog,
                optional(get("code")),
                equipment,
                optional(get("imo")),
                provenance,
                tuple(flags),
            ),
            (),
        )


@dataclass(frozen=True)
class VesselClassification:
    raw_code: str | None
    category: str
    mapping_version: str


def classify_vessel(
    raw_code: str | None, mapping: Mapping[str, str], mapping_version: str
) -> VesselClassification:
    """Caller-supplied reviewed mapping; preserve unknowns, no behavior inference."""
    if not mapping_version:
        raise ValueError("mapping version required")
    return VesselClassification(raw_code, mapping.get(raw_code, "unknown"), mapping_version)
