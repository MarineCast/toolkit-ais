"""Native v0.1 contracts. These do not claim shared manifest conformance."""

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from enum import StrEnum
from math import isfinite
from types import MappingProxyType
from typing import Mapping
from zoneinfo import ZoneInfo

SCHEMA_VERSION = "ais-native/0.1"


def utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timezone-naive timestamp requires explicit source policy")
    return value.astimezone(timezone.utc)


def finite_nonnegative(value: float, name: str) -> None:
    if not isfinite(value) or value < 0:
        raise ValueError(f"{name} must be finite and nonnegative")


@dataclass(frozen=True)
class Provenance:
    provider: str
    source_era: str
    asset_id: str
    row_number: int
    dictionary_version: str
    adapter_version: str
    source_checksum: str | None = None

    def __post_init__(self) -> None:
        if (
            not all(
                (
                    self.provider,
                    self.source_era,
                    self.asset_id,
                    self.dictionary_version,
                    self.adapter_version,
                )
            )
            or self.row_number < 1
        ):
            raise ValueError("source identity/version and positive physical row number required")


@dataclass(frozen=True)
class Position:
    """One source message, EPSG:4326 degrees, UTC event time, knots reported SOG.

    MMSI is a source identifier, not proof of a globally resolved physical vessel.
    Invalid rows belong in ParseResult, never in this accepted-position contract.
    """

    mmsi: str
    timestamp: datetime
    longitude: float
    latitude: float
    sog_knots: float | None
    raw_vessel_code: str | None
    equipment_class: str | None
    imo: str | None
    provenance: Provenance
    quality_flags: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if len(self.mmsi) != 9 or not self.mmsi.isascii() or not self.mmsi.isdigit():
            raise ValueError("MMSI must be nine ASCII digits; identity remains unresolved")
        object.__setattr__(self, "timestamp", utc(self.timestamp))
        if not isfinite(self.longitude) or not -180 <= self.longitude <= 180:
            raise ValueError("invalid WGS84 longitude")
        if not isfinite(self.latitude) or not -90 <= self.latitude <= 90:
            raise ValueError("invalid WGS84 latitude")
        if self.sog_knots is not None:
            finite_nonnegative(self.sog_knots, "SOG")
            if self.sog_knots > 102.2:
                raise ValueError("SOG sentinel/out of AIS range")


@dataclass(frozen=True)
class ParseResult:
    provenance: Provenance
    position: Position | None
    rejection_reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        if (self.position is None) != bool(self.rejection_reasons):
            raise ValueError("accepted position XOR rejection reasons required")
        if self.position is not None and self.position.provenance != self.provenance:
            raise ValueError("parse result and position provenance must match")


class MetricStatus(StrEnum):
    # Exact shared v0.1 vocabulary: observed means valid, including derived estimates.
    OBSERVED = "observed"
    UNAVAILABLE = "unavailable"
    UNKNOWN = "unknown"
    NOT_APPLICABLE = "not_applicable"
    PARTIAL = "partial"


@dataclass(frozen=True)
class Coverage:
    """Metric-specific support, not receiver uptime or real-fleet completeness."""

    definition: str
    available: float | None
    expected: float | None
    unit: str
    evidence: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.definition or not self.unit or not self.evidence:
            raise ValueError("coverage meaning, units and evidence required")
        for name in ("available", "expected"):
            value = getattr(self, name)
            if value is not None:
                finite_nonnegative(value, name)
        if self.available is not None and self.expected is not None:
            if self.available > self.expected:
                raise ValueError("available support exceeds expected support")


@dataclass(frozen=True)
class MetricValue:
    value: float | None
    status: MetricStatus
    method: str
    coverage: Coverage
    reason: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.status, MetricStatus) or not self.method:
            raise ValueError("recognized status and explicit method required")
        if self.value is not None:
            finite_nonnegative(self.value, "metric")
        if self.status == MetricStatus.OBSERVED and self.value is None:
            raise ValueError("valid metric requires value")
        if self.status not in (MetricStatus.OBSERVED, MetricStatus.PARTIAL):
            if self.value is not None:
                raise ValueError("unsupported metric must be null, never zero")
        if self.status != MetricStatus.OBSERVED and not self.reason:
            raise ValueError("non-valid status requires reason")
        if self.status == MetricStatus.PARTIAL and self.value is not None:
            if self.coverage.available is None or self.coverage.expected is None:
                raise ValueError(
                    "finite partial estimate requires known available/expected support"
                )
            if self.coverage.available <= 0 or self.coverage.expected <= 0:
                raise ValueError("finite partial estimate requires positive support")


@dataclass(frozen=True)
class ProductConfig:
    domain_id: str | None = None
    geometry_version: str | None = None
    h3_resolution: int | None = None
    local_zone: str | None = None
    tzdb_version: str | None = None
    dayparts: tuple[tuple[str, int, int], ...] = ()

    def __post_init__(self) -> None:
        if self.h3_resolution is not None and self.h3_resolution not in (6, 7):
            raise ValueError("foundation candidates are R6 and R7")
        if self.local_zone is not None and not self.tzdb_version:
            raise ValueError("local companion requires pinned tzdb")
        if self.local_zone is not None:
            ZoneInfo(self.local_zone)
        if self.dayparts and not self.local_zone:
            raise ValueError("dayparts require an explicitly selected local clock")
        for _, start, end in self.dayparts:
            if not 0 <= start < end <= 24:
                raise ValueError("daypart boundaries must be increasing hours in [0,24]")
        names = [name for name, _, _ in self.dayparts]
        if any(not name for name in names) or len(names) != len(set(names)):
            raise ValueError("dayparts require unique nonempty labels")
        ordered = sorted(self.dayparts, key=lambda part: part[1])
        if any(a[2] > b[1] for a, b in zip(ordered, ordered[1:])):
            raise ValueError("daypart bins cannot overlap")

    def require_spatial_selection(self) -> None:
        if not self.domain_id or not self.geometry_version or self.h3_resolution is None:
            raise ValueError("domain, geometry version and resolution are unresolved")


@dataclass(frozen=True)
class DailyExport:
    """One cell/UTC-day/variant; future wide Parquet projection, no writer yet."""

    cell: str
    utc_date: date
    window_start: datetime
    window_end: datetime
    variant: str
    config: ProductConfig
    source_refs: tuple[str, ...]
    method_version: str
    metrics: Mapping[str, MetricValue]

    def __post_init__(self) -> None:
        self.config.require_spatial_selection()
        start, end = utc(self.window_start), utc(self.window_end)
        midnight = datetime.combine(self.utc_date, datetime.min.time(), timezone.utc)
        if start != midnight or end != midnight + timedelta(days=1):
            raise ValueError("canonical daily window must be [UTC midnight,next midnight)")
        if not self.cell or not self.variant or not self.source_refs or not self.method_version:
            raise ValueError("export identity and provenance required")
        from .metrics import METRICS

        names = {m.name for m in METRICS}
        if not self.metrics or not set(self.metrics) <= names:
            raise ValueError("export requires recognized metric names")
        if any(not isinstance(value, MetricValue) for value in self.metrics.values()):
            raise ValueError("export metric values must carry status, method and coverage")
        object.__setattr__(self, "metrics", MappingProxyType(dict(self.metrics)))
        object.__setattr__(self, "window_start", start)
        object.__setattr__(self, "window_end", end)


def validate_export_batch(rows: list[DailyExport]) -> None:
    """Reject duplicate row keys and mixed variant/resolution/metric schemas."""
    keys = [(r.cell, r.window_start, r.window_end, r.variant) for r in rows]
    if len(keys) != len(set(keys)):
        raise ValueError("duplicate export keys; indexes/row order are not join identity")
    schemas = {(r.config, r.variant, tuple(sorted(r.metrics))) for r in rows}
    if len(schemas) > 1:
        raise ValueError("one spatial config, variant and metric schema per export")


@dataclass(frozen=True)
class LocalWindow:
    """Future civil/daypart companion; cannot be relabeled as a UTC day."""

    civil_date: date
    zone: str
    tzdb_version: str
    label: str
    start_utc: datetime
    end_utc: datetime

    def __post_init__(self) -> None:
        if not self.zone or not self.tzdb_version or not self.label:
            raise ValueError("local clock, tzdb version and label required")
        ZoneInfo(self.zone)
        if utc(self.end_utc) <= utc(self.start_utc):
            raise ValueError("nonpositive local window")
        object.__setattr__(self, "start_utc", utc(self.start_utc))
        object.__setattr__(self, "end_utc", utc(self.end_utc))

    @property
    def duration_seconds(self) -> float:
        return (utc(self.end_utc) - utc(self.start_utc)).total_seconds()
