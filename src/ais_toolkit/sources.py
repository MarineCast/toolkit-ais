"""Catalog evidence is not acquired availability, rights approval or complete coverage."""

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Mapping, Protocol, Sequence

from .contracts import utc


@dataclass(frozen=True)
class SourceCatalogEntry:
    provider: str
    reference_url: str | None
    delivery: str | None
    adapter_status: str
    access_status: str
    coverage_claim: str


CATALOG = (
    SourceCatalogEntry(
        "noaa_marinecadastre",
        "https://hub.marinecadastre.gov/pages/vesseltraffic",
        "CSV.zst (2015 onward); implemented parser eras start 2018",
        "offline_local_ingestion",
        "no_acquisition_authorized",
        "US received AIS; regional/fleet completeness unverified",
    ),
    SourceCatalogEntry(
        "canada_configurable",
        None,
        None,
        "unavailable_pending_access",
        "unverified",
        "No Canadian continuity or terms established",
    ),
)


@dataclass(frozen=True)
class SourcePartition:
    provider: str
    asset_id: str
    sha256: str | None
    acquired_at: datetime | None
    start: datetime
    end: datetime
    source_era: str
    schema_verified: bool
    rights_evidence: str | None
    coverage_evidence: str | None
    availability: str  # catalogued / available / unavailable / quarantined

    def __post_init__(self) -> None:
        if utc(self.end) <= utc(self.start):
            raise ValueError("partition bounds must be positive and timezone aware")
        object.__setattr__(self, "start", utc(self.start))
        object.__setattr__(self, "end", utc(self.end))
        if self.acquired_at is not None:
            object.__setattr__(self, "acquired_at", utc(self.acquired_at))
        if self.availability not in ("catalogued", "available", "unavailable", "quarantined"):
            raise ValueError("invalid partition availability")

    def require_normalization(self) -> None:
        if self.provider != "noaa_marinecadastre":
            raise ValueError("provider unavailable pending adapter/access validation")
        if self.source_era not in ("2018-2024", "2025+"):
            raise ValueError("unsupported era")
        if self.availability != "available" or not self.schema_verified:
            raise ValueError("partition unavailable or schema unverified")
        if (
            not self.sha256
            or len(self.sha256) != 64
            or any(c not in "0123456789abcdef" for c in self.sha256)
        ):
            raise ValueError("verified source SHA-256 required")
        if not self.acquired_at or not self.rights_evidence or not self.coverage_evidence:
            raise ValueError("acquisition, rights and coverage evidence required")


class PartitionReader(Protocol):
    """Future projected/streamed CSV.zst or Parquet reader; no dense time-cell grid."""

    def batches(
        self, partition: SourcePartition, columns: tuple[str, ...], batch_size: int
    ) -> Iterable[Sequence[Mapping[str, str]]]: ...
