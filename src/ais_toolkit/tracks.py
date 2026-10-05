"""Foundation validation/interfaces; the bounded local engine lives in trajectories.py."""

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from math import isclose
from typing import Iterable, Protocol

from .contracts import ParseResult, Position, Provenance, finite_nonnegative, utc


@dataclass(frozen=True)
class IdentityDecision:
    source_identity: str
    resolved_identity: str | None
    status: str
    evidence: tuple[Provenance, ...]
    resolver_version: str


class IdentityResolver(Protocol):
    """Future dated identity linkage; MMSI reuse and cross-feed overlap require evidence."""

    def resolve(self, positions: Iterable[Position]) -> Iterable[IdentityDecision]: ...


@dataclass(frozen=True)
class MessageGroup:
    mmsi: str
    timestamp: datetime
    outcome: str  # unique / duplicate / conflict
    messages: tuple[Position, ...]


def reconcile_messages(positions: Iterable[Position]) -> tuple[MessageGroup, ...]:
    """Bounded audit; retain all source rows. Never resolve conflicts by first-row choice.

    Simultaneous messages must match all retained semantic attributes to be duplicates.
    A dataframe index is never an identity key; callers supply physical source row IDs.
    """
    groups: dict[tuple[str, datetime], list[Position]] = defaultdict(list)
    for position in positions:
        groups[position.mmsi, position.timestamp].append(position)
    result = []
    for (mmsi, timestamp), messages in sorted(groups.items()):
        signatures = {
            (
                p.longitude,
                p.latitude,
                p.sog_knots,
                p.raw_vessel_code,
                p.equipment_class,
                p.imo,
                p.quality_flags,
            )
            for p in messages
        }
        outcome = (
            "conflict" if len(signatures) > 1 else ("duplicate" if len(messages) > 1 else "unique")
        )
        # Stable audit order independent of input ordering.
        messages.sort(
            key=lambda p: (
                p.provenance.provider,
                p.provenance.asset_id,
                p.provenance.row_number,
                repr(p),
            )
        )
        result.append(MessageGroup(mmsi, timestamp, outcome, tuple(messages)))
    return tuple(result)


@dataclass(frozen=True)
class TrackConfig:
    max_gap_seconds: float  # Explicit sensitivity setting, no scientific default.
    gap_policy: str = "reject"
    extrapolate: bool = False
    bridge_invalid_samples: bool = False

    def __post_init__(self) -> None:
        finite_nonnegative(self.max_gap_seconds, "maximum gap")
        if self.max_gap_seconds == 0:
            raise ValueError("positive explicit maximum gap required")
        if self.gap_policy != "reject" or self.extrapolate or self.bridge_invalid_samples:
            raise ValueError("foundation forbids extrapolation or bridging gaps/invalid samples")


@dataclass(frozen=True)
class IntervalDecision:
    left: Provenance
    right: Provenance
    outcome: str
    reasons: tuple[str, ...]
    supported_seconds: float | None
    stationary_candidate: bool


def assess_interval(
    left: ParseResult, right: ParseResult, config: TrackConfig, *, identity_conflict: bool = False
) -> IntervalDecision:
    """Assess consecutive original same-identity samples; rejected rows are barriers.

    No sorting/drop-invalid convenience is supplied. The future processor must carry
    conflict and rejection barriers across partition boundaries and class changes.
    Eligibility is not acceptance: geometry, displacement and identity review remain.
    """
    reasons: list[str] = []
    a, b = left.position, right.position
    if a is None or b is None:
        reasons.append("invalid_sample_barrier")
    if identity_conflict:
        reasons.append("identity_conflict")
    seconds = None
    stationary = False
    if a is not None and b is not None:
        if a.mmsi != b.mmsi or (a.imo and b.imo and a.imo != b.imo):
            reasons.append("identity_conflict")
        seconds = (b.timestamp - a.timestamp).total_seconds()
        if seconds <= 0:
            reasons.append("nonpositive_or_unordered_time")
        elif seconds > config.max_gap_seconds:
            reasons.append("excessive_gap")
        stationary = (a.longitude, a.latitude) == (b.longitude, b.latitude)
    return IntervalDecision(
        left.provenance,
        right.provenance,
        "rejected" if reasons else "pending_scientific_validation",
        tuple(sorted(set(reasons))),
        None if reasons else seconds,
        stationary,
    )


@dataclass(frozen=True)
class AcceptedInterval:
    """Future validated estimated interval; construction is not scientific certification."""

    interval_id: str
    resolved_identity: str
    start: datetime
    end: datetime
    distance_km: float
    endpoint_sources: tuple[Provenance, Provenance]
    method_version: str
    validation_evidence: tuple[str, ...]

    def __post_init__(self) -> None:
        if utc(self.end) <= utc(self.start):
            raise ValueError("accepted intervals must have positive UTC duration")
        object.__setattr__(self, "start", utc(self.start))
        object.__setattr__(self, "end", utc(self.end))
        finite_nonnegative(self.distance_km, "distance")
        if not all(
            (
                self.interval_id,
                self.resolved_identity,
                self.method_version,
                self.validation_evidence,
            )
        ):
            raise ValueError("interval identity, method and validation evidence required")


@dataclass(frozen=True)
class Contribution:
    """Future private vessel-cell/window evidence, half-open intervals."""

    interval_id: str
    resolved_identity: str
    cell: str
    start: datetime
    end: datetime
    distance_km: float
    speed_supported_seconds: float
    reported_sog_integral_knot_seconds: float | None
    variant: str

    def __post_init__(self) -> None:
        duration = (utc(self.end) - utc(self.start)).total_seconds()
        if duration <= 0:
            raise ValueError("contribution must have positive duration")
        object.__setattr__(self, "start", utc(self.start))
        object.__setattr__(self, "end", utc(self.end))
        if not all((self.interval_id, self.resolved_identity, self.cell, self.variant)):
            raise ValueError("contribution identity required")
        finite_nonnegative(self.distance_km, "distance")
        finite_nonnegative(self.speed_supported_seconds, "speed support")
        if self.speed_supported_seconds > duration:
            raise ValueError("speed support exceeds contribution duration")
        if self.reported_sog_integral_knot_seconds is not None:
            finite_nonnegative(self.reported_sog_integral_knot_seconds, "SOG integral")
            if self.speed_supported_seconds == 0:
                raise ValueError("speed integral requires positive speed support")
        elif self.speed_supported_seconds:
            raise ValueError("positive speed support requires speed integral")


def validate_conservation(
    interval: AcceptedInterval,
    parts: Iterable[Contribution],
    *,
    seconds_tolerance: float,
    km_tolerance: float,
) -> None:
    """Validate a full-interval synthetic allocation, including stationary time.

    Domain-clipped intervals must be supplied as explicitly clipped parents. This
    check does not validate geometry/cell ownership or turn parts into qualified data.
    """
    finite_nonnegative(seconds_tolerance, "time tolerance")
    finite_nonnegative(km_tolerance, "distance tolerance")
    ordered = sorted(parts, key=lambda p: utc(p.start))
    cursor = utc(interval.start)
    distance = 0.0
    variants = set()
    for part in ordered:
        if (
            part.interval_id != interval.interval_id
            or part.resolved_identity != interval.resolved_identity
        ):
            raise ValueError("contribution parent/identity mismatch")
        if not isclose((utc(part.start) - cursor).total_seconds(), 0, abs_tol=seconds_tolerance):
            raise ValueError("contribution overlap or gap")
        cursor = utc(part.end)
        distance += part.distance_km
        variants.add(part.variant)
    if len(variants) != 1 or not isclose(
        (cursor - utc(interval.end)).total_seconds(), 0, abs_tol=seconds_tolerance
    ):
        raise ValueError("contributions do not cover one parent variant")
    if not isclose(distance, interval.distance_km, rel_tol=0, abs_tol=km_tolerance):
        raise ValueError("distance not conserved")


class TrackProcessor(Protocol):
    """Future engine must emit accepted AND rejected intervals with source row links."""

    def process(
        self, samples: Iterable[ParseResult], config: TrackConfig, carried_state: bytes | None
    ) -> tuple[Iterable[AcceptedInterval | IntervalDecision], bytes]: ...


class ExactAllocator(Protocol):
    """Future exact cell-polygon/domain/time clipping, direct R6/R7 geometry.

    Must include stationary dwell, deterministic edge ownership, explicit outside-
    domain remainder and conservation diagnostics. H3 paths are not dwell allocation.
    """

    def allocate(self, interval: AcceptedInterval) -> Iterable[Contribution]: ...
