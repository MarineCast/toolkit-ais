"""Definitions only. No unsupported metric becomes a fabricated observed zero."""

from dataclasses import dataclass


@dataclass(frozen=True)
class MetricDefinition:
    name: str
    unit: str
    method_kind: str
    definition: str
    denominator: str
    reduction: str


METRICS = tuple(
    MetricDefinition(*row)
    for row in (
        (
            "unique_observed_vessels",
            "identities/window",
            "point_observation",
            "Resolved identity union with accepted positions in cell/window",
            "accepted point frame",
            "identity union; never sum subgroup counts",
        ),
        (
            "unique_estimated_present_vessels",
            "identities/window",
            "track_estimate",
            "Resolved identity union intersecting accepted allocated intervals",
            "accepted interval frame",
            "identity union; never sum subgroup counts",
        ),
        (
            "vessel_hours",
            "vessel h",
            "track_estimate",
            "Allocated accepted interval seconds / 3600, including stationary support",
            "accepted interval support; no fleet-completeness correction",
            "sum",
        ),
        (
            "vessel_km",
            "vessel km",
            "track_estimate",
            "Accepted segment length allocated within cell/window",
            "accepted interval support",
            "sum",
        ),
        (
            "reported_sog_time_weighted_knots",
            "knots",
            "reported_speed_reduction",
            "Sum reported SOG integrals (knot-seconds) / sum speed-supported seconds; "
            "using declared endpoint policy",
            "speed-supported seconds; unknown-speed intervals excluded; "
            "divide seconds by 3600 only when reporting support hours",
            "sum knot-seconds / sum supported seconds = knots; zero supported seconds yields null",
        ),
        (
            "speed_band_hours",
            "vessel h",
            "track_estimate",
            "Accepted interval time by reviewed speed bins plus unknown-speed bin",
            "accepted interval support; thresholds unresolved",
            "sum within same variant/band",
        ),
        (
            "mean_supported_concurrency",
            "vessels",
            "track_estimate",
            "Vessel-hours / full window hours",
            "full window hours, not received uptime",
            "sum vessel-hours / window hours",
        ),
    )
)
