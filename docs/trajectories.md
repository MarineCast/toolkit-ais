# Phase 3: private, offline estimated trajectories and contributions

`ais estimate-local job.json /chosen/private-output` consumes **verified Phase 2**
ingestion evidence. It does not acquire AIS, resolve physical vessel identity, select
scientific defaults, generate final daily wide products or qualify ecological uses.
All fixtures are fabricated. Hidden-fix interpolation accuracy, realistic memory/runtime,
source rights/coverage, Canadian access and regional products remain separate gates.

The native method is `estimated-local-trajectories/0.1`, schema `ais-contributions/0.1`.
Four closed Parquet artifacts carry method/schema metadata and checksums:

- `intervals.parquet`: accepted **estimates** and rejected original adjacencies, with
  endpoint JSON (source asset/checksum/physical row, raw attributes, QC and reasons),
  MMSI episode, original UTC bounds, original projected geometry WKT, core-owned
  distance/time, inside/outside remainders and numerical allocation bound.
- `contributions.parquet`: private MMSI-episode/cell/UTC-day intervals. A null cell
  explicitly records outside-AOI time/distance. Store interval and endpoint record keys,
  seconds, vessel-hours, km, reported-SOG numerator/support, method and raw class attributes.
- `points.parquet`: accepted core observations remain available even if adjacent intervals
  fail gap/speed/land validation. Unsupported geometry has a null cell and explicit status;
  outside-AOI observations remain source evidence. No message-count traffic claim follows.
- `state.parquet`: last original pre-core-end event, episode and conservative barrier bit.
  Unknown-time and rejected interval history cannot be erased by a later valid fix.

The complete receipt links the full input ingestion receipt/audit, carry receipt chain,
config/geometry/software hashes, producer Git SHA, versions, caps and output hashes.
Every rejected source message remains in the referenced Phase 2 audit. Unidentifiable
rows have no claimed vessel interval. Interval ledgers are original-adjacency decisions;
rejected hours are unknown when a supporting timestamp is unknown. No null is replaced
with zero and no completed run implies regional/fleet completeness.

## Explicit configuration and supported geometry

Job JSON requires exactly `config`, `geometry`, `ingestion_directory`,
`ingestion_carry_directory`, `carried_directory`; use explicit nulls for absent carries.
Paths are local, relative to the job file or deliberately absolute. Duplicate JSON keys,
nonfinite values, unknown fields and jobs above 1 MiB are rejected. Configuration has
no default thresholds, resolution, real domain or local clock. Required fields are:

| Setting | Meaning/limit |
| --- | --- |
| `max_gap_seconds` | Positive, <=1800; 120/300/600/1800 are test scenarios, not validated defaults |
| `max_implied_speed_knots` | Positive <=102.2; projection error envelope included conservatively |
| `material_land_crossing_m`, `shoreline_uncertainty_m` | Positive material threshold; nonnegative mask uncertainty in local metres |
| `center_longitude`, `center_latitude`, `projection_radius_m` | Explicit local spherical gnomonic center/radius; <=100 km |
| `max_relative_metric_error` | Required tolerance covering the computed WGS84 metric envelope; <=0.02 |
| `numerical_tolerance_m` | Positive <=0.01 m; numerical cut/edge ownership budget, not source-position accuracy |
| `seconds_tolerance`, `km_tolerance` | Explicit conservation checks, at least 1e-5 s/1e-8 km |
| `resolution` | Direct R6 or R7, one resolution per run |
| `batch_rows`, `max_records` | Positive caps <=10000/10000000; input and each carry also capped |
| `max_cells`, `max_candidates` | Explicit finite tile/cache and per-route candidates, <=5000/512 |
| `max_geometry_vertices`, `max_contributions`, `sqlite_cache_kib` | Per supplied polygonal geometry, run output and SQLite caps <=100000/10000000/65536 |
| `producer_git_sha` | Full lower-case 40-character software revision |

Geometry requires `domain_version`, `water_mask_version`, `aoi`, `land`, `mask_support`
and explicit unique canonical lower-case `cells`. Geometry version labels must be nonempty text. Polygon/MultiPolygon GeoJSON coordinates are longitude,
latitude; **ring edges mean short spherical great-circle segments**. Holes and islands
are retained. Empty land is allowed only as an explicit caller assertion within the
supplied mask-support polygon; it is not inferred from missing geometry. Version labels
and mask claims are caller evidence, not independent coastal qualification. The explicit
H3 tile must cover the AOI within the numerical tolerance, and each cell must be at the
chosen resolution. Any missing interior cell allocation fails the run.

## Geometry model, precision and independent reference

H3 uses a sphere with WGS84 authalic radius; actual `cell_to_boundary` vertices (including
face-intersection vertices) are used at **each requested resolution**. The spherical
[gnomonic projection](https://proj.org/en/stable/operations/projections/gnom.html) maps
short great-circle edges to straight lines, so these H3 edges do not require time sampling,
route grid paths or curved-edge densification. [H3's geometry overview](https://h3geo.org/docs/core-library/overview/)
describes the inverse face-centered gnomonic construction. Logical R7 parent aggregation
is not implemented or treated as R6 geometry.

The estimated path is a straight local projected segment at **constant projected speed**.
It is a spherical great-circle route with that declared local time parameterization,
not an ellipsoidal constant-geodesic-speed trajectory. WKT coordinates are local metres,
with center and sphere specified in the receipt. No navigation around land is invented.
All endpoints and polygon vertices must lie within the configured spherical radius;
projection centers beyond +/-74 latitude or +/-178 longitude, coordinates beyond +/-75
latitude, antimeridian crossings and distant points are unsupported. Unsupported vessel
routes are rejected and observed endpoints retained; unsupported configured geometry
fails preflight. This is a local-tile engine, not a national/geographic partition planner.

For angular radius theta, gnomonic directional scale lies between 1 and sec(theta)^2.
For WGS84 a=6378137 and f=1/298.257223563, meridional radius has minimum a(1-e^2)
and transverse radius maximum a/sqrt(1-e^2). Comparing R=6371007.180918475 with those
extremes bounds the sphere/geodetic metric discrepancy in every direction. The recorded
relative envelope is `(1+max(abs(R/Mmin-1),abs(R/Nmax-1)))*sec(theta)^2-1`, about 0.57%
for a 50 km tile. The disk is convex in this projection, so an endpoint-bounded straight
route stays within that envelope. This bounds the metric of the **same coordinate path**;
it does not bound error against an unknown real vessel route. Configurations that request
a tighter tolerance are rejected rather than advertised as more accurate.

GEOS performs intersections on the projected model in double precision. All intersection
parameters, AOI edges, cell edges and UTC/core cuts partition the original [0,1] interval.
Near-coincident cuts within epsilon metres are merged deterministically; boundary/corner
ownership is the lexicographically smallest touching cell ID within epsilon. Interior
samples with no owner fail; geometry gaps are not silently outside AOI. A per-interval
`numerical_allocation_error_bound_m` reports a conservative **ownership perturbation**
budget of 2*epsilon*spatial_part_count (zero-distance cases carry no length). It does not
claim a formal GEOS floating-point error bound. Cell-level time perturbation for a moving
route is bounded by this budget/L*duration plus timestamp rounding of <=1 microsecond
per boundary; very short legs can have large ownership uncertainty relative to duration.
Stationary edge/corner dwell uses the same ID rule. Point-presence ownership and modeled
interval presence are separate records. Source/mask uncertainty is separate from epsilon.

Tests compare full cell allocations with an independent convex half-plane line clipper
at both resolutions. Separately, H3 spherical distance and independent 3-D spherical
interpolation/chord integration check the projection envelope and numerical convergence;
the latter has chord-length relative error <=angle^2/(24*n^2). These are synthetic geometry
checks, not empirical AIS interpolation accuracy or a certified production error budget.

## Adjacency, barriers, land and checkpoint ownership

The full audit is spilled into SQLite, ordered by MMSI/exact UTC/source row. Original
rejected or filtered intermediate events remain adjacency barriers; duplicates alone
are skipped. Conflicts, changed known IMO, gaps and implied-speed jumps reject intervals
and start new unresolved source-MMSI episodes. An episode is a traceable evidence group,
not a resolved physical vessel. A wholly invalid Phase 2 fix has no normalized timestamp;
that MMSI's intervals are conservatively quarantined for the whole partition. Its valid
observed endpoints remain in the point output. No timestamp is guessed from a dropped row.

A segment outside mask support is unavailable. Land-interior crossing (mask eroded by
uncertainty) above the material threshold rejects the whole segment. Any remaining
intersection with the uncertainty-expanded mask is `shoreline_uncertainty` and also
withholds the whole segment. Stationary interior land points fail too. Land pieces are
never removed and their full original time redistributed onto water. Such conservative
withholding needs real narrow-channel/shoreline validation before scientific selection.

A referenced ingestion carry must be supplied and verified, with exactly matching receipt
hash/run key and adjacent core. Optional trajectory carry also requires identical full
policy/geometry/software/producer hashes and the referenced previous ingestion identity.
Both ingestion and trajectory barriers suppress initial cross-core bridging; a clean new
core fix can begin a new episode for subsequent clean within-core legs. A matching repeated
carried endpoint adds no fabricated row or zero-time leg. Earlier supplied halos replace
stale endpoint context in sorted order, while conservative inherited barriers remain.
Only interval portions inside the half-open ingestion core are emitted. UTC midnight
cuts then produce day-owned parts; future halos do not advance the pre-core-end checkpoint.
This ownership assumes one chosen run per nonoverlapping core. The library is not a catalog
that automatically suppresses overlapping alternative runs/configurations.

Every accepted core-owned interval reconciles in/out duration and distance to its parent
within the required tolerances, including stationary dwell and crossings whose endpoints
are outside the AOI. Reported SOG is linearly interpolated **only if both endpoints have
valid SOG**. Its integral is knot-seconds and denominator supported seconds; missing
speed yields null numerator, zero *known support*, `unknown` status. Positive stationary
duration can validly have zero distance and reported SOG zero. Changed raw class/equipment
makes interval class attribution unknown; original attributes remain in endpoint JSON.
Class mapping and dated fleet linkage remain separate. Counts require identity/episode
unions and are not additive; no final count product is emitted here.

## Resource bounds, completion and limits

SQLite has file-backed sorting and explicit cache bounds. Parquet readers/writers use
bounded batches, one projected route at a time, a capped prebuilt H3 polygon STRtree and
capped candidates. Per-route break/part lists are bounded by supplied geometry/candidate
caps; they are not claimed to equal the writer batch size. No second-level vessel grid,
H3 route path, constant process RSS or throughput qualification is used. Native Arrow/GEOS
buffers, row-group metadata, OS caches, disk usage and region tiling still need a pilot.

Phase 2's publication/checksum helpers are reused. Writers close before checksummed receipt
creation and same-filesystem directory rename. Failed publication invalidates completion;
failed/staging directories cannot serve as verified retries or carries. Completed corruption
fails instead of overwriting. Sources/carries are reverified before publication to detect
ordinary concurrent mutation. No power-loss durability or atomic multi-partition transaction
is claimed. Files/directories are private, 0600/0700; no source/AIS cleanup is performed.

Run `pytest -q`, `ruff check .`, `ruff format --check .`, `python -m build --outdir dist-current`,
and `python scripts/wheel_smoke.py dist-current`. The fresh wheel checker runs all synthetic
end-to-end ingestion/trajectory/CLI tests outside the checkout. Scientific defaults, approved
study AOI/water mask, R6/R7 choice, local jurisdiction/dayparts, final wide summaries,
identity resolution, fleet completeness, hidden-fix validation and consumer integration
remain unresolved. No noise, disturbance, people-onboard or whale truth is derived here.
