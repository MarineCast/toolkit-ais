# Foundation milestone: native contract version ais-native/0.1

This describes the bounded foundation from the [core design](https://chatgpt.com/space/page_ebb0dab5d82881919c98310c9a011ddc)
and [implementation plan](https://chatgpt.com/space/page_5a867c6287d08191a837f84676135248).
Typed frozen dataclasses and their constructor checks are the executable native schemas;
they are not a promise of validated movement science or a shared manifest adoption.
No existing legacy data is read, removed, migrated or recertified.

The subsequent [offline ingestion milestone](ingestion.md) implements local CSV/CSV.zst
reading, disk-spilled reconciliation, private native Parquet, halo/carry state and atomic
completion. Its implemented scope supersedes the ingestion/Parquet deferrals below; track
science, exact allocation and final daily-product conformance remain deferred.

## Source evidence, checked 2026-10-05

The official [NOAA hub](https://hub.marinecadastre.gov/pages/vesseltraffic) is the catalog
entry point. The [dictionary](https://coast.noaa.gov/data/marinecadastre/ais/data-dictionary.pdf),
updated 2026-07-31, documents separate 2018–2024 and 2025-present point schemas. The latter
uses snake-case fields, longitude before latitude, and `transceiver` for equipment class;
vessel type remains a distinct field. Both specify UTC times, geographic coordinates and
reported SOG in knots. The [2025 archive index](https://noaaocm.blob.core.windows.net/ais/csv2/csv2025/index.html)
lists daily CSV.zst assets. An index entry is not proof of acquired, validated or complete
regional coverage. No AIS asset was downloaded to develop this milestone.

The current delivery route covers 2015 onward according to the design review; **implemented
parser eras start at 2018**. Earlier schema support and decompression are deferred.
Canadian provider configuration is an unavailable catalog entry pending actual access,
terms, schema and continuity evidence. There is no Canadian adapter or fake fallback to US
coverage. Source partitions gate normalization on acquisition, checksum, schema, rights
and coverage evidence. Real-fleet completeness and receiver uptime remain unverified.

## Executable contracts and grain

| Contract | Grain, identity and validation |
| --- | --- |
| `Provenance` | Provider, era, asset, physical source row number, dictionary and adapter versions; optional source checksum. An array/dataframe index is never a key. |
| `Position` | One accepted message; nine-digit source MMSI, UTC event time, WGS84 degrees, optional reported SOG in knots, raw type code, equipment class, IMO and flags. MMSI is not a resolved physical vessel identity. |
| `ParseResult` | Exactly one accepted position or explicit rejection reasons, always retaining source row provenance. Invalid coordinates reject a row; missing/sentinel/invalid SOG becomes null with flags, preserving valid position support. |
| `IdentityDecision` | Future evidence-backed resolved identity or unresolved/conflict outcome; resolver version and all supporting source rows. No automatic MMSI-to-physical-vessel certification. |
| `MessageGroup` | Source MMSI × event time; unique, exact semantic duplicate or conflict. All source rows survive; conflicts are never arbitrarily reduced to a first record. |
| `IntervalDecision` | Consecutive original endpoint rows, eligibility/rejection, reasons and stationary candidate support. Passing bounded checks remains pending scientific validation. |
| `AcceptedInterval` | Future validated estimated interval ID and resolved vessel identity; half-open UTC bounds, km, endpoint sources, method and validation evidence. Construction checks do not certify science. |
| `Contribution` | Private interval × vessel × cell × half-open window × variant; km, speed-supported seconds and optional knot-second SOG integral. Geometry ownership and exact allocation remain unimplemented. |
| `DailyExport` | Cell × UTC midnight/start/end × variant; one domain/geometry/resolution and metric schema per batch, source references, method version and per-metric status/coverage. No H3 geometry validation or Parquet serialization yet. |
| `LocalWindow` | Separate civil date/daypart, IANA zone, explicit tzdb version, actual UTC bounds and duration. No default clock or daypart bins. |
| `ProcessingManifest` | Partition/core/halo bounds, source/config/state hashes, producer Git revision and method/schema versions. Deterministic reuse key; future ledger must verify outputs before reuse and commit atomically. |

Pure parsers validate complete headers by name, reject mixed/duplicate headers and verify
source-era provenance and event-time era. They reject naive timestamps unless the caller
explicitly invokes dictionary UTC policy, recording that assumption. Modern SOG is limited
to 0–99.9; legacy AIS 102.3 sentinel becomes unknown, never zero. Unknown vessel codes and
equipment classes survive. Vessel function mapping is caller-supplied and versioned;
there is no default behavior, fishing or whale-watch classification.

Event timestamps require `YYYY-MM-DDTHH:MM:SS`, optionally 1–6 fractional digits and
`Z` or a `±HH:MM` offset. Dictionary UTC policy supplies only the zone; it never invents
time components. Greater-than-microsecond precision is rejected pending a native contract
that can preserve it. CSV provenance records the physical starting line of each record,
including quoted multiline values and blank-line offsets; the inspection cap counts
nonempty records rather than physical lines.

## Track boundaries and synthetic validation

`TrackConfig.max_gap_seconds` must be supplied; candidate limits are sensitivity settings,
not validated defaults. Extrapolation and bridging invalid samples are forbidden. The
future processor must retain invalid/conflict barriers and original adjacency across
sorting and partitions. This release checks nonpositive/unordered time, identity/IMO
conflicts, gaps and invalid endpoints; it does not calculate displacement, reject land
crossings, resolve cross-feed identity or reconstruct tracks. No interval becomes accepted
through this checker alone.

Track/allocator/reader/ledger protocols support projected batches, private sparse
contributions, carried state and boundary halos. Halo rows support endpoints; only core-owned
contributions may be emitted. Sources, configuration, method, schema, state and core/halo
bounds affect reuse keys. Streaming and transactional publication are deferred implementations.
There is no dense vessel × cell × time grid.

The conservation validator checks supplied full-parent allocation coverage, gaps/overlaps,
identity and distance. Domain clipping must supply an explicitly clipped parent, with an
outside-domain remainder handled by the future allocator. A hand-calculable stationary
fixture splits 60 seconds into two 30-second pieces: total 1/60 vessel-hour and zero km;
unknown speed contributes no speed denominator. These tests validate contract arithmetic,
not cell geometry or a trajectory model. Direct R6/R7 polygon allocation, UTC crossings,
shorelines and independently validated track speed are future work.

## Metric semantics and shared application reference

`ais inspect-metrics` exposes the initial registry: observed identity union, estimated
present identity union, vessel-hours, vessel-km, time-weighted reported SOG, speed-band
hours and mean supported concurrency. Distinct counts use unions, SOG combines integrals
and denominators, and stationary intervals retain time with zero distance. None of these
aggregates is computed by this release. Unsupported values must be null with a reason;
empty reception is not evidence of empty water.

Reported SOG reduction uses summed knot-seconds divided by summed speed-supported
seconds, yielding knots. Unknown-speed time is excluded from that denominator and retained
as unknown support. An hours-based view converts both numerator and denominator by 3600:
knot-hours divided by supported hours. Zero supported seconds yields null. The synthetic
10-knot/20-knot/unknown example over three one-minute pieces has 1800 knot-seconds and
120 supported seconds, hence 15 knots; it does not divide by the full three-minute window.

The exact shared [v0.1 manifest schema](https://github.com/MarineCast/.github/blob/main/contracts/product-manifest.schema.json)
and [application delivery profile](https://github.com/MarineCast/.github/blob/main/contracts/application-delivery-profile.md)
were read as references and left unchanged. Native metric status uses the exact shared
vocabulary: `observed`, `unavailable`, `unknown`, `not_applicable`, `partial`. `observed`
encodes a valid result; **method** separately distinguishes observations and estimates.
Partial/null outputs retain metric-specific coverage meaning, available/expected support,
units and evidence. Accepted track time is not fleet completeness or receiver uptime.
Future wide Parquet exports require actual shared manifest/artifact conformance tests;
these native contracts do not claim that conformance.

Canonical application days are [UTC midnight, next midnight). Local companions retain
their clock and actual duration and require explicit civil-to-UTC reconciliation. Synthetic
Washington-zone fixtures cover 23/25-hour days. BC/Washington divergence and pinned tzdb
reproducibility must be tested when local clocks and supported tzdb are selected; a generic
Pacific offset is not assumed.

## Remaining decisions and gates

Domain polygon/version, R6 versus R7 (or separately allocated both), local reference versus
jurisdiction clock, daypart boundaries, vessel-function mapping, scientific gap/speed/land
rules and qualification criteria remain unresolved. `ProductConfig()` carries unresolved
values and cannot instantiate an export until spatial choices are explicit.

Deferred: source acquisition/decompression, Canadian access, identity registry and reuse,
track science and hidden-fix validation, exact H3/time allocation, Parquet exports, regional
pilot/benchmarks/backfills, data publication and Human/OrcaCast integration. Noise,
disturbance, biological response, whale-watch activity and people counts require independent
evidence and separate scientific work. Synthetic tests and a package build are not model
eligibility or a production qualification.
