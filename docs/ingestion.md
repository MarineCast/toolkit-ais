# Phase 2: offline ingestion and conservative source-identity handling

Start with the [native foundation contracts](foundation.md). Inputs are deliberately
provided local CSV or CSV.zst files with `SourcePartition` receipts: provider/era, unique
asset identity, exact SHA-256, acquisition time, half-open declared source window, schema
verification and rights/coverage evidence. These are caller-supplied assertions, not
independent source-rights verification or receiver/fleet completeness. Canada stays
unavailable. No account, order, agreement, download or live provider API is implemented.

Native record/state schema version is `ais-ingestion/0.1`, stored in Parquet metadata
and the completion receipt with dictionary/adapter versions, producer SHA and effective
configuration. It is separate from the shared application manifest contract.

`ais ingest-local job.json /chosen/private/output [--carried /chosen/previous/run]` requires
exactly `config` and `inputs` in the job JSON. Each input has `path` (relative to the job
file, or deliberate absolute path) and the complete receipt. Unknown config fields,
duplicate JSON keys and nonfinite JSON are rejected. All date/time bounds must be aware;
UTC is canonical. All `IngestionConfig` fields are required: partition ID, core/halo bounds,
`spatial` fields (domain reference, west/south/east/north, buffer in degrees), timestamp
policy, batch rows, total record cap, per-source decoded-byte cap, complete-record character
cap, SQLite cache/window caps and producer Git SHA. No real domain, R6/R7, dayparts or
scientific gap default is selected. The rectangular degree buffer is only a candidate
filter; antimeridian wrapping, water geometry and metric-distance buffering are unsupported.

Run the standalone synthetic example with deliberate local scratch paths:

```sh
python scripts/synthetic_ingestion.py /chosen/synthetic-workspace
ais ingest-local /chosen/synthetic-workspace/job.json /chosen/private-output
```

The example writes only fabricated data and a local job file; it does not run ingestion
or touch existing files. `ingest-local` exits 0 only for a verified completed directory
(including a verified retry). Configuration/ingestion failures exit 2; no success-shaped
empty product is emitted. A source with no records is unavailable. Row-level invalid
messages are rejected/audited; invalid headers, malformed CSV framing/width, decoder
errors or exceeded caps quarantine the whole run in staging.

## Streaming, projection and spill

Every CSV byte is framed/parsed to validate its full era header and physical record
boundaries. Only retained normalized position/identity/quality/provenance columns enter
Parquet and SQLite; no claim of a columnar CSV read or avoiding unused CSV bytes. Physical
start/end lines, source checksum and canonical raw-row checksum link every audit row to
source evidence. Private decoded CSV snapshots and SQLite spill remain inside the private
run directory, alongside four contracted Parquet artifacts. Snapshots/spill are working
evidence, not qualified product artifacts. No input/source file is modified or deleted.

CSV.zst currently requires one complete standard frame. Decoder window size, decoded
bytes and complete CSV record length are explicitly capped. Incremental decoding uses
64-byte compressed chunks to bound collected output per call; this conservative choice
has no national-throughput claim. EOF is checked and extra/concatenated frames rejected.
Many-frame delivery support requires an explicit follow-up. Checksums cover both the bytes
actually read and the final source bytes, detecting ordinary concurrent source mutation.

`zstd_window_kib` is an explicit KiB configuration value capped at 65,536 (64 MiB).
It is multiplied by 1,024 for the supported python-zstandard 0.23–0.25 implementations,
which pass `max_window_size` to `ZSTD_DCtx_setMaxWindowSize` in bytes. The upstream
docstring describes KiB, but both C/CFFI implementation paths pass the value unchanged
([0.25 C source](https://github.com/indygreg/python-zstandard/blob/0.25.0/c-ext/decompressor.c),
[CFFI source](https://github.com/indygreg/python-zstandard/blob/0.25.0/zstandard/backend_cffi.py)).
Synthetic streaming-frame tests exercise both a fitting 2 MiB window and its rejection
under a smaller configured limit. The bound is retained, not disabled.

SQLite orders/groups on spill disk with `temp_store=FILE` and an explicit page-cache cap.
Python fetches bounded row batches; Parquet row groups use the same cap. The receipt reports
maximum writer rows, Arrow table bytes and SQLite spill bytes. A 3,000-record synthetic test
measures Python allocations and verifies the batching bound. It excludes native caches,
OS cache, Parquet metadata growth and process RSS. Source/output disk caps, realistic RSS,
runtime and engine comparisons still need an approved pilot; this is no optimized-runtime
or constant-process-memory claim.

## Reconciliation, identity, halos and barriers

Every original row receives exactly one outcome: `accepted`, `duplicate`, `rejected` or
`filtered`. The ledger counts all rows, including halo evidence and spatial/time exclusions.
Source MMSI × exact UTC microsecond timestamp is a grouping key, not a resolved physical
vessel identity. Same-minute reports at different seconds remain separate. An exact
**normalized retained-attribute** duplicate agrees on coordinates, reported SOG, raw type,
equipment, IMO and quality flags. Unretained attributes can differ; raw-row fingerprints and
the decoded snapshot preserve that evidence. The lexicographically first asset/physical-row
key is a deterministic representative; duplicate audit aliases remain. Any conflicting
retained signature rejects all candidates in the simultaneous group. No first-row conflict
resolution, physical-vessel certification or cross-time IMO/MMSI registry is supplied.
Valid positions outside the candidate spatial filter still participate in simultaneous
conflict checks, so an outside fix cannot silently certify an incompatible inside fix.

`positions.parquet` emits representatives only in [core start, core end); `halo.parquet`
contains accepted evidence in [halo start, halo end) outside the core. `audit.parquet`
contains every supplied row and outcome/reasons. `state.parquet` stores one last accepted
point before core end per source MMSI plus a conservative `barrier_seen` bit. Invalid or
conflicting rows set this bit even when event time is unknown, so future track work cannot
silently bridge them. It is not automatically cleared by later fixes. Original rejection
references remain in the current audit and referenced prior receipt/audit chain.

Carried state requires a verified previous ingestion directory, identical filtering/time
policy and adjacent core windows. All four prior artifact checksums are verified; the
previous receipt checksum/run key enters the new identity. Carried points are endpoint
context only, never fabricated new source rows or core observations. Matching incoming
reports remain supplied observations, with duplicates reconciled among supplied rows.
A different retained signature at the carried MMSI/time rejects the incoming report,
records the prior endpoint record key in audit reasons and retires that endpoint from
state. The prior receipt/audit chain retains its evidence. The conflict sets a barrier
that later accepted core fixes cannot clear. Cross-file overlaps
are reconciled within the provided window; half-open core ownership prevents day-boundary
double emission. State can be stale and unresolved: future tracks must still validate gap,
identity, adjacency and barriers. Arbitrary checkpoint chains, mixed policy, missing files
and nonadjacent windows are rejected; no inferred continuity or receiver coverage follows.

## Completion, private data and limits

Sources, receipts, effective configuration, method/schema, producer SHA, core/halo bounds
and prior receipt affect deterministic run identity. On retry, all completed artifact hashes
must verify; corruption is an error, never an overwrite. The processing method is
`offline-ingestion/0.3`; older-method checkpoints must be regenerated from local inputs,
and are not silently reused. Verification rejects staging directories, failure markers
and directories whose name does not match the receipt run key. Input order does not affect output
row order, normalized outcomes, artifact bytes or receipt. Every writer closes before
`complete.json`; a same-filesystem directory rename exposes the completed run atomically.
Failed staging directories retain `failure.json` and have no completion marker. Concurrent
same-key publication can leave a failed loser staging directory; retry verifies the winner.
No automatic cleanup, whole-workspace transaction or power-loss durability is claimed.

New run directories are mode 0700 and files mode 0600; an existing output root's permissions
are not changed. Keep raw AIS, normalized vessel rows, spill and checkpoints private. Public
code and synthetic fixtures do not authorize their publication. Native Parquet is not a
shared final-product manifest or daily H3 conformance claim. Track geometry, estimated
activity allocation, scientific qualification, regional backfills and consumer integration
remain separate later phases.
