# AIS Toolkit

<img src="docs/assets/ais-toolkit-banner.png" alt="Ink panorama of a coastal archipelago with vessels, AIS signal paths, and a shore antenna" width="100%">

Independently installable, species-neutral normalization and contracts for future
source-to-daily-H3 processing. Phase 2 adds **offline local ingestion** to private native
Parquet using bounded batches and SQLite spill. Development/tests use synthetic fixtures
only. Phase 3 adds bounded local **estimated trajectories and direct R6/R7 contributions**
from verified ingestion evidence. Scientific defaults and interpolation accuracy remain
unqualified. Live acquisition, final daily products, regional qualification and consumer
integration remain deferred.

```sh
python -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
ais inspect-catalog
ais inspect-metrics
ais validate-csv tests/fixtures/current_synthetic.csv --era '2025+'
ais validate-csv tests/fixtures/legacy_synthetic.csv --era 2018-2024
pytest -q
ruff check .
ruff format --check .
python -m build --outdir dist-current
python scripts/wheel_smoke.py dist-current
```

When network access is unavailable, an environment with installed build/setuptools and
pytest tools can use `python -m build --no-isolation` and
`python scripts/wheel_smoke.py dist-current --offline-test-tools --wheelhouse /chosen/wheels`.
The wheelhouse must contain compatible declared Arrow/Zstandard/H3/Shapely (and transitive NumPy) dependency wheels.
Only pytest tool modules are copied into the temporary runner; the package and its runtime
dependencies install into a fresh environment from wheels with no index access.

`validate-csv` inspects a bounded, already-provided uncompressed CSV; its default cap is
10,000 rows. Exact field sets are required, independent of column order. Exit 0 means
nonempty contract inspection passed; 1 means rejected rows, conflicts or empty input;
2 means file/schema/cap failure with quarantine status. It does not certify source rights,
coverage or scientific suitability. The default rejects naive timestamps; explicitly
select `--naive-time-policy dictionary_utc` only when the pinned source dictionary applies.

See [offline ingestion](docs/ingestion.md) for deliberate input/output paths, receipts,
halos, carried barriers, atomic completion, limits and runnable synthetic example. Use
`ais ingest-local job.json /chosen/private/output --carried /chosen/previous/run` only with
explicitly provisioned local inputs; the carried directory is optional for the first run.

See [local estimated trajectories](docs/trajectories.md) for explicit gap/speed/coast/
projection settings, direct H3 clipping, UTC/core ownership, unresolved identity episodes
and numerical limits. `ais estimate-local job.json /chosen/private/output` emits private
contributions and accepted/rejected ledgers. To run a fabricated example in a NEW directory:

```sh
python scripts/synthetic_trajectories.py /chosen/new-synthetic-workspace
ais estimate-local /chosen/new-synthetic-workspace/trajectory-job.json /chosen/private-estimates
```

The script constructs fabricated Phase 2 evidence and a job; the CLI runs estimation.
Its synthetic thresholds, coordinates and fake producer SHA are not study defaults.

See [foundation contracts](docs/foundation.md) for schemas, metric semantics, source
verification, unresolved choices and deferred work. Real AIS, vessel-level intermediates
and model inputs stay private and outside tracked source. Code can be public; AIS presence
does not measure people aboard, whale presence, noise or disturbance.
