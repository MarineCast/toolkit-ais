# AIS Toolkit

First bounded milestone: independently installable, species-neutral normalization and
contracts for future source-to-daily-H3 processing. It uses **synthetic fixtures only**.
There is no acquisition, CSV.zst decoder, track reconstruction, H3 allocation, Parquet
writer, regional product, or consumer integration in this release.

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
python -m build
python scripts/wheel_smoke.py dist
```

When network access is unavailable, an environment with installed build/setuptools and
pytest tools can use `python -m build --no-isolation` and
`python scripts/wheel_smoke.py dist --offline-test-tools`. The latter copies only test
tool modules into the temporary runner; the package still installs into a fresh environment
from the wheel with no index or dependency access.

`validate-csv` inspects a bounded, already-provided uncompressed CSV; its default cap is
10,000 rows. Exact field sets are required, independent of column order. Exit 0 means
nonempty contract inspection passed; 1 means rejected rows, conflicts or empty input;
2 means file/schema/cap failure with quarantine status. It does not certify source rights,
coverage or scientific suitability. The default rejects naive timestamps; explicitly
select `--naive-time-policy dictionary_utc` only when the pinned source dictionary applies.

See [foundation contracts](docs/foundation.md) for schemas, metric semantics, source
verification, unresolved choices and deferred work. Real AIS, vessel-level intermediates
and model inputs stay private and outside tracked source. Code can be public; AIS presence
does not measure people aboard, whale presence, noise or disturbance.
