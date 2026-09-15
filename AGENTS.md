# toolkit-ais agent guidance

## Scope and current state

Vessel tracks, vessel activity, traffic density, and maritime-use products.

This is an initial repository as inspected on 2026-09-15. No package, executable pipeline, or test
suite is established yet. Inspect the checkout before assuming this snapshot is still current.
Preserve unrelated changes and read deeper instructions before editing a subdirectory.

## Shared MarineCast context

Before changing repository boundaries, dependencies, shared schemas, provenance, or application
integration, read the MarineCast [infrastructure guide](https://github.com/MarineCast/.github/blob/HEAD/INFRASTRUCTURE.md).
In the multi-repository workspace, the local copy is `../../.github/INFRASTRUCTURE.md`.
Prefer that local copy when present; in an independent checkout, read the linked document. If it
cannot be retrieved, report that limitation and use the local contracts below; do not invent a
shared standard. These instructions explicitly request that reading; a sibling repository's
`AGENTS.md` is not automatically inherited.

The infrastructure guide owns cross-repository context. This repository owns its implementation
and scientific contracts. Surface conflicts before changing an interface; do not silently replace
an existing local contract with a proposed ecosystem convention.

## Domain contracts

Preserve vessel identity, source timestamps, position quality, and coverage gaps. Define speed,
distance, and aggregation units. Validate duplicate messages and implausible positions before
deriving tracks. AIS coverage is not all vessel activity; vessel activity is not observer effort
or a measured disturbance response.

## Implementation boundaries

- Keep this toolkit species-neutral and independently installable; do not require an OrcaCast
  checkout or import through sibling filesystem paths.
- Before adding a pipeline, define source rights, input/output grain, units, spatial and temporal
  support, missingness, provenance, and validation in the repository documentation.
- Add dependency declarations and runnable setup/validation commands with the implementation;
  do not copy viewshed's GDAL stack or commands unless this toolkit actually needs them.
- Prefer deterministic calculations and small synthetic/offline fixtures. Keep credentials,
  downloads, and large generated products out of tracked source.
- Keep unknown and unavailable values distinct from observed zero. Validate uniqueness and join
  cardinality rather than silently dropping conflicting records.

## Validation and completion

For documentation-only work, inspect `git status --short` and the diff, verify references, and run
`git diff --check` from this repository. There are currently no established package tests to run.
When adding executable behavior, add appropriate checks and document their exact commands here.
Report tests actually run, unverified source acquisition, and any unrun integration paths.
