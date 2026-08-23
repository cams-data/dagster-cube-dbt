# dagster-cube-dbt

A Dagster [Component](https://docs.dagster.io/guides/build/components) that generates a
[Cube](https://cube.dev) semantic-layer schema from a dbt project's manifest, and exposes
each generated cube as a **virtual** Dagster asset.

## Why

Cube's data model (`cubes:`, with `dimensions`, `measures`, `joins`) largely mirrors a dbt
model plus its columns. Rather than hand-writing and hand-maintaining Cube YAML files
alongside dbt schema files, `dagster-cube-dbt` derives them from the dbt manifest at
component-refresh time, and lets you layer user-authored **merge patch** YAML files on top
for anything the generator can't infer (extra measures, removed dimensions, hand-written
joins, etc).

Each generated cube is represented as its own Dagster asset, declared with
`AssetSpec(is_virtual=True)` — a real Dagster feature (currently in preview), not just a
description. Materializing one never moves or stores data. Dagster's own staleness engine
treats a virtual asset as transparent: a downstream Cube pre-aggregation asset depending on
it has its freshness resolved by looking straight *through* the virtual layer to the dbt
model it's derived from, without the cube asset needing to be materialized itself for that
to work.

## How it works

`CubeDbtProjectComponent` extends `dagster_dbt`'s `DbtProjectComponent`, so a single
component definition both builds your dbt project *and* generates/exposes the Cube layer
on top of it — one dbt translator, one set of dbt CLI settings, for both concerns.

1. **State refresh** (`dg utils refresh-defs-state`, or automatically in `dagster dev`):
   the dbt project is prepared (as it already is for `DbtProjectComponent`), producing a
   `manifest.json`. This component then reads that manifest via `cube_dbt`, derives a base
   cube per dbt model (dimensions from columns, typed from `data_type` and required to be
   declared), and folds in every merge-patch YAML file found underneath the component's
   `defs.yaml` directory using a strategic, name-keyed merge (`$mergeStrategy: remove |
   replace | merge`). No measures or joins are generated — those, along with any wholly
   hand-written cube or Cube `views:`, are entirely up to the merge patches. The merged
   result is cached as component state (JSON) — nothing is written to disk outside that
   cache yet.
2. **Definitions load**: one virtual `AssetSpec` is built per cube from the cached state,
   each depending on the dbt asset for its underlying model.
3. **Materialization**: this is when the generated files actually get delivered somewhere —
   staged in a temp directory and handed to a `CubeFilePromoter` resource (bound under the
   component's `promoter_resource_key`, e.g. an S3 upload or a git-sync push; a
   `LocalFileCubeFilePromoter` ships for local dev). A cube asset still materializes
   automatically, but only once when its own generated content actually changes (a
   `code_version` hash of its YAML), not on every dbt model data update the way
   `AutomationCondition.eager()` would. A downstream pre-aggregation asset's own freshness
   doesn't depend on that ever happening either way — Dagster resolves it by looking through
   the virtual cube straight to the dbt model.

See [PLAN.md](PLAN.md) for the development roadmap and [DECISIONS.md](DECISIONS.md) for a
running log of implementation decisions, issues found, and assumptions made along the way.

## Packages

- [python_modules/dagster-cube-dbt](python_modules/dagster-cube-dbt) — the library.
- [python_modules/dagster-cube-dbt-tests](python_modules/dagster-cube-dbt-tests) — a small
  end-to-end dbt + Dagster + Cube project used to develop and test the component against.
