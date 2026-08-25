# Development plan

**Status**: Stages 0–20 done and verified end to end. Stage 21 (CI/CD/publishing) is live and
has already cut a real `0.1.1` release. Stage 22 (docs site) is code-complete and locally
strict-build-verified, pending the one-time Read the Docs import. Two real design bugs found
via an actual production deployment after Stage 20 shipped, both fixed: `build_defs_from_state`
silently required the live dbt project directory at deploy time (DECISIONS.md Phase 36), and
`get_cube_asset_spec`'s `extends`-dependency resolution didn't respect a subclass overriding it
to rename cube keys (Phase 37 -- surfaced by fixing Phase 36, itself a real regression, caught
before merge). A third bug, found via real usage rather than deployment: `GENERATED_ASSET_
AUTOMATION_CONDITION`'s `code_version_changed()` branch had no deps-readiness gate, so editing
a cube's own definition before its backing dbt model had ever run fired a request against a
table that didn't exist yet (Phase 38). A new opt-in feature, `landing_check` (Phase 39): after
promotion, optionally poll Cube's own REST API until the promoted content is actually visible
there before considering a cube/view materialized, closing the gap where "materialized" only
meant "handed to the promoter." Scoped ahead of the planned Superset dataset sync
(`SUPERSET_SYNC_PLAN.md`), which depends on this to avoid its own propagation-lag risk. A fourth
bug, found on `landing_check`'s first real production deployment: its code_version lookups were
keyed off a subclass-renamed `AssetSpec.key`'s last path segment, on the (false) assumption a
renaming override would only ever prepend to the default key -- a real override computing a
wholly new key broke it with a bare `KeyError` (Phase 40). `CubeRestApiClient` also gained a
`verify_tls` toggle (default `True`) for deployments behind a self-signed/internal-CA cert
(Phase 41). The planned Superset dataset sync (`SUPERSET_SYNC_PLAN.md`) is now implemented too
-- `CubeSupersetSyncComponent` + `SupersetResource`, a separate component chained onto
`CubeDbtProjectComponent` via `context.load_component` rather than a subclass (Phase 42). 130
tests passing throughout: `python_modules/dagster-cube-dbt/tests/`,
run against both dbt-core and dbt Fusion — see Stage 5/8/9/10/11/12/13/14/15/16/17/18/19/20/21/22 and DECISIONS.md.
Stage 1's fixture ended up in two forms — a library-internal one (`tests/fixtures/dbt_project`,
used directly by the test suite) and a separate `dg`-runnable example project
(`python_modules/dagster-cube-dbt-tests/`) — both verified end to end, including a real
`dg utils refresh-defs-state` / `dg list defs` run producing the full expected asset graph.
Stage 6's docs/verification work is done; its one optional item (a custom `dg scaffold defs`
scaffolder mirroring `DbtProjectComponentScaffolder`) was deliberately skipped — the default
scaffolder already produces a working skeleton, confirmed while building the example project.
See [DECISIONS.md](DECISIONS.md) for what was found/decided along the way, including a real
Windows/`uv`-Python environment issue that was found, root-caused, and fixed, and two library
bugs the real end-to-end run caught that the unit test suite's synthetic fixtures had been
masking.

Design decisions this plan assumes (see README.md for the rationale):

- `CubeDbtProjectComponent` **extends** `dagster_dbt.DbtProjectComponent` (one component, one
  translator, does both dbt build + cube generation).
- Cube generation/merge happen only during **state refresh** (`write_state_to_path`, i.e.
  `dg utils refresh-defs-state`), not on every defs load — but the result is cached as JSON
  state, not written anywhere on disk. `build_defs_from_state` reads that cached state to
  build `AssetSpec`s. There's no default delivery mechanism on the component: no on-disk path
  is reachable by both a Dagster run and an independently running Cube instance in a real
  deployment, so delivering the generated files is delegated to a `CubeFilePromoter`
  **resource** (a `dg.ConfigurableResource`, not a component subclass — promotion needs
  credentials/destination config, which is what resources are for), bound under the
  component's `promoter_resource_key` (`cube_file_promoter` by default) and exercised at
  **materialization** time (see Stage 9). `LocalFileCubeFilePromoter`, a local-dev-only
  resource, ships with the library for the "write straight to disk" case.
- Cube assets are regular materializable assets, one per cube, depending only on the dbt
  asset for their own underlying model (not on joined cubes) — and only if the cube's `name`
  actually matches a dbt model; a purely patch-defined cube with no matching model gets
  `deps=[]`. Materializing does real I/O: it stages every generated cube/view file in a temp
  directory, calls the bound promoter's `promote()`, then emits a `MaterializeResult` per
  selected cube, yielded in `specs` order (cubes before views) so multi-asset topological
  ordering is never violated regardless of which subset is selected.
- Merge patches aren't limited to patching dbt-derived cubes: a file can define an entirely
  new cube (no backing dbt model) or a `views:` list (Cube's composition-over-cubes concept)
  and both are surfaced as their own assets, since the merge is purely name-keyed and simply
  appends anything that doesn't match an existing entry. Every view gets an `AssetSpec` whose
  `deps` are the cube assets it's composed of (its own `cubes:` list) — a real dependency,
  unlike a cube's query-time `joins`.
- No measures or joins are generated at all — both are left entirely to merge patches.
  Cube/dimension `description` is passed through from dbt where present.
- Every `cube_select`-matched model must have dbt contract enforcement turned on
  (`config: {contract: {enforced: true}}`) -- a **hard error** (`UnenforcedContractError`) at
  generation time otherwise, checked before anything else about the model. This is what
  guarantees a model's `columns:` declarations are complete (dbt itself won't parse a
  contracted model with an undeclared column data_type) and can't silently drift over time
  (dbt's own contract validation fails the build if real output stops matching what's
  declared) -- see Stage 10.
- A column with no explicit `data_type` declared in dbt is a **hard error** at generation
  time, not a silent `string` fallback -- in practice only reachable for an uncontracted
  model, since a contract-enforced one can't parse without one. All violations across a run
  are collected and reported together.
- A column whose `data_type` can't be mapped to a Cube type (warehouse-specific types, e.g.
  ClickHouse's `Date32`/`UInt32`/`Nullable(...)`) is also a **hard error**
  (`UnrecognizedColumnTypeError`), not a silent `string` fallback -- fixable per-column via a
  `meta.cube.type` override, which bypasses type inference entirely. This is the only
  general fix, not just a workaround: the normalization step strips parenthesized content
  before inspecting a type name, so `Nullable(String)`/`Nullable(Int32)` are
  indistinguishable -- no fixed mapping table could resolve that correctly either way. See
  Stage 11.
- A column can opt out of dimension generation entirely via `meta.cube.dimension: false`.
  Excluded columns are exempt from the `data_type` check.
- Cube/dimension dicts are built directly from plain manifest dicts via `manifest.py`
  (vendored -- see Stage 12 -- rather than depending on the `cube_dbt` PyPI package), not
  through any Jinja-oriented string-dumping helpers.
- `cube_select` (`{paths, tags, names}`) is a component attribute controlling which dbt
  models generate a cube at all, independent of the inherited
  `select`/`exclude`/`selector` attributes that control what dbt actually builds.
- Merge-patch algorithm: vendored (on top of `deepmerge` from PyPI), not a dependency on the
  unpublished `pyyaml-merger` GitHub repo.
- Output layout (`LocalFileCubeFilePromoter` only): one YAML file per cube in `output_dir`,
  one YAML file per view in `views_output_dir` (defaults to `output_dir`), written at
  materialization time via the bound promoter, not at state-refresh time.

## Stage 0 — Dependencies & environment

- Update `python_modules/dagster-cube-dbt/pyproject.toml`: add `pyyaml` and `deepmerge`;
  keep `dagster~=1.13.0`, `dagster-dbt`, `cube_dbt`.
- `uv sync` and confirm the environment builds cleanly.

## Stage 1 — Test fixture project

Build `python_modules/dagster-cube-dbt-tests/`: a small, runnable dbt + Dagster project used
to develop and test the component against (not shipped, but real enough to `dg dev` against
manually).

- A minimal dbt project (DuckDB profile so it runs with no external warehouse) with models
  mirroring the transport example from this conversation: `journey_samples`,
  `destination_locations`, `origin_locations`, `dates` — laid out under `models/marts/` —
  plus at least one intermediate model under `models/intermediate/` that should *not* get a
  cube, to exercise `cube_select`.
- `schema.yml` declarations: `unique`/`not_null` tests (or constraints) for primary keys, an
  explicit `data_type` on every surfaced column of every mart model (required now that a
  missing `data_type` is a hard error), and a `description` on at least one model/column to
  prove passthrough. The intermediate model can deliberately omit `data_type` on a column,
  since it's excluded from cube generation by `cube_select` and should never trigger the
  error. One mart model column should set `meta.cube.dimension: false` and also omit
  `data_type`, to prove exclusion happens before — and therefore exempts it from — the type
  check.
- A Dagster `defs.yaml` wiring `CubeDbtProjectComponent` at the fixture's dbt project, with
  `cube_select: {paths: ["marts"]}` (manifest paths are relative to dbt's `model-paths`
  root, so no `models/` prefix — see DECISIONS.md).
- One or more merge-patch YAML files under that `defs.yaml`'s directory: the `journey_type`
  dimension removal example from this conversation (exercising `remove`), plus patches that
  add the `count` measure and the `journey_samples` joins to the other three models (since
  generation no longer produces either). Also include one patch defining a wholly new,
  hand-written cube with no backing dbt model, and one defining a `views:` entry composed of
  two of the generated cubes — to exercise both the `deps=[]` and view-composition-deps rules.

This fixture backs the tests built in Stage 5.

## Stage 2 — Cube generation core (pure, framework-agnostic)

New module(s) with no Dagster/Component dependency:

- Model selection: apply `cube_select` (`paths`/`tags`/`names`) via `cube_dbt.Dbt.filter(...)`
  before generating anything, so unselected models never reach column filtering, the type
  check, or the generator at all.
- Column selection: for each selected model's `columns` (`cube_dbt.Model.columns`, public),
  drop any column whose raw dict has `meta.cube.dimension == False` before anything else runs
  against it. Excluded columns produce no dimension and are invisible to the type check.
- Type-presence check runs *before* calling `cube_dbt`'s `Column.type` (which can't
  distinguish "declared as string" from "no `data_type` given" after the fact): for each
  remaining (non-excluded) column, inspect its underlying dict for a present, non-null
  `data_type`. Collect every `model.column` missing one across the whole run and raise a
  single error listing all of them, rather than stopping at the first. This is the one place
  generation reaches past `cube_dbt`'s public API — isolate it in a small helper and add a
  regression test pinning that a `cube_dbt` upgrade doesn't change the underlying dict shape.
- For models/columns that pass both filters, build the cube/dimension dicts directly from
  `cube_dbt`'s public `Model`/`Column` properties: cube-level `name`, `description` (if
  present), `sql_table`; dimension-level `name`, `description` (if present), `sql`, `type`,
  `primary_key: true` (if detected). No `measures` or `joins` keys are produced — both are
  left for merge patches to add.
- Deterministic ordering (stable across runs, driven by manifest node order) so generated
  output — and therefore diffs — don't churn without a real change.

## Stage 3 — Merge patch engine

- Vendor the strategic-merge-by-name algorithm (credit `john-wd/pyyaml-merger` as the
  origin) on top of the `deepmerge` PyPI package: list items matched by `name`, with
  `$mergeStrategy: remove | replace | merge` (default `merge`) support at any nesting level.
- Directory scanner: find every `*.yml`/`*.yaml` file under the component's `defs.yaml`
  directory, excluding `defs.yaml` itself and anything under `output_dir`, sorted by path for
  deterministic merge order.
- Pipeline: base document `{cubes: [...]}` from Stage 2, fold in each patch file in order. No
  special-casing needed for patch-only cubes or a patch-introduced `views:` key — the
  strategic array merge already appends unmatched-by-name list items, and the default dict
  merge strategy already unions top-level keys the base doesn't have.
- Unit tests reproducing the `journey_type` removal example verbatim, plus `replace` and
  default-`merge` cases, a case with no patches (base passes through unchanged), a patch that
  adds a wholly new cube by name, and a patch that introduces a `views:` list from nothing.

## Stage 4 — Component & state lifecycle

- `CubeDbtProjectComponent(DbtProjectComponent)`: add `output_dir` (required-ish path
  attribute, resolved relative to the component like `project` is), `views_output_dir`
  (optional, defaults to `output_dir`), `cube_select` (optional `{paths, tags, names}`,
  defaults to no filter/all models), and `cube_translation` (optional `AssetSpec`
  customization hook, mirroring `translation`, applied to both cube and view specs).
- `write_state_to_path`: call `super().write_state_to_path(state_path)`, then run the Stage
  2 + 3 pipeline against the resulting manifest and write one merged YAML file per cube into
  `output_dir` and one per view into `views_output_dir`.
- `build_defs_from_state`: call `super().build_defs_from_state(...)` to get the dbt
  `Definitions`; separately read the YAML files already in `output_dir`/`views_output_dir` to
  build:
  - one `AssetSpec` per cube (key derived from cube name; `deps=[asset_key_for_model(name)]`
    if the cube's name matches a dbt model anywhere in the manifest — checked by name only,
    independent of `cube_select` — else `deps=[]`; metadata holding the generated YAML;
    `automation_condition=AutomationCondition.eager()` by default).
  - one `AssetSpec` per view (key derived from view name; `deps` = the cube asset key for
    each entry in the view's own `cubes:` composition list; same metadata/automation-condition
    defaults).
  - a `@multi_asset` covering both spec sets whose op yields `dg.MaterializeResult(asset_key=...)`
    for each requested key with no actual I/O.
  Merge both `Definitions` and return them.
- Raise a clear, actionable error if `output_dir`/`views_output_dir` is missing/empty (or
  doesn't match the current manifest) telling the user to run `dg utils refresh-defs-state`,
  rather than silently generating on the fly.
- `asset_key_for_cube(name)` / `asset_key_for_view(name)` helpers, mirroring the parent's
  `asset_key_for_model`.

## Stage 5 — Tests

- Stage 2 golden-file generation tests against the Stage 1 fixture's manifest.
- Stage 3 merge-patch tests (unit-level, no manifest involved).
- Component-level tests against the Stage 1 fixture:
  - run state refresh (directly via `write_state_to_path`, not just the CLI) and assert the
    written `output_dir`/`views_output_dir` files match expected merged YAML, including the
    patch-only cube and the view.
  - build `Definitions` and assert asset keys, deps (each dbt-backed cube depends only on its
    own dbt model; the patch-only cube has `deps=[]`; the view depends on its composed cube
    assets), and metadata.
  - materialize the cube+view multi-asset and assert `MaterializeResult`s are produced for
    every spec, including the patch-only cube and the view, with no filesystem/database side
    effects.
- Regression test pinning the raw column-dict shape (`data_type` key) used for the type
  presence check in Stage 2.
- A test that a column missing `data_type` on a `cube_select`-ed model raises, with all
  offending `model.column` names present in the error message in one pass.
- A test that `cube_select` correctly excludes the fixture's intermediate model (no cube
  generated for it, and its undeclared-`data_type` column never triggers an error).
- A test that `meta.cube.dimension: false` excludes a column from the generated dimensions
  list, and that such a column being undeclared for `data_type` does not raise.
- A test that model and column `description` are passed through into the generated cube and
  dimension dicts when present, and omitted when absent.
- **`tests/test_dbt_layer_parity.py`** (added after the initial plan, user-prompted): the
  dbt-asset layer `CubeDbtProjectComponent` inherits from `DbtProjectComponent` must be
  identical to what plain `DbtProjectComponent` produces for the same project -- a user
  swapping our component in for the vanilla one should never see the dbt portion change.
  Compares `AssetNode` fields (description, group_name, tags, kinds, code_version, owners,
  deps, checks, partitioning) and metadata keys for every dbt model between the two
  components built from the same fixture project, plus an execution-level test that actually
  runs `dbt build` through both and confirms identical materializations/passing checks. See
  DECISIONS.md for why this was missing and what closing the gap actually required (fixture
  scoping, not a real component bug).
- **`$mergeStrategy: patch`** (added after the initial plan, user-prompted): a fourth
  `$mergeStrategy` value, per-item (not per-file, so one file can patch an existing cube and
  introduce new ones below it in the same document). Behaves like the default `merge`
  strategy when matched; raises `UnmatchedPatchTargetError` (collected across every patch
  file folded into one `merge_documents()` call, reported together) when it isn't matched at
  all -- guarding against a real bug where a patch whose target had since been renamed or
  removed would silently become a new, broken cube missing `sql_table` and still carrying
  its own nested `$mergeStrategy` keys unprocessed. `$mergeStrategy: remove` targeting
  something that no longer exists stays a no-op (the outcome converges either way) but now
  emits a `warnings.warn` since it's usually a sign of a stale patch. See DECISIONS.md.
- **dbt Fusion test matrix** (added after the initial plan, user-prompted): the same 45-test
  suite now also runs against the [dbt Fusion engine](https://docs.getdbt.com/guides/fusion),
  not just dbt-core, via Hatch matrix environments (`hatch run test.core:run` /
  `hatch run test.fusion:run` -- see `[tool.hatch.envs.test]` in `pyproject.toml`). Fusion
  can't share a venv with dbt-core (colliding console script names), so this needed a
  genuinely separate environment, not just an added dependency. Building this caught three
  real bugs before either engine's test run went green: two dbt-schema/DuckDB compatibility
  gaps between the engines (fixed in the fixture, no code changes) and a dependency-spec trap
  where the naive fusion package spec silently resolved to an unrelated, also-`dbt`-named
  product instead of Fusion. See DECISIONS.md for the full, empirically-verified account.

## Stage 6 — Docs & scaffolding polish

- Root and package READMEs (done).
- ~~Optional: a `ComponentScaffolder` for `dg scaffold defs dagster_cube_dbt.CubeDbtProjectComponent`,
  mirroring `DbtProjectComponentScaffolder`, for parity with the parent component's DX.~~
  Deliberately skipped: the default scaffolder already produces a working `defs.yaml`
  skeleton (used as-is to build `dagster-cube-dbt-tests`), so a custom one wasn't worth the
  added maintenance surface.
- Walk the root README's example against the actual Stage 1 fixture output once built, so
  the docs never drift from a real, tested example. Done — the real `dg utils
  refresh-defs-state` run against `dagster-cube-dbt-tests` is what caught the
  `cube_select.paths` documentation bug and the `merge.py` stray-key bug (see DECISIONS.md).

## Stage 7 — Column meta passthrough and file promotion

Added after the initial plan, based on real usage needs.

- `meta.cube` promoted-key passthrough: `order`, `mask`, `public` (alongside the existing
  `dimension` control flag) are read from a dbt column's `meta.cube` and set as top-level
  attributes on the generated dimension, rather than staying nested under the dimension's
  own `meta:`. Anything else under `meta.cube`, or outside the `cube` namespace entirely,
  still passes through into the dimension's `meta:` unchanged — only the recognized
  control/promoted keys are consumed.
- `promote_cube_files(context)`: a `@public`, overridable, no-op-by-default method called
  once per materialization of the cube/view multi-asset (before any `MaterializeResult` is
  yielded, so a failure fails the run rather than reporting a false materialization). Lets a
  subclass push the already-generated `output_dir`/`views_output_dir` files to wherever a
  running Cube server actually reads from, when that isn't already the same location — S3,
  a git repo behind a Cube Core git-sync sidecar, etc. Deliberately just the hook, not a
  packaged S3/git implementation — see DECISIONS.md for the reasoning and what a follow-up
  `dagster-cube-dbt[s3]`/`[git]` extra would need.

## Stage 8 — Delivery is a materialization-time concern, not a state-refresh one

Added after the initial plan, user-prompted (a correct architectural pushback on Stage 4/7's
original design — see DECISIONS.md Phase 12 for the full account).

- `write_state_to_path` no longer writes real cube/view YAML files anywhere — it caches the
  merged cube/view data as JSON in the component's state directory. `build_defs_from_state`
  reads that cache to build `AssetSpec`s, with no filesystem I/O beyond the state cache.
- Real file writing (`write_entities` into a per-materialization `tempfile.TemporaryDirectory()`,
  then `promote_cube_files(context, cubes_dir, views_dir)`) moved into the `_cube_assets`
  multi-asset's execution body — materializing a cube/view asset now does real I/O.
- `promote_cube_files`'s default implementation now raises `NotImplementedError` instead of
  being a silent no-op: there's no deployment topology where a fixed on-disk path is reachable
  by both an ephemeral Dagster run and an independently-running Cube instance, so a component
  author who hasn't implemented delivery should see a clear failure, not silent nothing.
- `output_dir`/`views_output_dir` moved off the base component entirely, onto a new
  `LocalFileCubeDbtProjectComponent(CubeDbtProjectComponent)` for local dev/testing only,
  which implements `promote_cube_files` by writing the staged files straight to those
  directories. `dagster-cube-dbt-tests`' `defs.yaml` now uses this subclass.

## Stage 9 — Promotion is a resource, not a component override

Added after the initial plan, user-prompted (a correct architectural pushback on Stage 8's
component-subclass-based promotion -- see DECISIONS.md Phase 13 for the full account).

- `promote_cube_files` and `LocalFileCubeDbtProjectComponent` removed. New
  `CubeFilePromoter(dg.ConfigurableResource, ABC)` (`resources.py`) with an abstract
  `promote(context, cubes_dir, views_dir)`, and `LocalFileCubeFilePromoter(CubeFilePromoter)`
  replacing the old local-dev subclass with a resource.
- `CubeDbtProjectComponent` gained `promoter_resource_key: str = "cube_file_promoter"`; the
  `_cube_assets` multi-asset declares `required_resource_keys={self.promoter_resource_key}`
  and fetches the resource dynamically via `getattr(context.resources, ...)`. The resource
  itself is bound anywhere in the project (e.g. a plain `defs/resources.py` module) -- it
  doesn't need to be declared alongside the component.
- Fixed an unrelated real bug caught while verifying this end-to-end against the actual
  example project: `_cube_assets` yielded `MaterializeResult`s by iterating
  `context.selected_asset_keys` (an unordered set), which could yield a view before one of its
  dependency cubes and hit `DagsterInvariantViolationError`. Fixed by iterating `specs` order
  instead (cubes always precede views).

## Stage 10 — Contract enforcement required for cube-generating models

Added after the initial plan, user-prompted (a real gap found in production use -- see
DECISIONS.md Phase 14 for the full account).

- A model matched by `cube_select` with zero columns declared in `schema.yml` previously
  produced a silently empty cube (`dimensions: []`) instead of an error, since
  `MissingDimensionTypeError`'s per-column check has nothing to iterate over when no columns
  are declared at all.
- Fixed by requiring dbt contract enforcement (`config: {contract: {enforced: true}}`) on
  every `cube_select`-matched model -- stricter and better-motivated than just checking for
  at least one declared column, since a contract also guards against a column being added to
  the model's SQL later without schema.yml being updated (dbt's own contract validation fails
  the *build* if real output doesn't match what's declared; a manifest-only check can never
  catch that).
- New `UnenforcedContractError` (`generation.py`), checked per model before the existing
  `MissingDimensionTypeError` per-column check (the latter is now only reachable for an
  uncontracted model, since dbt won't parse a contracted one with an undeclared column type).
  Confirmed the `contract.enforced` manifest field is shaped identically on dbt-core and dbt
  Fusion before relying on it.
- Both fixture dbt projects (library-internal and the `dagster-cube-dbt-tests` example)
  updated to enable contracts on all `marts` models.

## Stage 11 — `meta.cube.type` override for types `cube_dbt` doesn't recognize

Added after the initial plan, user-prompted (a real bug report against a ClickHouse dbt
project -- see DECISIONS.md Phase 15 for the full account, including why extending
`cube_dbt`'s own type-mapping table wasn't a viable alternative).

- New `meta.cube.type` reserved key, consumed by `_resolve_dimension_type(column)`
  (`generation.py`): returns the override if set, otherwise calls `cube_dbt`'s `column.type`,
  catching the plain `RuntimeError` it raises for an unrecognized `data_type` and returning
  `None` instead of propagating it.
- New `UnrecognizedColumnTypeError`, collecting every column across a run that `cube_dbt`
  couldn't type and had no override for -- checked last, after the contract and
  missing-`data_type` checks (both logical prerequisites).
- Handled as an explicit step in `generate_cubes`'s loop rather than folded into the generic
  `order`/`mask`/`public` promoted-meta-keys loop, since it must be consulted *before*
  `cube_dbt`'s own `column.type` is called (calling it is what raises), not applied as a
  post-hoc overwrite.

## Stage 12 — Vendored `cube_dbt` instead of depending on it

Added after the initial plan, user-prompted (see DECISIONS.md Phase 16 for the full account,
including the release-cadence and "Cube's own newer dbt integration is a separate Cube Cloud
product feature" research behind the decision).

- New `manifest.py`: the specific subset of `cube_dbt`'s behavior this library actually used
  (model/column filtering, primary-key detection, dbt-type-to-Cube-type mapping,
  `sql_table`'s `relation_name` fallback), ported faithfully but operating on plain manifest
  dicts throughout -- no wrapper classes, since `cube_dbt`'s `Model`/`Column` added no
  behavior beyond property access this library was already unwrapping immediately.
- `generate_cubes` now takes the raw manifest dict directly, not a `Dbt` wrapper.
- The `cube_dbt` PyPI package is no longer a dependency anywhere (`pyproject.toml`'s main
  deps and both hatch matrix legs).
- The two regression tests pinning `cube_dbt`'s private attribute shapes were deleted, not
  adapted -- there's no more external private API being reached into for them to guard.

## Stage 13 — Native ClickHouse type support

Added after the initial plan, prompted by a real-world error report immediately after Stage
12 landed (see DECISIONS.md Phase 17 for the full account).

- `manifest.TYPE_MAPPINGS` now includes ClickHouse's explicitly-sized int/uint types,
  `Date32`, `DateTime64`, `UUID`, `FixedString`, `Enum8`/`Enum16`, `IPv4`/`IPv6` directly --
  a real user's ClickHouse project hit 28 unrecognized columns across two models on first
  use, all plain `UIntN`/`Date32`, confirming this was a missing dialect, not a case for
  per-column `meta.cube.type` overrides.
- `Nullable(T)`/`LowCardinality(T)` are now unwrapped recursively to whatever `T` is, instead
  of falling through the blind paren-stripping regex that collapses them to a meaningless
  bare `nullable`/`lowcardinality` -- the exact gap Stage 11 identified as unfixable via a
  bigger mapping table alone, now closed because owning `manifest.py` directly (Stage 12)
  made a real fix cheap instead of requiring a fork.
- **Bug fix, user-reported (Phase 25 in DECISIONS.md)**: `int16`/`int32` were missing from
  the ClickHouse addition above -- `int8`/`int64` happened to already resolve via accidental
  collisions with other vocabularies' same-spelled types, masking that `int16`/`int32` had no
  equivalent coverage. Added directly; audited `float32`/`float64` at the same time, already
  both present.
- **`geo` dimensions rejected outright (Phase 26 in DECISIONS.md)**: a ClickHouse `Point`
  column error's suggested fix listed `geo` as a valid `meta.cube.type` target, but Cube's
  `geo` dimension needs `latitude`/`longitude` SQL sub-expressions instead of the single `sql`
  field every dimension this library generates uses -- structurally impossible to build here,
  for any column. Investigating this surfaced a worse, pre-existing, untested bug: BigQuery's
  `GEOGRAPHY` was already auto-mapped to `"geo"` and silently produced a broken dimension with
  no error at all. `generate_cubes()` now rejects any column resolving to `geo` (inferred or
  via override) with a new, dedicated `UnsupportedGeoDimensionError` explaining why and
  pointing at `meta.cube.dimension: false` or a hand-authored merge-patch dimension instead.

## Stage 14 — `extends`-resolved fields available to `get_cube_asset_spec`

Added after the initial plan, user-prompted (see DECISIONS.md Phase 18 for the full account,
including verification against Cube's own documented `extends` semantics).

- New `merge.resolve_extends(entities)`: for every cube, follows its `extends` chain
  (recursively, multi-level) and returns its fully resolved fields -- parent's fields with
  its own folded on top -- reusing `StrategicMerger` directly, since Cube's own `extends`
  merge semantics (reuse all declared members, child overrides by name) are the same shape
  as this library's existing merge-patch application. Read-only: never mutates its input,
  and its result is never written to disk or handed to a promoter.
- `component.py`'s `build_defs_from_state` passes each cube's *resolved* dict into
  `get_cube_asset_spec` (the existing `cube` argument, no signature change) -- so a cube
  that only overrides a field or two via `extends` still gets its parent's
  `description`/`meta`/etc. reflected in its `AssetSpec`, while the actual generated/
  promoted YAML keeps `extends:` literal for Cube to resolve itself.
- New `CircularExtendsError` for a genuine `extends` cycle; an `extends` target not found
  among the component's own cubes (e.g. a hand-authored cube elsewhere in the Cube project)
  is left unresolved rather than an error, since Cube itself still resolves it at runtime.

## Stage 15 — Column schema metadata (not column lineage)

Added after the initial plan, user-prompted (see DECISIONS.md Phase 19 for the full account,
including why `dagster/column_lineage`/`TableColumnLineage` -- what was originally asked for
by name -- can't actually express "name, type, description" and `dagster/column_schema`/
`TableSchema` is the metadata that does).

- Every cube `AssetSpec` now carries static `dagster/column_schema` metadata (`TableSchema`),
  built from the cube's dimensions and measures: name, Cube's own type, description where
  present, tagged `dagster_cube_dbt/member_type: dimension|measure`.
- Built from the same extends-resolved `cube` dict `get_cube_asset_spec` already receives
  (Stage 14), so an extending cube's column schema reflects its parent's dimensions/measures
  too.
- Scoped to cubes only -- views don't declare their own dimensions/measures directly, so
  there's no column list to build without also resolving view composition.

## Stage 16 — Primary-key constraints on column schema metadata

Added after the initial plan, user-prompted (see DECISIONS.md Phase 20 for the full account).

- A single-column primary key dimension gets column-level `TableColumnConstraints(nullable=False,
  unique=True, other=["primary key"])`.
- A composite primary key's dimensions each get `nullable=False` but not `unique=True` (no
  single column in a composite key is unique alone) -- the composite relationship is instead
  stated as a table-level `TableConstraints(other=["primary key: (col_a, col_b)"])`, the only
  way this API can express a multi-column relationship at all.
- Every other column keeps the ordinary default constraints -- no other constraint is
  inferred from dbt.
- **Bug fix, user-reported (Phase 21 in DECISIONS.md)**: primary-key detection only read
  dbt's model-level `constraints:` declaration style; a column-level
  `columns: [{name: ..., constraints: [{type: primary_key}]}]` declaration (dbt keeps the two
  in separate manifest fields) was invisible. `model_primary_key_names()` now checks both.
- **Correction (Phase 22 in DECISIONS.md), then a real gap found and fixed (Phase 23)**: the
  Phase 21 fix unioned both constraint tiers unconditionally; Phase 22 switched to strict
  priority (model-level, then column-level only if model-level found nothing) based on the
  user's own testing. That testing turned out to be on ClickHouse, an unrepresentative
  adapter for this question (no SQL primary-key constraint concept at all) -- verified
  independently instead with a real `dbt parse` against dbt-core + DuckDB, confirming dbt's
  parser hard-errors if both tiers are declared on the same model, so a valid manifest can
  never populate both (the priority order is defensive-only, not exercised by real projects).
  The actual, useful finding: ClickHouse models declare a primary key via
  `config(primary_key=...)` instead, since they can't use `constraints:` at all -- confirmed
  against a real `dbt-clickhouse` `dbt parse` that this lands in a wholly separate manifest
  field (`config.primary_key`, string or list) that detection never read. Added as a fourth
  tier, consulted after both constraint tiers and before the tags/tests fallback.
- **`config.order_by` fallback (Phase 24 in DECISIONS.md)**: a ClickHouse MergeTree table's
  `ORDER BY` *is* its primary key whenever `PRIMARY KEY` isn't explicitly set (verified
  against ClickHouse's own docs, not dbt's -- this is warehouse behavior). Added as a fifth
  tier, consulted only when `config.primary_key` found nothing. `order_by` can hold an
  arbitrary SQL expression rather than plain column names, so both this tier and the
  `config.primary_key` tier now intersect their raw value against the model's real columns,
  silently dropping anything that isn't an actual column rather than falling through to the
  tags/tests heuristic being blocked by a bogus name.

## Stage 17 — `meta.cube` promotion on models (`public`, `title`)

Added after the initial plan, user-prompted (see DECISIONS.md Phase 27 for the full account).

- The same `order`/`mask`/`public` promotion dimensions already got from `meta.cube` on a dbt
  column now has a model-level equivalent: a model's `meta.cube.public`/`meta.cube.title`
  become real top-level attributes (`public`/`title`) on the generated cube, following the
  exact same consume-and-fall-through pattern (anything else under `meta.cube`, or `meta`
  outside the `cube` namespace, stays in the cube's own `meta:` rather than being dropped).
- Verified empirically (not assumed) that model `meta` lives at the same manifest location as
  column `meta` -- a real `dbt parse` (DuckDB) confirmed `config: {meta: {...}}}` on a model
  compiles to `node["meta"]` at the top level, same as columns.
- No `component.py` change needed -- the generated cube dict (including the new `public`/
  `title` keys) already flows straight into the asset's `dagster_cube_dbt/yaml` metadata.

## Stage 18 — Renaming a cube via `meta.cube.name` / `meta.cube.suffix`

Added after the initial plan, user-prompted (see DECISIONS.md Phase 28 for the full account).

- `meta.cube.name` (full override) or `meta.cube.suffix` (appended to the model's own name,
  mutually exclusive with `name`; raises `ConflictingCubeNameError` if both set) rename the
  generated cube. Enables a common Cube pattern: a suffixed, `public: false` base cube plus a
  hand-authored `extends:` cube (via a merge patch) exposing it publicly under a plain name.
  Renaming happens inside `generate_cubes`, before merge-patch application, so patches target
  the cube's *final* name -- the same name visible in the promoted YAML.
- The harder part, found by tracing (not assuming) how deep `cube["name"] == model["name"]`
  is relied on: `get_cube_asset_spec` computes the cube asset's dbt-model dependency via
  `asset_key_for_model(cube["name"])`, which matches purely against the dbt manifest, with no
  notion of Cube at all -- a naive rename would silently drop that dependency edge (not point
  at the wrong asset, just lose it), breaking the freshness/lineage propagation `is_virtual`
  assets rely on. Fixed via a new `cube_source_models: {cube_name: model_name}` sibling key
  returned by `generate_cubes()` (kept separate from each cube dict so it never leaks into
  the real promoted YAML), used by `build_defs_from_state` for asset-spec purposes -- no
  change to `get_cube_asset_spec`'s public single-argument signature.
- Verified end to end against `dagster-cube-dbt-tests`: a real `dg utils refresh-defs-state`
  with `dates` suffixed to `dates_base` showed the real Dagster asset graph correctly
  depending on `dates` (the real dbt model), not a nonexistent `dates_base` one.
- **Design correction, user-prompted (Phase 29 in DECISIONS.md)**: a more realistic version of
  the pattern -- one base cube extended by *multiple* hand-authored public cubes -- first
  exposed a real bug (only one of several extending cubes actually got a working dependency,
  by coincidence), which was initially fixed by propagating the dbt-model dependency through
  `extends` chains via the same inheritance mechanism `description`/`meta` already use. The
  user then asked, correctly, whether an extending cube should depend on the dbt model
  directly at all, versus on the cube it extends (with the dbt model still reachable
  transitively). The latter is both what they preferred and the more accurate design --
  cube/view assets are `is_virtual`, and Dagster's staleness engine already looks straight
  through *chains* of virtual assets to the nearest real ancestor, so a cube-to-cube edge per
  `extends` hop loses no freshness propagation while matching the real reuse relationship
  more closely than a flat fan-out onto one dbt model. Reworked accordingly; this also fixed
  a pre-existing gap in the original (Stage 14) `extends` feature -- a cube extending another
  previously got *no* dependency edge at all if its name didn't happen to match a dbt model.

## Stage 19 — A concurrency pool for the promotion op

Added after the initial plan, user-prompted (see DECISIONS.md Phase 30 for the full account) --
follow-up from building a real git-pushing `CubeFilePromoter` (in the user's own project) that
mutates a persistent local checkout, which is unsafe for two concurrent runs to touch at once.
The user pointed out this is true of nearly every real promoter, not just that one.

- `dg.multi_asset` already accepts a `pool: str | None` parameter -- Dagster's concurrency
  pools feature. New `promotion_pool` component field (default `None` -> auto-derived as
  `f"{dbt_project.name}_cube_promotion"`, scoped per project so unrelated components' pools
  don't serialize against each other unless explicitly shared). Assigning a pool changes
  nothing on its own -- confirmed against Dagster's own docs -- until a max concurrency is
  actually set for that pool name in the Dagster UI, making this a zero-cost default.
- Verified end to end against `dagster-cube-dbt-tests`: the real built multi-asset's op
  reports the expected auto-derived pool name.

## Stage 20 — `GitCubeFilePromoter` ships with the library

Added after the initial plan, user-prompted -- see DECISIONS.md Phase 31 for the full,
multi-turn account (built in the user's own project first, then moved here once it had
matured through several rounds of real feedback: a persistent-checkout-vs-temp-dir design
question, a real k8s deployment gap found in their Dockerfile, research into Cube Cloud's vs.
Cube Core's actual CI/CD conventions, and a request to support both).

- New `dagster_cube_dbt.git_promoter.GitCubeFilePromoter`, exported at the package top level.
  Shallow-clones (`--depth 1`) fresh into a throwaway temp directory on every `promote()` call
  rather than keeping a persistent local checkout -- no shared local state for concurrent runs
  to corrupt, and no meaningful cost given this resource never needs history, only ever adding
  one new commit on the remote's current tip.
- Two mutually exclusive auth modes (enforced via a pydantic validator): `ssh_private_key`
  (deploy-key auth, for an arbitrary repo something else -- e.g. a manually-configured
  `kubernetes/git-sync` sidecar -- polls, the self-hosted Cube Core convention) and
  `http_username`/`http_token` (HTTP Basic Auth via a per-invocation `-c http.extraHeader`,
  for pushing directly to Cube Cloud's own git remote, which authenticates that way rather
  than with SSH -- confirmed against Cube's own docs). The HTTP token (and its base64-encoded
  header form) is redacted from any error message this resource raises.
- Defaults `cubes_subdir`/`views_subdir` to `model/cubes`/`model/views`, Cube's own documented
  project layout (verified, not guessed).
- Requires the `git`/`ssh` binaries on `PATH` wherever it runs -- documented as a runtime
  prerequisite (the same category as `dbt`/`libpq`), not a pip dependency, since there's no
  PyPI package that installs a working `git` CLI. Raises a clear, specific error immediately
  if `git` isn't found, rather than a confusing failure deep inside a subprocess call.

## Stage 21 — Publishing: Conventional Commits + Trusted Publishing to PyPI

Added after the initial plan, user-prompted, once `GitCubeFilePromoter` had shipped and been
exercised against a real k8s-bound deployment (see DECISIONS.md Phase 32 for two real
production bugs that surfaced along the way, and Phase 33 for the CI/CD pipeline itself).

- `.github/workflows/ci.yml` (reusable): the existing Hatch test matrix (dbt-core + dbt
  Fusion), called from both `pr.yml` (every pull request) and `release.yml` (before anything
  is allowed to publish) — nothing reaches PyPI without passing the same tests a PR must pass.
- `.github/workflows/release.yml`: on push to `main`, [python-semantic-release](https://python-semantic-release.readthedocs.io/)
  computes the next version from Conventional Commits since the last release, updates
  `pyproject.toml`, generates `CHANGELOG.md`, commits, tags, and creates a GitHub Release —
  then a separate job builds with `uv build` and publishes via PyPI **Trusted Publishing**
  (OIDC through `pypa/gh-action-pypi-publish`; no PyPI API token stored as a secret anywhere).
  Push to `next` instead produces a **prerelease** (`0.2.0rc1`, ...) published to the same
  PyPI project — invisible to plain `pip install dagster-cube-dbt`, reachable via `--pre` or
  an exact pin, the "preview package" channel.
- `pyproject.toml`: real `description`, `license`/`license-files` (LICENSE copied into the
  package directory — the build backend's project root, not the repo root, so the root-level
  copy alone wouldn't have been packaged), `classifiers`, and `project.urls`; a
  `[tool.semantic_release]` config with `major_on_zero = false` (deliberate — see the comment
  in `pyproject.toml` for why) and the `main`/`next` branch-to-release-type mapping.
- Verified locally: `uv build` + `twine check dist/*` both pass, and the built wheel actually
  contains the license file (`*.dist-info/licenses/LICENSE`) — confirms the metadata is
  genuinely PyPI-uploadable, not just syntactically present in `pyproject.toml`.
- One-time setup that only a maintainer with PyPI access can do (documented in the README's
  new "Releasing" section, not something CI or an agent can complete unattended): create a
  GitHub Environment named `pypi`, and register a PyPI "pending publisher" pointing at this
  repo/`release.yml`/that environment name.

**Status: pushed, first real run found (and fixed) two bugs, not yet confirmed to have
actually published.** Committed and pushed to `origin/main` (see DECISIONS.md Phase 32 for the
two Python-side bugs the resulting real k8s/git usage caught, and Phase 34 for two more the
pipeline's own first live run caught: a nonexistent `astral-sh/setup-uv@v9` tag, and
`allow_zero_version` defaulting to `false` and computing `1.0.0` instead of `0.1.0` for the
first release — both fixed and pushed). Also since expanded to test both Python 3.12 and 3.13
(Stage 21 was 3.12-only despite `classifiers` already claiming 3.13 support). Remaining:

1. **Confirm the current push actually goes green end to end**, including the `publish` job —
   nothing has confirmed a real PyPI upload has happened yet. Watch the Actions tab; confirm
   `pip install dagster-cube-dbt` resolves afterward.
2. **Decide when `major_on_zero` flips.** Currently `false` (see the comment in
   `pyproject.toml`) so a breaking-change commit bumps `0.x`'s minor version, not straight to
   `1.0.0`. Revisit once the library is actually meant to declare a stable `1.0`.

## Stage 22 — a docs site: MkDocs + Material + mkdocstrings on Read the Docs

Added after the initial plan, user-prompted -- see DECISIONS.md Phase 35 for the full account
(tool/host choice reasoning, and the real bugs it caught along the way).

- `mkdocs.yml` + `docs/` at the repo root; `docs/index.md`/`docs/changelog.md` pull in
  `README.md`/`CHANGELOG.md` verbatim via `pymdownx.snippets` rather than duplicating them;
  `docs/reference.md` is a real, new page -- an mkdocstrings-generated API reference for the
  five public symbols, something the README doesn't otherwise provide.
- `.readthedocs.yaml`: installs the package plus a new `[project.optional-dependencies] docs`
  extra (`mkdocs`, `mkdocs-material`, `mkdocstrings[python]`).
- New `docs` job in `ci.yml` (`mkdocs build --strict`) -- broken docs now fail PRs the same way
  a failing test would.
- Fixed along the way, not incidental to it: three docstrings still using Sphinx/RST
  `.. code-block::` directives that `mkdocstrings` can't render; four genuinely broken relative
  links and one wrong heading-anchor slug in the README, invisible on GitHub's own rendering
  but caught immediately by `--strict`; and a real CI bug where `pr.yml`'s calling job was
  named differently from `release.yml`'s, meaning the required-status-check names the user had
  just configured in their branch ruleset would never have matched a PR run at all.
- Verified locally: `mkdocs build --strict` is clean; the built HTML actually contains 5
  real API-reference sections and correctly-rendered (not double-fenced) code examples; full
  test suite still 95/95 after the docstring edits.
- One-time setup only a maintainer with a Read the Docs account can do: import the repo at
  readthedocs.org (auto-detects `.readthedocs.yaml`) and confirm the project slug matches
  `dagster-cube-dbt` (or update the badge/`site_url` if not) -- documented in the README's new
  "Documentation" section.

## Out of scope (future work)

A companion component/asset layer to trigger Cube pre-aggregation refreshes downstream of
these virtual cube assets is the natural next project, but isn't part of this one. This plan
is scoped to make sure the generated asset keys and dependency edges are clean enough for
that layer to be built on top without rework.
