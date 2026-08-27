# Decisions, issues, and assumptions

A chronological log of non-obvious decisions, problems discovered, and assumptions made while
building this library. `PLAN.md` is the forward-looking design; this is the record of what
actually happened implementing it — kept up to date as work continues, not just written once
at the end.

## Phase 1 — generation core, merge engine, component wiring

### Decisions

- **Cube/dimension dicts are built from `cube_dbt`'s public `Model`/`Column` properties**
  (`name`, `description`, `sql_table`/`sql`, `type`, `meta`), not its private
  `_as_cube()`/`_as_dimensions()` dict methods (which are meant for its Jinja
  string-dumping use case). The only place generation reaches past the public API is a
  single helper checking whether a column's raw `data_type` was declared at all — `.type`
  can't tell "declared as string" apart from "nothing declared" after the fact. See
  `generation.py::_raw_data_type`, pinned by `test_raw_data_type_shape_is_pinned`.
- **`cube_translation` became overridable Python methods**, not a YAML-resolvable
  `TranslationFn` field. `get_cube_asset_spec()` / `get_view_asset_spec()` on
  `CubeDbtProjectComponent` mirror `DbtProjectComponent.get_asset_spec()`'s own
  override-in-a-subclass pattern. Simpler to implement correctly than wiring up
  `TranslationFnResolver`, and consistent with how the parent component already exposes
  its main customization point.
- **`output_dir`, `views_output_dir`, and `cube_select` are `field(kw_only=True)`.**
  `DbtProjectComponent`'s own fields (after `project`) all have defaults; a dataclass
  subclass can't add a required (no-default) field after inherited defaulted fields unless
  it's keyword-only. Since components are always constructed via keyword args by the
  Components framework anyway, this has no real downside.
- **The component's source directory (for patch-file discovery) is captured by overriding
  `load()`**, stashing `context.path` onto the instance. `write_state_to_path` doesn't
  receive a `ComponentLoadContext`, so it can't call `context.resolve_source_relative_path`
  itself — this had to be captured earlier, at load/resolution time, same as how `output_dir`
  itself gets resolved.
- **Generated cube/view files are always written as single-entity documents**
  (`{cubes: [<one cube>]}` / `{views: [<one view>]}`), never combined. This is what lets
  `output.py::read_entities` tell cube files apart from view files by top-level key even when
  `output_dir` and `views_output_dir` are the same directory (the default).
- **A `dagster_dg_cli.registry_modules` entry point was added** to `pyproject.toml` so `dg`
  can discover `CubeDbtProjectComponent` at all (`dg list components`, `dg scaffold defs`) —
  mirrors `dagster_dbt`'s own registration for `DbtProjectComponent`.

### Issues found

- **`Column.primary_key` (per-column) never looks at model-level `constraints`.** It only
  checks the column's own `primary_key` tag or `unique`+`not_null` test presence.
  Constraint-based PK detection (dbt 1.5+ `constraints: [{type: primary_key, ...}]`) only
  happens on `Model.primary_key` (the aggregated list, via `_detect_primary_key`, which
  prioritizes constraints and only falls back to per-column tag/test checks when there are
  none). Generation originally called `column.primary_key` directly, which would have
  silently produced wrong `primary_key: true/false` for any project using constraints
  instead of tests/generic tests. Fixed by computing `{c.name for c in model.primary_key}`
  once per model and checking membership. Caught by
  `test_generation.py::test_primary_key_detected_from_constraint` before it shipped —
  written specifically because the two properties looked interchangeable but weren't.
- **`pyyaml-merger` (the tool named in the original request) is not published to PyPI at
  all.** It's GitHub-only (`john-wd/pyyaml-merger`), last touched ~2021, pinned to exact
  ancient deps (`PyYAML==5.4.1`, `click==8.0.1`, `deepmerge==0.3.0`) that would conflict with
  dagster's own pins. Its actual logic is ~30 lines on top of `deepmerge`. Resolved by
  vendoring that algorithm on top of the actively-maintained `deepmerge` PyPI package
  instead of taking a git dependency on the original — user-confirmed decision.
- **`dg --help` / `dg scaffold --help` render as empty panels** (no Options/Commands
  sections at all) in this shell environment — reproduced identically in both Git Bash and
  PowerShell, with `COLUMNS` set explicitly, and independent of whether run inside a
  recognized `dg` workspace. Root cause not chased further since it's a CLI rendering
  quirk, not a functional block — worked around by reading `dagster_dg_cli`'s command
  registration source directly instead of relying on `--help` output.

### Assumptions

- dbt manifest columns always include `meta`, `tags`, and `description` keys (dbt populates
  these with empty defaults even when the user writes nothing) — `generation.py` and
  `cube_dbt` itself rely on direct dict access (`self._column_dict['tags']` etc.) rather than
  `.get()` with fallbacks. Verified against a real `dbt parse`d manifest, not just the
  synthetic fixtures used in `test_generation.py`.
- A cube's dbt-model dependency match is by name **against the whole manifest**, independent
  of `cube_select` — i.e. `cube_select` only controls which models get an auto-generated
  cube, not which model names are eligible to be linked back to as a dependency. A
  patch-defined cube named after a `cube_select`-excluded model would still correctly link to
  that model's dbt asset.
- A view's `cubes: [{join_path: "...", ...}]` entries are treated as literal cube names for
  dependency-wiring purposes (splitting on `.` and taking the first segment as a
  defensive measure for potential multi-hop `join_path` strings). Cube's full join-path
  syntax for deeply-nested view compositions wasn't exhaustively explored — this covers the
  common single-hop case.

## Phase 2 — `dg`-runnable example project (`dagster-cube-dbt-tests`)

### Decisions

- **`create-dagster` (a separate PyPI package) used instead of `dg scaffold project`.**
  The latter no longer exists in `dagster-dg-cli` 1.13.18 — confirmed by reading
  `dagster_dg_cli.cli.__init__`'s lazily-registered command groups directly (`check`,
  `utils`, `launch`, `list`, `scaffold`, `dev`, `api`, `labs`, `plus` — no `project`/`init`).
  Project scaffolding moved to the standalone `create-dagster project <path>` command.
- **`dagster-cube-dbt-tests` depends on the library via `[tool.uv.sources]`**
  (`{ path = "../dagster-cube-dbt", editable = true }`), so library changes are picked up
  live without reinstalling.
- **`requires-python` bumped from the scaffolded default (`>=3.10,<3.15`) to `>=3.13,<3.15`**
  to match `dagster-cube-dbt`'s own `>=3.13` floor, avoiding a resolvable-but-misleading
  version range.

### Issues found

- **`create-dagster project` crashed outright** (`OPENSSL_Uplink(...): no OPENSSL_Applink`,
  exit 1, no Python traceback — a native-level abort before Python's own error handling
  runs) when its PyPI update-check made an HTTPS call. Fixed with
  `DAGSTER_DG_UPDATE_CHECK_ENABLED=0`.
- **On this Windows development machine, `dbt.exe --version` reliably fails** the same way
  (`OPENSSL_Uplink`, no stdout at all — a harder failure than `dbt parse`, which still
  produces correct output despite a wrong exit code). Since `dagster_dbt`'s
  `DbtCliResource._cli_version` (a `cached_property` gating *every* dbt CLI invocation,
  including the `prepare`/parse step run by `write_state_to_path`) calls
  `subprocess.check_output(["dbt", "--version"])` with no tolerance for a nonzero exit code,
  this unconditionally blocks `dg utils refresh-defs-state` / `dg dev` against the real
  project on this machine — **before any of `dagster-cube-dbt`'s own code runs.**
  Diagnosis performed (each ruled out in turn):
  - Not Strawberry Perl shadowing OpenSSL DLLs via `PATH` (stripped it, no change).
  - Not import-time (`import dbt`, `import dbt.adapters.duckdb`, `import duckdb` all exit 0
    cleanly on their own).
  - Not dbt's own telemetry/version-check network calls (`DO_NOT_TRACK=1`,
    `DBT_SEND_ANONYMOUS_USAGE_STATS=false`, `--no-version-check` all made no difference).
  - Reproduces in a bare scratch venv containing only `dbt-core` + `dbt-duckdb` — not
    specific to this repo's dependency set.
  - Already on the latest `duckdb` (1.5.5 at time of writing) — not a stale-version bug.
  - `dg check yaml` (schema/`Resolver` validation only, no dbt execution) **succeeds** —
    confirming `defs.yaml`, `CubeSelect`, and the `output_dir`/`views_output_dir` resolvers
    are wired correctly through the real Components/`Resolver` pipeline. The failure is
    isolated to the dbt-CLI-execution boundary, not this library's resolution logic.
  - At this point the working theory was "a Windows DLL conflict between `dbt-duckdb`'s
    bundled OpenSSL and this machine's `uv`-managed Python, not fixable from here." **This
    turned out to be wrong in an important way — see Phase 3, which found the precise cause
    and an actual fix.**

### Assumptions

- (Superseded by Phase 3.) At the time this phase ended, `dagster-cube-dbt-tests`'s
  correctness was verified structurally (`dg check yaml`) and by equivalence with the
  library's test suite, without a successful live `dg dev`/`refresh-defs-state` run.

## Phase 3 — root-causing and fixing the environment blocker, then real end-to-end verification

The user pushed back on the Phase 2 conclusion ("I regularly run dagster dbt projects on
this machine, so why would this fail?") — rightly so. Re-investigating properly rather than
resting on the earlier hypothesis:

### Issues found (corrected root cause)

- **The crash is `uv`'s own managed python-build-standalone interpreter, not dbt-core,
  not dbt-duckdb, not any version of either.** Proven with a controlled comparison, all
  using the *exact* `dbt-core==1.11.13` + `dbt-duckdb==1.11.0` this project depends on:
  | Interpreter | `dbt --version` |
  |---|---|
  | `uv`-managed python-build-standalone 3.13.12 (what this project got, since it required `>=3.13`) | Crashes |
  | `uv`-managed python-build-standalone 3.12.13 (downloaded fresh to test) | Crashes — **so it's not a 3.13-specific bug either** |
  | System python.org-installed 3.12.7 (`D:\...\Programs\Python\Python312\python.exe`) | Works perfectly, including a live PyPI check |

  Further: `uv venv -p 3.12` (no flags) **silently downloads and prefers its own managed
  3.12.13** over the already-installed working system 3.12.7, even though both satisfy "3.12"
  — this is `uv`'s default `python-preference` behavior, not something that only bites you if
  you ask for a managed interpreter explicitly. Pinning `.python-version` to the *exact*
  system patch (`3.12.7`, matching no managed build) is what actually forces `uv` to use it.
  In-process reproduction (`python -c "from dbt.cli.main import cli; cli()"` wrapped in
  `try/except Exception`) caught nothing — confirms this is a native-level abort before
  Python's own exception handling ever runs, not a catchable Python error.

- **Fix applied**: both `dagster-cube-dbt` and `dagster-cube-dbt-tests` have
  `.python-version` pinned to `3.12.7`. `requires-python` lowered from `>=3.13` to `>=3.12`
  on both — nothing in the codebase used 3.13-only syntax (checked: no PEP 695 `type`
  statements, no `except*`, only `typing.Self`/`X | None` which are 3.11+/3.10+), and
  `dagster`/`dagster-dbt`/`cube_dbt` all support `>=3.10` or looser, so this wasn't a real
  constraint, just the scaffolded default.
  - `[tool.uv] python-preference = "system"` was added alongside the pin initially, as a
    belt-and-suspenders measure against future drift (if the system Python's patch version
    ever changes, an exact `.python-version` pin alone would stop matching and `uv` could
    fall back to downloading its own broken managed build again). **Removed again on the
    user's call**: the `.python-version` pin alone is sufficient once `uv.lock` itself
    records the resolution against 3.12, and the setting isn't normally needed day to day.
    Re-add it (see git history / this note) if a future `uv sync` on this machine ever
    silently reaches for a managed interpreter despite the pin. If 3.13 (or any version
    without a working local interpreter) ever needs testing on this machine, the plan is to
    use WSL rather than fighting `uv`'s managed-Python download on Windows.

- After the fix, `dg utils refresh-defs-state` and `dg list defs` **succeed completely**
  against the real project — full asset graph builds: 5 cube assets, 1 view asset with
  correct `deps`, 5 dbt assets, dbt's generic-test asset checks, and the default automation
  sensor, all correctly wired.

- **Two real bugs surfaced by this first successful real run — both masked by the synthetic
  fixtures used in Phase 1's unit tests:**

  1. **`merge.py`'s `_merge_list` leaked a literal `$mergeStrategy` key into output** for any
     element that didn't match an existing base entry (the "append" branch popped nothing,
     only the "match found" branches did — a bug inherited faithfully from the original
     vendored algorithm, see Phase 1). Exposed because `cube_select` (next bug) accidentally
     matched zero models, so every patch element hit the append branch instead of the merge
     branch, and the resulting `journey_samples.yaml` came out as raw, un-merged patch
     content with a stray `$mergeStrategy: remove` key still attached instead of an actually
     merged cube. Fixed: the strategy key is now always popped first, before branching, and
     `remove` on a non-existent item is now a documented no-op (skip) rather than leaking a
     meaningless key onto an appended item. Two regression tests added
     (`test_appended_new_item_does_not_leak_merge_strategy_key`,
     `test_remove_on_unmatched_item_is_a_noop`).
  2. **`cube_select: {paths: ["models/marts"]}` never matches anything.** dbt manifest
     node `path` is relative to the `model-paths` root (`models/` by default) — a model at
     `models/marts/journey_samples.sql` has manifest `path` `marts/journey_samples.sql`
     (backslash on Windows), so a filter of `"models/marts"` never matches as a prefix. This
     wrong example had propagated into the component's own docstring, its `Resolver`
     `examples=`, both READMEs, and PLAN.md — all written from the same incorrect assumption,
     and none of it caught by unit tests because `test_generation.py`'s synthetic manifests
     used simplified paths (`"marts/model.sql"`) that happened to make the bug untestable.
     `test_component_integration.py` and `tests/fixtures/dbt_project` happened to use the
     *correct* form (`paths=["marts"]`) already, which is why the library's own test suite
     never caught this — it only showed up once a real `dbt`-generated manifest and a
     hand-written example `defs.yaml` were both in play at once. Fixed everywhere it
     appeared, and the `paths` semantics are now spelled out explicitly in the README and the
     component's own `Resolver` description so this can't recur silently.

### Assumptions

- `dagster-cube-dbt-tests` is now verified via an actual, successful `dg utils
  refresh-defs-state` + `dg list defs` run on this machine, not just structurally or by
  equivalence with the unit test suite. The `.python-version`/`python-preference` fix is
  specific to this machine's toolchain; a machine where `uv`'s managed Python doesn't have
  this issue wouldn't need it, but the pin is harmless there either way.
- This phase is a good illustration of why the real fixture project was worth building even
  though the unit test suite already had 27 (then 29) passing tests: both bugs found here
  were invisible to synthetic-manifest unit tests by construction, and only surfaced once a
  real dbt-parsed manifest and a hand-authored `defs.yaml` were exercised together through
  the actual `dg` pipeline.

## Phase 4 — column meta passthrough, file promotion hook

### Decisions

- **`meta.cube` is now a fully reserved namespace with two kinds of keys**: `dimension`
  (control flag, consumed, never emitted) and `order`/`mask`/`public` (promoted to
  top-level attributes on the generated dimension, not left nested under `meta:`). Anything
  else under `cube.*`, or any meta outside the `cube` namespace, still passes through into
  the dimension's own `meta:` unchanged. Chosen over the alternative of leaving `order` etc.
  inside `meta:` because the request was explicitly to have them "read into the cube model"
  as first-class attributes, not just tagged as arbitrary metadata — and because `public` is
  already a real, native Cube dimension attribute (API visibility), it needed to land at the
  top level to mean anything to Cube itself.
  - Fixed a related smell while doing this: previously the *whole* `column.meta` dict
    (including our own internal `cube.dimension` control flag) got dumped verbatim onto the
    dimension's `meta:` field. That flag now never leaks into output.
- **File promotion is a single overridable method (`promote_cube_files`), not a separate
  asset, not a resource, not a config-driven strategy selector.** Matches the user's own
  proposed design exactly: called once per materialization of the cube/view multi-asset,
  before any `MaterializeResult` is yielded. No-op by default — `output_dir`/
  `views_output_dir` are assumed to already be where the Cube server reads from (e.g. a
  mounted/shared volume) unless overridden.
- **Did not build `dagster-cube-dbt[s3]`/`[git]` extras.** The user floated both "just
  override a method" and "ship opinionated extras with the method pre-filled" as options;
  asked explicitly, and confirmed the documented-pattern-only approach is enough for now —
  packaged extras would mean real new dependencies, credential/auth handling, and testing
  against actual external systems, meaningfully more scope than the hook itself. The
  git-sync-sidecar example the user described (their own real setup) is documented as a
  worked example in both the docstring and the README. Revisit if this becomes a recurring
  need rather than building speculatively now.

### Assumptions

- `promote_cube_files` runs regardless of which subset of cube/view assets was selected for
  a given run (`can_subset=True` on the multi-asset) — it always pushes whatever the *whole*
  `output_dir`/`views_output_dir` currently holds, not per-selected-asset files. Generation
  already regenerates that whole set atomically at refresh time, so a partial push would risk
  leaving the target (S3/git/etc) with a mix of stale and fresh files; a full push each time
  keeps it consistent regardless of which assets triggered the run.

## Phase 5 — `cube` kind tag

- **Decision**: `kinds={"cube"}` added to both `get_cube_asset_spec` and
  `get_view_asset_spec`. Verified two ways: a unit test asserting
  `asset_graph.get(key).kinds == {"cube"}`, and a real `dg utils refresh-defs-state` +
  `dg list defs` run against `dagster-cube-dbt-tests`, confirming the "Kinds" column actually
  shows `cube` for the generated assets, not just that the tag exists in isolation.

## Phase 6 — actually making the assets virtual (`AssetSpec(is_virtual=True)`)

### A real miss, corrected

The very first message of this whole project said the assets "must be virtual, as they pass
through data from upstream to downstream without ever storing it." That framing was used
throughout every doc and docstring written since — but it turns out Dagster has a literal,
first-class feature for exactly this: `AssetSpec(is_virtual=True)` (currently a preview
feature — `dagster_shared.utils.warnings.preview_warning`, "may have breaking changes in
patch version releases"). It wasn't used. Instead, the design approximated "virtual" with
`automation_condition=eager()` plus always emitting a `MaterializeResult` on every run — a
reasonable-looking but home-grown substitute for a feature that already existed and does the
job better. The user caught this by directly asking whether the original ask had actually
been fulfilled, rather than trusting the "virtual" language already in the docs.

### What `is_virtual` actually does (read from `dagster/_core/definitions/data_version.py`
and `.../assets/graph/base_asset_graph.py` before using it, not assumed)

- `BaseAssetGraph.get_non_virtual_ancestor_keys(key)`: walks a node's direct parents; for any
  parent that is itself virtual, recurses into *its* parents instead of stopping there. The
  result is the set of nearest **non**-virtual ancestor keys, however many virtual hops away.
  Confirmed recursive through a real two-hop chain (view → cube → dbt model) both in
  isolation and against this component's actual generated asset graph.
- The staleness resolver (`CachingStaleStatusResolver._get_status` /
  `_get_stale_causes`): for a virtual asset that has *never* been materialized (`NULL_DATA_VERSION`),
  status is computed by checking whether any of its non-virtual ancestors (via the method
  above) have been materialized — the virtual asset itself never needs a materialization
  event for that determination to work. This is the mechanism that makes "downstream
  pre-aggregation assets react correctly when the virtual assets' upstream deps change"
  (the original request) actually true at the framework level, rather than something this
  component has to fake by forcing eager re-materialization of the whole virtual layer.

### Decisions

- **`is_virtual=True` added to both `get_cube_asset_spec` and `get_view_asset_spec`.**
- **Kept `automation_condition=eager()` and the existing `MaterializeResult`-yielding op.**
  `is_virtual` makes materializing the virtual layer *unnecessary* for freshness propagation,
  but not useless: the multi-asset op is still where `promote_cube_files` runs (Phase 4), and
  eager materialization still gives the asset catalog real materialization history. This
  wasn't re-litigated with the user — it's an independent decision from `is_virtual` itself,
  flagged here rather than silently changed.
- **Noted but not acted on**: eager auto-materialization is triggered by the dbt model's
  *data* changing, not by the cube *schema* changing (which only happens at
  `refresh-defs-state` time) — so `promote_cube_files` could run more often than the actual
  generated files change. Not a correctness problem (a git commit/push with no diff is a
  no-op; an idempotent S3 upload is too), but worth the next person knowing about if they're
  choosing what to put in a `promote_cube_files` override.

### Verification

- Unit test (`test_cube_and_view_assets_are_virtual_and_freshness_looks_through_them`):
  asserts `is_virtual` on cube/view specs, `not is_virtual` on the real dbt model asset, and
  `get_non_virtual_ancestor_keys` resolving correctly through the view → cube → dbt-model
  chain (including a view with two member cubes resolving to both underlying models, and the
  patch-only `exchange_rates` cube — virtual, but with zero non-virtual ancestors since it
  has no `deps` at all).
  - The behavior actually exercised is the ancestor-resolution mechanism the staleness
    resolver depends on, not the full `CachingStaleStatusResolver` (which needs a
    `DagsterInstance` + `LoadingContext` to construct — heavier internal-API surface than
    seemed worth pulling into a unit test for a preview feature). `get_non_virtual_ancestor_keys`
    is what determines the actual behavior; the resolver's own logic on top of it was read,
    not independently re-verified.
- Re-ran `dg utils refresh-defs-state` and `dg check defs` against the real
  `dagster-cube-dbt-tests` project after the change — both succeed cleanly, confirming
  `is_virtual` doesn't break real component loading (only a `PreviewWarning`, non-fatal, same
  as the `BetaWarning`s already seen from `dagster_dbt`).

## Phase 7 — automation condition: `code_version` instead of `eager()`

### The ask

Cube assets shouldn't use `eager()` — it re-runs on every upstream dbt model *data* update,
but a cube's generated content only changes when its dbt model or a merge patch changes, not
every time the model's data refreshes. The right trigger is "run once after the asset's own
`code_version` changes" (a hash of its generated YAML), gated on its deps being ready. Views
should use the same condition. A custom, scoped `AutomationConditionSensorDefinition` (rather
than relying on the platform default) was also requested.

### Getting the condition wrong first, then right (worth recording in full — the failure
mode is instructive)

First attempt: `(newly_missing() | code_version_changed()).since_last_handled() & ~any_deps_missing() & ~any_deps_in_progress() & ~in_progress()`
— structurally mirroring `eager()`'s own composition (`(newly_missing() | any_deps_updated()).since_last_handled() & ...`),
just swapping the trigger clause. This seemed reasonable by analogy but was wrong, and empirical
testing (via `dg.evaluate_automation_conditions` + `report_runless_asset_event`, both against a
minimal repro *and* directly against the real docstring example for `eager()` itself) showed:
- `.since_last_handled()` unconditionally suppresses the *very first* evaluation ever,
  regardless of the inner condition — confirmed by testing `missing()` alone (fires on tick
  1) vs. `missing().since_last_handled()` (doesn't, on tick 1; does on tick 2). This is a
  deliberate safety behavior (don't mass-materialize a whole pre-existing asset graph the
  moment an automation condition using it is first turned on), not a bug -- but it meant even
  the *actual* `eager()`, run against the exact literal code from its own docstring example,
  did not reproduce the documented "requested == 1 on the first tick" result in this
  environment/version (1.13.18) when tested directly. Whether that's a stale docstring, a
  version-specific change, or something about the preview feature specifically wasn't
  resolved -- the practical fix didn't require resolving it.
- Wrapping `code_version_changed()` in `.since_last_handled()` was based on an incorrect
  assumption (that its cursor advances on every evaluation regardless of outcome, the same
  way a raw edge-triggered condition would, and so could be "lost" while blocked by a
  not-yet-ready dependency). **The user corrected this directly**: `code_version_changed()`
  is not an edge/event condition — it stays true from the tick the version changes until an
  evaluation actually picks it up, so it doesn't need `.since_last_handled()` at all. The user
  also pointed out the `newly_missing()` choice was needlessly complex: since the whole point
  of wrapping in `.since_last_handled()` was to make a trigger "sticky" across blocked ticks,
  just wrap the simpler *level* condition `missing()` directly, rather than layering
  `missing().newly_true()` (`newly_missing()`) underneath it.

Both corrections were verified empirically (not just accepted on faith) via a 5-tick
`evaluate_automation_conditions` sequence against a minimal two-asset repro before touching
the real component code again, and again with a full 7-tick sequence against the actual
generated `journey_samples` cube asset (see `test_generated_asset_automation_condition_only_fires_on_code_version_change`):
nothing materialized (tick 1: suppressed, initial evaluation) → still nothing, dep missing
(tick 2: blocked) → dep materializes (tick 3: requested, exactly once) → materialize the cube
itself, nothing else changes (tick 4: not re-requested) → dep re-materializes with new *data*,
`code_version` unchanged (tick 5: **not** re-requested — the actual point of this whole
change) → a merge patch changes the generated cube's content, `code_version` changes (tick 6:
requested, exactly once) → nothing further changes (tick 7: not re-requested).

### Decisions

- **Final condition**: `(missing().since_last_handled() | code_version_changed()) & ~any_deps_missing() & ~any_deps_in_progress() & ~in_progress()`,
  as `GENERATED_ASSET_AUTOMATION_CONDITION` in `component.py`, applied to both cube and view
  specs (per the user's "views would probably work with the same automation condition" —
  superseding an earlier, passing "eager makes sense on the view" comment in the same
  message, resolved in favor of the final, unified conclusion).
- **`code_version` on every cube/view spec** is `sha256(generated_yaml_text)[:16]` — see
  `_code_version()`. Deterministic and changes exactly when the entity's generated content
  changes, independent of dbt data.
- **A dedicated `AutomationConditionSensorDefinition`** (`<dbt_project_name>_cube_automation_condition_sensor`,
  `default_status=RUNNING` since a custom sensor otherwise starts `STOPPED` unlike the
  always-on platform default) targets exactly the cube/view asset keys via
  `AssetSelection.assets(...)`. Confirmed via source reading
  (`get_default_automation_condition_sensor_target` in `dagster/_core/definitions/utils.py`)
  that Dagster automatically excludes any asset targeted by an explicit
  `AutomationConditionSensorDefinition` from the platform's own default one — no double
  evaluation. Verified for real against `dagster-cube-dbt-tests`: `dg list defs` shows
  `cube_dbt_fixture_cube_automation_condition_sensor` as the only sensor, not a default one
  alongside it.

### Assumptions

- (Resolved by Phase 8 below.) The `eager()`-docstring-vs-reality discrepancy noted above
  turned out to have a real explanation, not a stale doc or version quirk -- see Phase 8.

## Phase 8 — getting the `missing()` clause right (two more rounds of correction)

Phase 7's condition shipped with `missing().since_last_handled() | code_version_changed()`.
It passed the test written for it at the time, but that test didn't probe hard enough. Two
more rounds of user-caught correction, in order:

### Round 1 — "why does `missing()` need `since_last_handled()` at all? It already stays true
until first materialization."

Fair question, and the honest answer required testing, not just reasoning about it:
- `missing()` alone (no wrapping) **does** work for the "eventually recovers once a blocked
  dep becomes ready" case -- confirmed. But without any wrapping, it has no memory of "I
  already requested this," so once a request finally goes out but the asset is still
  pending (not yet actually materialized, not yet visible as `in_progress` -- e.g. the run
  hasn't started yet, or `evaluate_automation_conditions` doesn't simulate a real run at
  all), it starts **re-requesting on a later tick** even with nothing else changed --
  confirmed empirically (tick 1 requested, tick 2 requested *again*, both with nothing
  materialized or launched in between). `~any_deps_missing()`/`~in_progress()` don't cover
  this because they gate on *dependency*/*self* run state, not "was a request already
  issued for this specific transition."
- So `.since_last_handled()` earns its place here, but for a different, more precise reason
  than Phase 7's writeup gave (which said the trigger would be "lost" if blocked -- true for
  edge-triggered conditions, but not the actual risk with a level condition like `missing()`
  alone). The real risk is duplicate requests during the request→actually-visible-as-running
  gap.

### Round 2 — the user quoted the actual Dagster docs' canonical pattern: `missing().newly_true().since_last_handled()`

This is the literal definition of `newly_missing()` (`missing().newly_true()`), i.e. Phase
7's *very first* attempt, before Phase 7's own (incorrect) simplification away from it. So
the docs point back to where this started. Re-verifying that exact canonical pattern against
the real generated cube asset, carefully this time (nothing skipped), showed it does **not**
recover once blocked at the transition tick: requested 0 on every one of 4 ticks, even after
the previously-missing dependency became ready. Root cause, understood by comparing directly
against real `eager()`'s own structure:
- `newly_true()` tracks a single transition (false→true). For an unpartitioned asset that has
  been missing continuously since the very first evaluation, there is exactly one such
  transition, and if the outer AND-gate (deps missing) blocks the request at that exact tick,
  the transition is gone -- it will never be true again for this asset, ever, regardless of
  what happens to its deps afterward.
- `eager()` doesn't have this problem because its trigger clause is `newly_missing() |
  any_deps_updated()` -- **two independent triggers**. `any_deps_updated()` provides a
  separate recovery signal: when the blocked dependency itself later updates, that's a fresh
  event on its own, independent of whether `newly_missing()`'s one-shot window already
  closed. Phase 7 swapped `any_deps_updated()` for `code_version_changed()` specifically to
  stop reacting to every dependency data update -- which also silently discarded the
  recovery path that made the original pattern's `newly_missing()` half actually work in
  practice.

**The actual fix**: wrap `.newly_true().since_last_handled()` around the *whole* "ready to
run" state -- `(missing() & ~any_deps_missing() & ~any_deps_in_progress())`, not around bare
`missing()` ANDed with the deps gate afterward. The transition being tracked is now "this
asset just became actually able to run" (whenever that happens, including well after it
first became missing), not "this asset just became missing" (which only ever happens once).
This simultaneously fixes both rounds' problems: it recovers correctly when blocked at first,
*and* only fires once per such transition (no duplicate-request risk), because it's still
wrapped in `.since_last_handled()`.

Verified with a 7-tick sequence (nothing → still blocked → dep ready → requested once →
materialize → dep refreshes with same code_version → still not re-requested → code_version
changes → requested once → nothing further → not re-requested), *and* a dedicated regression
test (`test_generated_asset_automation_condition_does_not_duplicate_request_while_pending`)
that specifically leaves the asset un-materialized for several ticks after its one legitimate
request, to catch Round 1's exact failure mode if it were ever reintroduced.

### A third finding, this one not a bug

Building that regression test surfaced one more real behavior, checked directly against
`eager()` itself before concluding anything: if a cube's dbt model is *already* materialized
**before** the cube asset's very first automation-condition evaluation (e.g. adding this
component to an existing, already-running dbt project), the fixed condition never
auto-materializes the cube on its own -- the "missing and deps ready" state is already true
at the baseline tick (which `.since_last_handled()` deliberately suppresses, to avoid
mass-materializing a whole pre-existing asset graph the instant an automation condition using
it is first turned on), and no further transition ever happens after that. Confirmed the real
built-in `eager()` has the identical characteristic in the identical scenario (0 requested
across 4 ticks). This is not a gap introduced here -- it's how Dagster's declarative
automation fundamentally works: automation conditions manage transitions from the point
they start being evaluated forward, not retroactively act on state that was already true
before that. A component's very first cube materialization needs one manual kick if its dbt
model already had data before the component was ever added; every materialization after that
is handled automatically. Documented in the package README and in the regression test's
docstring so it isn't mistaken for a bug later.

### Decisions

- **Final condition, replacing Phase 7's**:
  `((missing() & ~any_deps_missing() & ~any_deps_in_progress()).newly_true().since_last_handled() | code_version_changed()) & ~in_progress()`.
- Kept as `GENERATED_ASSET_AUTOMATION_CONDITION`, applied to both cube and view specs,
  unchanged from Phase 7 otherwise (same sensor, same `code_version` computation).

### Assumptions

- The "first materialization needs a manual kick if the dbt model predates the component"
  behavior is accepted as-is, matching `eager()`'s own characteristic, rather than engineered
  around (e.g. by trying to special-case "already ready at time zero"). Revisit only if this
  turns out to be a recurring real friction point, not preemptively.

## Phase 9 — dbt-layer parity testing (a real, previously-missing gap)

### The gap

Every test up to this point exercised the cube/view layer thoroughly, but none of them ever
compared the *inherited* dbt-asset layer against what plain `dagster_dbt.DbtProjectComponent`
would produce for the same project. The assumption -- "we don't override `get_asset_spec`,
`execute`, or any of the dbt-facing resolver fields, so it must be identical, we just call
`super()`" -- is reasonable, but was never actually verified. A subtle interaction (in
`Definitions.merge`, in how `write_state_to_path`/`build_defs_from_state` compose with the
parent's own state handling, or in a future `dagster-dbt` upgrade) could silently change dbt
asset behavior for anyone who swaps this component in for the vanilla one, and nothing in the
suite would have caught it.

### What was built (`tests/test_dbt_layer_parity.py`)

- **Spec-level parity**: builds `Definitions` from a plain `DbtProjectComponent` and from
  `CubeDbtProjectComponent`, both against the same fixture project, and compares every
  dbt-model `AssetNode`'s `description`, `group_name`, `tags`, `kinds`, `code_version`,
  `owners`, `parent_keys` (upstream deps), `check_keys`, partitioning, and metadata *keys*
  (not exact metadata values, which can legitimately carry incidental differences like
  embedded timestamps) field-by-field. `child_keys` is deliberately excluded from comparison
  -- cube assets add themselves as new children of their dbt model, which is the intended,
  additive difference, not drift.
- **Execution-level parity**: actually runs `dbt build` through both components' generated
  multi-assets (not just comparing declared specs) and confirms the same dbt models
  materialize and the same generic-test-derived asset checks pass, with identical results.
  This is the strongest available "no drift" signal, since it exercises the real dbt CLI
  invocation path dagster_dbt builds, not just what we declared about it.
- Both use the `NoopDbtProjectManager` pattern already established elsewhere in the suite
  (pre-instantiated `DbtProject`, `state_path=None`/pre-parsed manifest), so no dbt
  prepare/parse subprocess is needed for the spec-level tests -- only the execution-level
  test actually shells out to `dbt build` (twice, ~8s each; acceptable given what it verifies).

### What the first run surfaced (fixture setup, not a real component bug)

The first attempt failed immediately with `MissingDimensionTypeError` on
`int_raw_journey_events.raw_payload` -- because the parity test constructed
`CubeDbtProjectComponent` without a `cube_select` scope, so cube generation ran against *all*
five fixture models, including the one deliberately given an untyped column specifically to
test the `cube_select`-exemption behavior (see Phase 1). Fixed by scoping `cube_select` to
`marts` in the test, matching how the fixture is meant to be used -- while deliberately
*not* passing `select`/`exclude`/`selector` differently between the two components, so all
five dbt models still get built identically either way. This is exactly the distinction the
README documents: `cube_select` controls what gets a cube, never what dbt itself builds.

The two spec-level tests and the execution-level test all passed on the very next attempt
(after that fixture-scoping fix, and one API-usage fix -- `AssetCheckEvaluation` objects
returned by `get_asset_check_evaluations()` are used directly, not nested under an
`.asset_check_evaluation` attribute). This is a genuinely reassuring result: the assumption
("we don't touch anything dbt-facing, so it must be identical") held up under real
verification, rather than needing an actual component fix. That doesn't make the tests
redundant to have built -- confirming an assumption that was never checked is exactly the
point, and the suite now guards against the assumption becoming false on a future change or
`dagster-dbt` upgrade.

### Assumptions

- Only `AssetNode`-level fields and full-execution materialization/check results were
  compared -- not, e.g., byte-identical op names, run tags, or code_version fingerprints of
  the underlying `@dbt_assets` function object itself. If a future concern is specifically
  "is the exact same op/job structure produced," that would need a different, more invasive
  comparison than this phase built.

## Phase 10 — `$mergeStrategy: patch`: a real bug, and why per-file marking was rejected

### The bug, traced precisely before designing a fix

The user raised: nothing marks a merge-patch file as expecting to modify something that
already exists, vs. being free to create something new. Concretely: if the dbt model behind
a patched cube (e.g. `journey_samples`) is later renamed or dropped, a patch meant to modify
it (e.g. `remove_journey_type.yaml`, removing a dimension) stops matching at the cube level
and, under the merge behavior as it stood, silently gets appended as a *new* cube instead of
raising anything.

Traced the actual resulting output, not just the abstract risk: the append-when-unmatched
branch (`_merge_list`) only strips the item's *own* top-level `$mergeStrategy` key -- it does
not recursively re-process the item's nested lists through the merge machinery (that only
happens for matched items, via `self.value_strategy(...)`). So the orphaned append would
retain its nested `dimensions: [{name: journey_type, $mergeStrategy: remove}]` **verbatim,
stray key and all**, and the resulting cube would also be missing `sql_table` (never present
in a patch fragment meant to modify an existing cube, only to remove a dimension from it).
This is genuinely broken output, not just "an unexpected new cube" -- written to disk and
pushed toward the Cube server to fail there, with nothing surfaced at generation time.

### Why per-file marking (my first suggestion) was wrong

I initially proposed a per-file `patch: true` flag. The user correctly rejected this: a
realistic pattern is one file that patches an existing cube (e.g. adding a join) and then
defines a handful of new cubes below it in the same file that build on that patch --
splitting that across files just to satisfy a per-file marker would be needless friction with
no benefit. The marker needs to be per-item.

### Why YAML anchors/aliases (the user's other suggestion) weren't the right tool

The user also floated hijacking YAML's native `&anchor`/`*alias` syntax to make a patch
marker visually distinct (their stated objection to a plain boolean flag: "it blends in").
Anchors/aliases are a different mechanism entirely -- reusing a *value* elsewhere in the same
document -- and repurposing them for match-or-error semantics would be solving this with an
unrelated tool, before even accounting for how awkwardly that interacts with `cubes:` being a
list of dicts (which the user themselves flagged as a complication).

### Decisions

- **`$mergeStrategy: patch`**, a fourth value alongside the existing `remove`/`replace`/
  default-`merge`, reusing the already-established `$`-prefixed marker convention instead of
  inventing a second, parallel marking mechanism. Per-item by construction, since
  `$mergeStrategy` already lives on individual list entries. Behaves exactly like the default
  merge strategy once matched -- the only difference is what happens when there's no match:
  collected (across every patch file folded into a single `merge_documents()` call, via new
  `StrategicMerger.unmatched_patch_targets`/`unmatched_remove_targets` list attributes) and
  raised together as one `UnmatchedPatchTargetError`, rather than silently appended.
- **`remove` targeting nothing stays a no-op, not an error** -- deliberately not unified with
  `patch`'s strictness. The two failure modes are different in kind: an unmatched `remove`
  converges to the same *output* either way (the target isn't there whether removal ran or
  was moot), while an unmatched default-merge/`patch` produces a materially different,
  broken output (a fabricated, incomplete entry). Converging cases are safe to no-op;
  non-converging ones are not.
- **Unmatched `remove` now warns** (user-prompted follow-up): via `warnings.warn` (no new
  dependency, and it surfaces the same way the existing `PreviewWarning`/`BetaWarning`s from
  dagster/dagster_dbt already do in `dg utils refresh-defs-state` output), aggregated the
  same way as the `patch` error. Not upgraded to an error, since the safety argument above
  still holds -- it's a hygiene signal (this patch is probably stale, clean it up), not a
  correctness one.
- Retrofitted `$mergeStrategy: patch` onto the two existing patch files (in both the
  library's own fixture-backed integration test and the `dagster-cube-dbt-tests` example
  project) that actually modify an existing cube (`journey_samples.yaml`,
  `remove_journey_type.yaml`); left the two that introduce genuinely new resources
  (`exchange_rates.yaml`, `journeys_overview_view.yaml`) unmarked. Re-verified against the
  real `dagster-cube-dbt-tests` project via a full `dg utils refresh-defs-state` + `dg check
  defs` run after the change -- output is clean, no stray `$mergeStrategy` keys, no spurious
  errors.

### Assumptions

- No file-path/source tracking was added to the error or warning messages (both report only
  the matched-key `name` values, not which patch *file* introduced them). Considered it, but
  given `merge_documents()` currently takes a plain `Iterable[dict]` with no file identity
  attached, threading that through would have meant a real signature change for a nice-to-have
  message improvement. With patch counts realistically small per project, the `name` alone
  should usually be enough to locate the offending file by search. Revisit if this turns out
  to be a real pain point in practice, not preemptively.

## Phase 11 — dbt Fusion: a second engine to test against

### Two other, smaller items resolved first

- **The "patch has no target" test already existed** (`test_patch_strategy_raises_when_unmatched`,
  added in Phase 10) — confirmed rather than assumed, by pointing at it directly.
- **User hit a confusing raw `pydantic_core.ValidationError` traceback from `dg utils
  refresh-defs-state`** for a missing required field (`output_dir`) on their own subclass of
  `CubeDbtProjectComponent`, and asked whether first-party components surface missing-field
  errors the same way. Verified empirically rather than asserted: reproduced the identical
  failure mode with a genuine first-party component (`dagster_dbt.DbtProjectComponent`
  missing its required `project` field) via `dg utils refresh-defs-state` -- same
  `ComponentTreeException`-wrapped Pydantic traceback, word for word in structure. This is
  generic Components-framework behavior, not anything specific to this library. Also found
  (and worth knowing, independent of us): `dg check yaml` gives a dramatically clearer error
  for the exact same problem (`'output_dir' is a required property`, with a source-line
  pointer) for *both* the vanilla and custom component -- worth reaching for first when
  `refresh-defs-state`/`dev` produce a wall of traceback.

### Fusion feasibility, investigated empirically before proposing anything

- **Installs cleanly on Windows**: `pip install --pre dbt` (or an exact prerelease pin, e.g.
  `dbt==2.0.0rc210`), version reports as `dbt-fusion 2.0.0-preview.N`.
- **Dagster already has first-class support**: `DbtCliResource` auto-detects Fusion vs. Core
  since dagster 1.13.5 (per Dagster's own blog post on the integration) -- no code changes
  needed in this library for basic compatibility.
- **Cannot coexist with dbt-core in the same environment.** Fusion's PyPI distribution is
  literally named `dbt` (colliding with dbt-core's own `dbt` console script) -- installing
  both in one venv means one overwrites the other's `dbt.exe`. Any dual-engine setup needs
  genuinely separate environments, not an added dependency or an extra.

### Two real bugs found, only by actually running the fixture against Fusion

1. **`meta:` declared bare on a dbt column is silently different between engines.** dbt-core
   accepts it; Fusion rejects it outright (`UnusedConfigKey (dbt1060)`). Both engines accept
   -- and, critically, produce an *identical* compiled-manifest shape for -- `meta:` nested
   under `config:` (verified by inspecting `target/manifest.json` from both engines directly,
   not assumed from docs). **Fixed**: switched the library's fixture, both README examples,
   and the `dagster-cube-dbt-tests` example project's fixture to the `config:`-nested form.
   No code changes needed -- this is purely a schema.yml authoring convention, and the
   `config:`-nested form is arguably the more modern/correct one regardless of Fusion.
2. **`:memory:` DuckDB doesn't behave the same under Fusion with multiple models.** Building
   all 5 fixture models against `:memory:` under Fusion: 1 succeeds, the other 4 fail with
   `Catalog with name main does not exist!` (`DbDriverFailed dbt1308`). Confirmed this isn't a
   `threads:` config issue (explicit `--threads 1` on the CLI made no difference) -- looks
   like Fusion's DuckDB adapter doesn't share one in-memory catalog across the separate
   connections it opens per model, the way dbt-core's does. **Fixed** with a named on-disk
   file (`target/fusion_test.duckdb`, already inside the gitignored `target/`) as a *second*
   profile target (`fusion`), rather than switching dbt-core's default away from `:memory:`
   -- an on-disk file adds real file-locking/flakiness risk (a crashed prior run can leave a
   stale lock) that isn't worth taking on for the primary, most-frequently-run dbt-core path
   just to accommodate Fusion. Verified: all 5 models + 8 tests pass against the on-disk
   target under Fusion.

### Decisions

- **`tests/dbt_engine.py`**: a single-purpose module exposing `DBT_TARGET`, read from
  `DAGSTER_CUBE_DBT_TEST_DBT_TARGET` (defaulting to `"dev"`), naming which `profiles.yml`
  target to use. Threaded through every `DbtProject(...)` construction in the test suite
  (`test_component_integration.py`, `test_dbt_layer_parity.py`) rather than duplicating the
  env-var read in each file. `conftest.py`'s `dbt parse` call was left alone -- parsing
  doesn't touch the database, so it doesn't matter which target (both `type: duckdb`) is
  selected for it.
- **Hatch matrix environments** (`[tool.hatch.envs.test]` + `[[tool.hatch.envs.test.matrix]]`
  with a `dbt-engine` axis) to drive the two runs, per the user's direct ask ("doesn't hatch
  support test matrices?") rather than a hand-rolled shell script or introducing `tox`/`nox`.
  `python = "3.12"` pinned explicitly on the environment, matching the `.python-version`
  pin already established for `uv` (see Phase 3) -- Hatch has its own, separate Python
  resolution machinery from `uv`'s, so this needed re-verifying independently rather than
  assumed to inherit the earlier fix. Verified: `hatch run test.core:python -c "import sys;
  print(sys.executable)"` resolved to the same working system Python 3.12.7, not a freshly
  downloaded one -- the exact failure mode Phase 3 fixed for `uv` doesn't reappear here.
- **A third bug, found setting the matrix up, before either fixture bug**: the naive matrix
  override `{ value = "dbt", if = ["fusion"] }` (a bare, unconstrained dependency) resolved
  to **`dbt-cloud-cli` 0.40.18** -- a real, published, *completely unrelated* product, also
  distributed under the bare name `dbt`, since Hatch's default (pip-based) installer doesn't
  consider prereleases unless asked to, and quietly fell back to the latest stable release
  under that name instead of erroring. This would have been a silent, confusing failure: the
  fusion leg would have "installed successfully" and then failed at test time with
  unrelated-looking dbt-cloud-cli errors, giving no hint that the real cause was a dependency
  name collision. **Fixed** by changing the spec to `dbt>=2.0.0rc0` instead of an exact pin
  or a `--pre`/prerelease-allow flag: per PEP 440, a version specifier whose *lower bound* is
  itself a prerelease implicitly permits prereleases to satisfy it, which excludes
  dbt-cloud-cli (currently 0.x) without needing an installer-level prerelease flag, and keeps
  picking up newer Fusion prereleases automatically as they're published, rather than going
  stale the way an exact pin (`dbt==2.0.0rc210`) would. Verified directly against both `uv`
  and Hatch's own installer, not assumed to generalize from one to the other.

### Verification

All of the following were actually run, not assumed:
- `hatch env show` -- confirms the matrix config parses into `test.core`/`test.fusion`.
- `hatch run test.core:python -c "..."` -- confirms the system Python 3.12.7 is used, not a
  freshly downloaded one.
- `hatch run test.core:run` -- **45/45 passed** (dbt-core, ~33s).
- `hatch run test.fusion:dbt --version` -- confirms `dbt-fusion 2.0.0-preview.210` (not
  dbt-cloud-cli) after the version-spec fix.
- `hatch run test.fusion:run` -- **45/45 passed** (dbt Fusion, ~9s -- notably faster, expected
  given Fusion is a Rust reimplementation with much lower CLI startup overhead than dbt-core's
  Python-based CLI).

### Assumptions

- Both matrix legs currently pull in the *full* dependency list (`dagster`, `dagster-dbt`,
  `cube_dbt`, `pyyaml`, `deepmerge`, `pytest`) plus their engine-specific dbt package, rather
  than reusing `dependency-groups.dev` from the rest of the project. Hatch's matrix
  `dependencies`/`overrides` system doesn't compose with PEP 735 dependency groups the way
  `uv`'s does, so this is some duplication to keep in sync by hand if the core dependency list
  changes. Judged acceptable given the list is short and changes rarely; revisit (e.g. by
  reading `pyproject.toml`'s own `[project.dependencies]` programmatically, or moving to a
  `uv`-native matrix mechanism if one matures) if it starts drifting in practice.
- Only the full `pytest tests/` run was verified through the matrix, not a narrower "only the
  tests that actually invoke the dbt CLI" subset. Given the whole suite runs in under 35s
  even for the slower (dbt-core) leg, there was no need to narrow it for speed.

## Phase 12 — `output_dir` was doing the wrong job at the wrong time

### Problem

User pushback, correctly identified as a real architectural bug, not a style nit: `output_dir`
was required, but `write_state_to_path` (invoked by `dg utils refresh-defs-state`, a
defs-load-time cache-building step, exactly analogous to `DbtProjectComponent` caching the
compiled manifest) was doing the actual generation *and* writing the final merged YAML
straight to `output_dir` on disk -- real "delivery" work happening during what's supposed to
be a cheap cache-refresh phase. Meanwhile `_cube_assets`'s materialization body did nothing but
call `promote_cube_files` (a no-op by default) and yield a `MaterializeResult` -- so
materializing a cube asset in Dagster never actually produced anything; whatever was on disk
was whatever the last `refresh-defs-state` happened to write, possibly on a completely
different machine (a code server, CI) that never ran a Dagster run at all.

Pushed further: even after moving the write to materialization time, is a *required*,
user-configured `output_dir` still justified, or should staging just be an ephemeral temp
dir? Investigated concretely rather than assumed either way -- `dagster-cube-dbt-tests`'
`defs.yaml` was the existing "proof" that a fixed on-disk `output_dir`, read directly by a
locally-running Cube instance with no `promote_cube_files` override, was a real supported
path. But the user's counter-argument holds up against actual Dagster deployment topology:
Dagster Cloud Serverless/Hybrid and most Docker/Kubernetes run-launcher setups give each run
its own throwaway filesystem -- there is no real production topology where a fixed on-disk
path is reachable by both an ephemeral Dagster run container and a separately-running Cube
instance, short of deliberately wiring a shared volume into both pods (a real but unusual,
infra-level setup, not something to default component behavior around). The "direct
shared-volume, no promotion needed" story only actually holds for local dev, where the Dagster
process and the Cube process are the same machine.

### Decisions

- **Generation moved entirely into `write_state_to_path`, but the *cached* artifact is now the
  merged cube/view data itself (JSON), not real per-entity YAML files.** `write_state_to_path`
  writes `json.dumps(merged)` to a file in the component's per-key state directory;
  `build_defs_from_state` reads it back to build `AssetSpec`s (metadata, `code_version`, deps)
  -- no filesystem I/O beyond the state cache happens at defs-load time any more.
- **`state_path` (the argument to `write_state_to_path`/`build_defs_from_state`) is a sentinel
  *file* `DbtProjectManager.prepare` touches, not a directory** -- confirmed by reading
  `dagster_dbt`'s `DbtProjectManager.prepare()` source directly (`_local_project_dir` returns
  `state_path.parent / "project"`; `state_path.touch()` at the end). Our own cached JSON goes
  in `state_path.parent / CUBE_STATE_FILENAME` (a distinct filename, no collision with
  dagster_dbt's own `project/` subdirectory) -- assuming `state_path` itself was a directory
  produced the exact WinError 183 (`mkdir(exist_ok=True)` still raises when the existing path
  is a file, not a directory) that caught this the first time round.
- **Real file writing moved into `_cube_assets`'s execution body**, staged in a
  `tempfile.TemporaryDirectory()` per materialization, then handed to `promote_cube_files(context,
  cubes_dir, views_dir)` before any `MaterializeResult` is yielded. The *full* generated
  cube/view set is always staged and promoted, regardless of `context.selected_asset_keys` --
  simplest safe choice given `can_subset=True`; avoids partial/inconsistent output at the
  promotion target across repeated subset materializations, and staging is cheap (just
  re-serializing already-cached data, not recomputing anything).
- **`promote_cube_files`'s default implementation now raises `NotImplementedError`** instead of
  being a silent no-op -- there is no default that's safe in a real deployment (see Problem
  above), so a component author who hasn't thought about delivery should get a clear failure
  the first time a cube asset materializes, not silent nothing.
- **`output_dir`/`views_output_dir` moved off `CubeDbtProjectComponent` entirely, onto a new
  subclass `LocalFileCubeDbtProjectComponent`**, which implements `promote_cube_files` by
  reading the staged temp dirs back (`read_entities`) and rewriting them to `output_dir`/
  `views_output_dir` via the existing `write_entities` (reused as-is, including its
  stale-file-cleanup behavior). Documented explicitly as local-dev/testing-only, not a
  production pattern -- consistent with the Problem investigation above. Added in the same
  `components/cube_dbt_project/component.py` file as a small subclass rather than a new
  subpackage: it's tightly coupled to the base class and short enough that a whole new
  subpackage (mirroring `dagster_dbt`'s one-component-per-directory convention) would be
  premature structure for ~15 lines.
- **`_patch_discovery_exclude()` hook** added to `CubeDbtProjectComponent` (default `[]`,
  overridden by `LocalFileCubeDbtProjectComponent` to return `[output_dir,
  _resolved_views_output_dir]`). Needed because `discover_patch_files` recursively globs
  *every* `*.yml`/`*.yaml` under the component's defs directory -- if a
  `LocalFileCubeDbtProjectComponent` user points `output_dir` somewhere inside that tree, the
  *previous* materialization's promoted output would otherwise get re-ingested as a patch on
  the *next* state refresh, corrupting the merge. Only relevant to the local-file subclass now,
  since the base class no longer writes anywhere inside the defs tree during state refresh at
  all (which also let the old hardcoded `output_dir` exclude in `write_state_to_path` be
  dropped for the base case).
- **`dagster-cube-dbt-tests`' `defs.yaml`** switched from `CubeDbtProjectComponent` to
  `LocalFileCubeDbtProjectComponent` -- it's exactly the local-dev use case the new subclass
  exists for (a `cube dev` process reading `cube_project/model/{cubes,views}` directly off the
  same checkout).

### Verification

- Full library suite (`uv run pytest tests/`, `hatch run test.core:run`, `hatch run
  test.fusion:run`) -- **46/46 passed** on all three, not just the plain `uv` path.
- Real end-to-end check against `dagster-cube-dbt-tests` (not just the library's own
  `NoopDbtProjectManager`-bypassed tests): cleared the gitignored `cube_project/model/{cubes,
  views}` output, ran `dg check yaml` (passes against the new component type), then `dg utils
  refresh-defs-state` -- confirmed `cube_project/` stayed *empty* afterward (proving generation
  no longer writes real files at state-refresh time). Then loaded the real
  `dagster_cube_dbt_tests.definitions.defs` (the actual Resolver/YAML-driven path, not the
  test bypass) and materialized all 6 cube/view assets in-process -- succeeded, and
  `cube_project/model/{cubes,views}` were populated afterward with exactly the expected 5
  cube + 1 view files, confirming `promote_cube_files` is what does the writing now, and only
  at materialization time.

### Assumptions

- No external consumers of this library exist yet (still being co-designed with its one real
  user), so the `output_dir` removal / `promote_cube_files` signature change (`context` ->
  `context, cubes_dir, views_dir`) was made as a breaking change with no deprecation path,
  rather than keeping a compatibility shim.
- `promote_cube_files` always receives the *complete* generated cube/view set, never a
  subset matching `context.selected_asset_keys`. Revisit if partial-materialization promotion
  (writing/pushing only the selected assets' files) turns out to matter in practice -- not
  implemented since it would require either filtering `cubes`/`views` before staging (extra
  complexity) or leaving the promotion target briefly inconsistent between a partial run and
  the next full one, and nothing so far has needed it.

## Phase 13 — promotion moved from a component override to a resource

### Problem

User pushback, again correct: overriding the component (Phase 12's `promote_cube_files`) is
the wrong extension point for delivery logic. Subclassing a component is the established
pattern for asset-*shape* customization (`get_cube_asset_spec`/`get_view_asset_spec`,
mirroring `DbtProjectComponent.get_asset_spec()`), not for runtime dependencies like
credentials and destination config -- which is exactly what Dagster resources are for, and
what most users already reach for when wiring up an S3 client, a git remote, etc. Bundling
promotion into a component subclass also meant every distinct promotion strategy needed its
own component *type*, which doesn't compose with the rest of the Components model.

### Decisions

- **New `CubeFilePromoter(dg.ConfigurableResource, ABC)`** (`resources.py`), with one
  abstract method: `promote(context, cubes_dir, views_dir) -> None`. Confirmed empirically
  that `ConfigurableResource` (Pydantic-based) combines cleanly with `ABC`/`@abstractmethod`
  before committing to the design -- instantiating the bare base class raises `TypeError`,
  concrete subclasses work normally.
- **`CubeDbtProjectComponent` gained `promoter_resource_key: str = "cube_file_promoter"`** (a
  plain resolvable string field) instead of a fixed hardcoded key, so a project with more than
  one `CubeDbtProjectComponent` instance can bind more than one promoter. The `_cube_assets`
  multi_asset declares `required_resource_keys={self.promoter_resource_key}` and fetches it
  via `getattr(context.resources, self.promoter_resource_key)` inside the op body, since the
  concrete resource type isn't known statically -- confirmed this dynamic-key pattern works
  end-to-end (including when the resource satisfying the key is registered in a *different*
  `Definitions` object than the one containing the asset that requires it, which is the whole
  point: `Definitions.merge()` unions resources by key across the project, so the promoter
  doesn't need to be declared anywhere near the component).
- **`promote_cube_files` and `LocalFileCubeDbtProjectComponent` removed entirely.**
  `LocalFileCubeFilePromoter(CubeFilePromoter)` replaces the old subclass -- same behavior
  (write straight to `output_dir`/`views_output_dir` via `write_entities`/`read_entities`),
  now a resource instead of a component. This actually simplifies the component surface back
  down to one class instead of two.
- **`output_dir`/`views_output_dir` on `LocalFileCubeFilePromoter` are typed `str`, not
  `Path`.** `ConfigurableResource` fields must resolve to a Dagster config type via
  `_convert_pydantic_field`, and `pathlib.Path` isn't one -- confirmed by hitting
  `DagsterInvalidConfigDefinitionError: <class 'pathlib.Path'> cannot be resolved` when this
  was first tried with `Path`-typed fields, unlike a plain `Resolvable` component attribute
  (which uses a completely different resolution path and accepts `Path` fine, as
  `CubeDbtProjectComponent`'s own fields already did in Phase 12). Converted to `Path`
  internally inside `promote()` instead.
- **`_patch_discovery_exclude()` (Phase 12's hook for `LocalFileCubeDbtProjectComponent` to
  protect its own output from being re-ingested as a patch) is gone, with no replacement.**
  The component genuinely cannot know what a bound resource writes or where -- the resource
  is opaque to it by design, often defined in a completely different file. Documented as a
  caveat instead (README: keep promoter output outside the component's own defs directory)
  rather than solved in code.
- **A real, unrelated bug found while doing the required end-to-end verification of this
  redesign**: materializing all cube *and* view assets together against the actual
  `dagster-cube-dbt-tests` project intermittently raised `DagsterInvariantViolationError:
  Asset "cube_view/journeys_overview" was yielded before its dependency "cube/..."`.
  `_cube_assets` was iterating `context.selected_asset_keys` (an unordered set) to decide
  `MaterializeResult` yield order, and Dagster requires multi-assets to yield in topological
  order -- this bug predates this phase (the loop was unchanged from Phase 12) and had gone
  unnoticed because prior verification runs happened to get lucky on `AssetKey` hash-seed
  ordering. **Fixed** by iterating `specs` (cubes always listed before views, and views never
  depend on other views, so that order is always a valid topological order) instead, filtering
  down to what's selected. Added a regression test that materializes every cube and view
  together and asserts the view's materialization event comes after both its dependency
  cubes' events, rather than relying on getting lucky against the real project again.

### Verification

- Full library suite (`uv run pytest tests/`, `hatch run test.core:run`, `hatch run
  test.fusion:run`) -- **48/48 passed** on all three (up from 46 -- one new resource-key test,
  one new topological-order regression test).
- Real end-to-end check against `dagster-cube-dbt-tests`: switched `dbt_cubes/defs.yaml` back
  to plain `CubeDbtProjectComponent` (no `output_dir` attributes), added
  `defs/cube_promoter.py` (a plain `@dg.definitions`-decorated Python module, not a YAML
  component) binding `LocalFileCubeFilePromoter` under `cube_file_promoter` -- confirmed a
  plain Python defs module alongside YAML component dirs is auto-discovered and its resources
  correctly satisfy a *different* component's required resource key. `dg check yaml` passed,
  `dg utils refresh-defs-state` left `cube_project/` empty (state refresh still only caches),
  and materializing all 6 cube/view assets through the real `dagster_cube_dbt_tests.definitions.defs`
  wrote exactly the expected 5 cube + 1 view files -- this run is what caught the topological-
  order bug above (the very first attempt, before the fix, hit it).

### Assumptions

- `ConfigurableResource` instances are recreated (not reused by identity) when a run
  initializes its resources -- confirmed empirically (`id()` differs between the object passed
  to `Definitions(resources=...)` and the one seen inside the executing op) before relying on
  it. This is why the test double for "was promote() called correctly" (`_recording_promoter`)
  records into a plain list captured by closure rather than an instance/`PrivateAttr` field --
  the latter looked correct locally but silently recorded onto an instance nothing ever reads
  back from.
- `resolve_asset_graph()` / `resolve_implicit_global_asset_job_def()` eagerly validate that
  *every* required resource key across the whole `Definitions` is satisfiable, even for code
  paths that never execute anything (building the asset graph for display, evaluating
  automation conditions). Confirmed empirically, not assumed -- this is why spec-only and
  automation-condition tests now bind a no-op `CubeFilePromoter` too, not just the tests that
  actually materialize.

## Phase 14 — a model with zero declared columns silently generated an empty cube

### Problem

User report: a `cube_select`-matched dbt model with no `columns:` block declared in
`schema.yml` at all didn't raise `MissingDimensionTypeError` on `dg utils
refresh-defs-state`, contrary to expectation. Root-caused by reading `generate_cubes`
directly (`generation.py`): the missing-`data_type` check only iterates `model.columns`,
which `cube_dbt.Model._init_columns` populates straight from the manifest's `columns` dict --
itself populated only from what's declared in `schema.yml` (dbt doesn't back-fill a model's
actual output columns into `manifest.json` at parse time). With zero declared columns, the
per-column loop body never runs even once, so `missing_data_types` stays empty and a cube
with `dimensions: []` is silently produced -- not because anything was validated as correct,
but because there was nothing to iterate over in the first place.

User's proposed fix, after confirming the above: require every `cube_select`-matched model
to have dbt contract enforcement turned on (`config: {contract: {enforced: true}}`), rather
than just checking "at least one column exists." This is strictly stronger and better-
motivated than the narrower fix originally suggested (just check for zero columns): a
contract also guards against *future* silent drift -- a new column added to a model's SQL
later, with schema.yml never updated, wouldn't be caught by a manifest-only check at any
point, but dbt's own contract validation fails the *build* if the real output doesn't
exactly match what's declared.

### Decisions

- **New `UnenforcedContractError`** (`generation.py`), raised when any `cube_select`-matched
  model lacks `contract.enforced` in the manifest, collected across the whole run (same
  one-error-lists-everything philosophy as `MissingDimensionTypeError`). Checked *before* the
  per-column `data_type` check, with `continue` to skip straight past an uncontracted
  model's columns entirely -- confirmed the two checks are mutually exclusive in practice
  (dbt itself refuses to parse a contract-enforced model unless every declared column already
  has a `data_type`, so an uncontracted model is the only way to reach the old failure mode).
- **`_contract_enforced(model)` reaches into `cube_dbt.Model`'s private `_model_dict`** (same
  established pattern as `_raw_data_type` reaching into `Column._column_dict` -- `cube_dbt`
  has no public accessor for contract status either). Pinned by
  `test_generation.py::test_contract_enforced_shape_is_pinned`.
- **Verified the manifest field name/shape directly** rather than assumed from memory: dbt's
  contract state is `node.contract.enforced` (a nested dict, also mirrored under
  `node.config.contract` but the top-level field is the canonical one), confirmed by
  inspecting the fixture project's actual `manifest.json` both before and after enabling a
  contract on a model. **Confirmed dbt-core and dbt Fusion produce the same shape** (Fusion's
  `contract` dict omits the `checksum` key dbt-core includes, but the `enforced` key this
  library actually reads is identical) -- checked directly against both engines' manifests,
  not assumed to carry over from the dbt-core check alone, consistent with how every other
  manifest-shape assumption in this project has been verified per-engine (see Phase 11).
- **Fixture updated**: all four `marts` models (library's own `tests/fixtures/dbt_project`,
  and separately the `dagster-cube-dbt-tests` example project's identical fixture) now
  declare `config: {contract: {enforced: true}}`. `journey_samples`' `internal_row_hash`
  column (excluded from becoming a dimension via `meta.cube.dimension: false`) needed a
  `data_type` added purely to satisfy dbt's own contract validation, which has no notion of
  that exclusion flag -- a real, documented interaction between the two features, not a bug.
  Real `dbt build` against both fixtures was verified to still pass with contracts enabled
  (contract validation includes a runtime check against the actual built table, not just a
  parse-time one).
- **`test_generation.py`'s synthetic `_model()` helper defaults `contract_enforced=True`**,
  so none of the many existing tests using it needed individual changes -- only the new
  tests explicitly pass `contract_enforced=False` to exercise the failure path.

### Verification

- Full library suite (`uv run pytest tests/`, `hatch run test.core:run`, `hatch run
  test.fusion:run`) -- **52/52 passed** on all three (up from 48 -- four new tests: the
  UnenforcedContractError cases, the zero-columns regression test, and the shape-pinning
  test).
- Real end-to-end re-verification against `dagster-cube-dbt-tests` after enabling contracts
  in its fixture: `dg check yaml`, `dg utils refresh-defs-state` (cache-only, as established
  in Phase 12), materializing all 6 cube/view assets through the real
  `dagster_cube_dbt_tests.definitions.defs`, and a direct `dbt build` against the real
  project (not just through Dagster) -- all passed, confirming contract enforcement doesn't
  just parse cleanly but the models' actual DuckDB output genuinely satisfies their
  contracts.

### Assumptions

- Contract enforcement is required for every `cube_select`-matched model with no opt-out --
  there's no equivalent of `meta.cube.dimension: false` for skipping the contract
  requirement at the model level; narrowing `cube_select` is the only way to exclude a model
  that can't yet be contracted. Revisit if this proves too strict in practice (e.g. an
  incremental-migration path where some models are contracted and others aren't yet), but the
  user's own framing ("every model having a cube built for it should be expected to have all
  columns declared") was for a hard, universal constraint, not a soft default.

## Phase 15 — cube_dbt doesn't recognize ClickHouse (or other non-mainstream) data types

### Problem

User report, with a real traceback: `RuntimeError: Unknown column type of dates.date_day:
Date32`, raised from inside `cube_dbt.Column.type`, not this library's own code. Root-caused
by reading `cube_dbt`'s source directly: `Column.type` normalizes a column's dbt `data_type`
(lowercased, with any parenthesized/angle-bracket content stripped) and looks it up in a
fixed `TYPE_MAPPINGS` dict built around common warehouse type names (Snowflake, BigQuery,
Postgres/Redshift, generic SQL) -- ClickHouse-specific names like `Date32`, `DateTime64(...)`,
`UInt32` aren't in it, so the lookup falls through and `RuntimeError`s.

Investigated whether extending `TYPE_MAPPINGS` (even just locally, e.g. monkeypatching it at
import time) could fix this properly, rather than assuming a per-column override was the only
option. Found a real, structural reason it can't, at least not for ClickHouse's `Nullable(T)`
wrapper specifically: `cube_dbt`'s normalization strips parenthesized content *before*
inspecting the type name, so `Nullable(String)` and `Nullable(Int32)` both collapse to the
same bare `nullable` -- there's no way for a fixed mapping table (upstream or monkeypatched)
to map that string back to the right Cube type in general, since the actual wrapped type is
already gone by the time the mapping table is consulted. This ruled out "just extend the
mapping table" as a real fix and confirmed a per-column escape hatch is the right shape for
this, not a stopgap.

Also considered (and rejected): silently falling back to `"string"` on an unrecognized type,
the way `cube_dbt` itself defaults to `"string"` for an absent `data_type`. Rejected because
it directly contradicts this library's own established philosophy just next to it in the
same function (`MissingDimensionTypeError`'s whole reason for existing is refusing to
silently mistype a column as `string`) -- a `Date32` column silently typed as `string` loses
real Cube capabilities (time-granularity queries) with no error pointing at why.

### Decisions

- **New `meta.cube.type` override key**, consumed by a new `_resolve_dimension_type(column)`
  helper: returns the override if set, otherwise calls `cube_dbt`'s `column.type`, catching
  `RuntimeError` and returning `None` (rather than letting it propagate) if `cube_dbt`
  doesn't recognize the type and no override was given. `_build_dimension` now takes the
  already-resolved `dimension_type` as a parameter instead of calling `column.type` itself,
  so the resolution/error-collection step lives in `generate_cubes`'s loop alongside the
  existing `missing_data_types` collection, not buried inside dimension construction.
- **New `UnrecognizedColumnTypeError`**, collecting every column `cube_dbt` couldn't type
  across a run (same one-error-lists-everything philosophy as the other two generation
  errors), checked last (after the contract and missing-`data_type` checks, which are logical
  prerequisites -- an uncontracted or type-undeclared column never reaches this check at all).
  Error message includes the raw offending `data_type` string per column (e.g.
  `dates.date_day (Date32)`) so the user doesn't have to go looking for it.
- **`type` added to the `meta.cube` reserved-key set** but deliberately *not* added to
  `PROMOTED_CUBE_META_KEYS` (`order`/`mask`/`public`) -- those are simple post-hoc dict
  overwrites applied after `dimension["type"]` is already set; `type` needs to be consulted
  *before* `column.type` is even called, since calling it is what raises. Handled as its own
  explicit step instead of forcing it through the same generic loop.
- **No changes to any fixture** -- neither the library's own nor `dagster-cube-dbt-tests`'
  use any warehouse-specific types the existing `TYPE_MAPPINGS` doesn't already cover, so
  there was nothing to reproduce this against in the real fixtures. Covered entirely by new
  synthetic-manifest unit tests in `test_generation.py` (matching how `test_generation.py`
  already tests other generation-layer behavior without needing a real dbt project).

### Verification

- Reproduced the exact normalization `cube_dbt` performs against several real ClickHouse type
  strings (`Date32`, `DateTime64(3)`, `UInt32`, `Nullable(String)`, `Bool`, `Decimal(10,2)`,
  `Array(String)`) directly against the installed `cube_dbt` package's own regex and
  `TYPE_MAPPINGS`/`VALID_DIMENSION_TYPES`, rather than assumed from reading `Column.type`'s
  source alone -- confirmed which specific ones fail and, critically, confirmed the
  `Nullable(...)` collapse-to-bare-`nullable` behavior that ruled out the mapping-table-
  extension approach.
- Full library suite (`uv run pytest tests/`, `hatch run test.core:run`, `hatch run
  test.fusion:run`) -- **55/55 passed** on all three (up from 52 -- three new tests: the
  unrecognized-type error itself, the override bypassing it, and the override taking priority
  over an already-recognized type too).

### Assumptions

- `cube_dbt.Column.type` only ever raises plain `RuntimeError` for this specific failure mode
  (unrecognized type) -- confirmed by reading its source directly (a single `raise
  RuntimeError(...)` at the end of the property, no other exception paths), so catching
  `RuntimeError` specifically (not a bare `except:`) inside `_resolve_dimension_type` doesn't
  risk masking an unrelated error from elsewhere in that call.

## Phase 16 — vendoring `cube_dbt` instead of depending on it

### Problem

User question, prompted directly by the accumulated workarounds across Phases 14-15: given
how much of `cube_dbt`'s behavior this library was already working around, was it still the
right dependency to keep? Investigated properly rather than answering from impression.

**What `cube_dbt` actually is**: officially authored by Cube.dev's own team
(`artyom@cube.dev`, `igor@cube.dev`, `paco@cube.dev`), but small -- 540 lines across 4 files
(`dbt.py`, `model.py`, `column.py`, `dump.py`). Its PyPI release history (fetched directly,
not assumed): dense daily releases in Sept-Oct 2023 while first built, then 0.6.0 -> 0.6.1
took 14 months, 0.6.1 -> 0.6.2 five months, 0.6.2 -> 0.6.3 (current as of writing) another
five -- minimal-maintenance mode, not active development.

**What this library actually still used from it**, read end to end: `Dbt.filter()` +
model/column iteration (~15 lines of real logic), primary-key detection (constraint-based,
falling back to unique+not_null tests, ~25 lines -- the one genuinely non-trivial piece),
and thin property passthroughs (`name`, `description`, `sql_table` -- which has a
`relation_name`-vs-manually-built fallback worth preserving faithfully -- `sql`, `meta`).
Everything else was already being bypassed: `as_cube()`/`as_dimensions()` (Phase 1, never
used), contract-awareness (Phase 14, doesn't exist in `cube_dbt` at all, read via
`model._model_dict` directly), and now type inference for non-mainstream dialects (Phase 15,
worked around via `meta.cube.type` after `cube_dbt`'s own `RuntimeError`). Two of this
library's own private-attribute reaches (`_column_dict`, `_model_dict`) existed specifically
because `cube_dbt`'s *public* API didn't cover what generation.py needs.

**Checked the user's specific hypothesis** (Cube working on a newer, possibly closed-source
dbt integration that could mean `cube_dbt` gets sunset) via web search rather than
speculating: confirmed. Cube has a newer "dbt Integration" feature (`Settings -> Data
Sources`, `Integrations -> dbt -> Pull`, review-branch governance workflow) that reads as a
Cube Cloud product feature, not part of open-source Cube Core, and its own blog
announcement makes zero mention of the `cube_dbt` package -- no explicit deprecation notice,
but no acknowledgment it exists either. That integration's supported warehouse list
(Snowflake, Redshift, Postgres, BigQuery) doesn't include ClickHouse regardless, so it
wouldn't have solved the Phase 15 problem even if it were the same thing. Taken together:
Cube's own product investment in dbt integration appears to be going into a separate,
commercial, Cube-Cloud-native feature, not the open-source package this library depends on --
consistent with, and reinforcing, the sporadic release cadence already observed.

### Decisions

- **New `manifest.py`**: vendors the specific subset of `cube_dbt` behavior this library
  actually uses, operating on plain manifest dicts throughout (no wrapper classes -- the
  previous `Model`/`Column` wrappers added no behavior beyond property access this library
  was already unwrapping immediately). Ported faithfully: `filter_models` (path prefix /
  tag / exact-name filtering, non-ephemeral only), `build_test_index` +
  `model_primary_key_names` (constraint-based, falling back to tag/unique+not_null-test
  detection -- the exact same priority order and test-index-building logic as `cube_dbt`'s
  `Dbt._build_test_index`/`Model._detect_primary_key`), `model_sql_table` (`relation_name`
  fallback preserved), `column_sql`, and `TYPE_MAPPINGS`/`VALID_DIMENSION_TYPES` (the
  BigQuery+Redshift+Snowflake union, copied verbatim from `cube_dbt`'s source, confirmed via
  a direct diff against the installed package's own dict before removing the dependency).
- **`infer_dimension_type` returns `None` instead of raising**, unlike `cube_dbt`'s
  `Column.type` -- this actually *simplifies* `generation.py`'s `_resolve_dimension_type`
  (Phase 15's `try/except RuntimeError` around a third-party call is no longer needed; a
  plain `None` check suffices), since there's no external exception type to guard against
  once this library owns the whole call chain.
- **`generate_cubes` now takes the raw manifest dict directly**, not a `Dbt` wrapper --
  `component.py` no longer constructs a `Dbt(manifest)` at all, just passes
  `validate_manifest(...)`'s own return value straight through.
- **The two "private attribute shape is pinned" regression tests
  (`test_raw_data_type_shape_is_pinned`, `test_contract_enforced_shape_is_pinned`) were
  deleted, not adapted.** They existed specifically to catch an upstream `cube_dbt` release
  renaming its private attributes out from under this library -- with no more external
  private API being reached into, there's nothing left for them to guard.
- **`cube_dbt` removed from `pyproject.toml`** in both the main `dependencies` list and the
  `[tool.hatch.envs.test]` matrix's dependency list (both legs, core and fusion, needed the
  same removal since both previously installed it alongside their respective dbt engine).

### Verification

- Full library suite (`uv run pytest tests/`, `hatch run test.core:run`, `hatch run
  test.fusion:run`) -- **53/53 passed** on all three (down from 55: two pinning tests
  deleted, not replaced, since they no longer apply) -- confirmed `uv sync`
  uninstalled `cube_dbt` from the venv (`uv pip list` showed no match) after the
  `pyproject.toml` change, and confirmed the same for both hatch matrix environments after
  an explicit `hatch env prune` + rebuild (hatch does not automatically uninstall a removed
  dependency from an existing environment, only add new ones, so this needed forcing to
  actually prove the removal rather than just prove nothing currently imports the leftover
  package).
- Real end-to-end re-verification against `dagster-cube-dbt-tests` (`uv sync` there too, to
  drop the transitive `cube-dbt==0.6.3` install, confirmed via lockfile diff): `dg check
  yaml`, `dg utils refresh-defs-state`, and materializing all 6 cube/view assets through the
  real `dagster_cube_dbt_tests.definitions.defs` -- all passed with `cube_dbt` completely
  absent from the environment. Manually inspected the generated `journey_samples.yaml`
  output against what earlier phases' verification runs had produced (patch-removed
  `journey_type` dimension gone, `internal_row_hash` excluded, primary-key/number typing on
  `journey_sample_key`, measures/joins from patches intact) -- byte-for-byte consistent with
  the `cube_dbt`-backed behavior it replaces.

### Assumptions

- `manifest.py`'s `TYPE_MAPPINGS` is a frozen copy of `cube_dbt` 0.6.3's own mapping table,
  not a live dependency -- future warehouse-type gaps (the same class of problem Phase 15
  fixed for ClickHouse) now get fixed directly in this library rather than inherited from an
  upstream release, which was the explicit point of this phase, not an oversight to revisit.
- Dropping `cube_dbt` doesn't reduce alignment with Cube's own semantics for anything this
  library actually still relies on (dimension type vocabulary, primary-key conventions) --
  those are stable, publicly documented Cube concepts (linked from this library's own docs),
  not `cube_dbt`-internal behavior that could drift independently of them.

## Phase 17 — ClickHouse types added natively, not just via meta.cube.type

### Problem

Real-world confirmation, immediately after Phase 16 landed: a user's actual ClickHouse dbt
project hit `UnrecognizedColumnTypeError` for **28 columns across two models** (a date
dimension and a time dimension) -- all either `Date32` or an explicitly-sized `UIntN`.
`meta.cube.type` (Phase 15's fix) technically resolves this, but needing a manual override on
every single column of an entire dimension table isn't a real per-column escape hatch
anymore, it's a missing warehouse dialect -- exactly the class of gap Phase 16's vendoring
was meant to make cheap to close directly, rather than waiting on an upstream release (or,
per Phase 16's research, an upstream release that may never come).

### Decisions

- **Added ClickHouse entries directly to `manifest.TYPE_MAPPINGS`**: `UInt8`/`16`/`32`/`64`/
  `128`/`256`, `Int128`/`256` (the smaller `Int8`/`16`/`32`/`64` already had equivalents from
  the Redshift/BigQuery mappings under different aliases), `Float32`, `Date32`,
  `DateTime64`, `UUID`, `FixedString`, `Enum8`/`Enum16`, `IPv4`/`IPv6` -- covering every
  column in the real reported error, plus a few more common ClickHouse types for completeness.
- **`Nullable(T)`/`LowCardinality(T)` are now unwrapped recursively to whatever `T` is**,
  rather than falling through the existing blind paren-stripping regex (which would collapse
  `Nullable(String)` and `Nullable(Int32)` to the same bare `nullable`, the exact problem
  Phase 15 identified as unfixable via a bigger mapping table alone). This wasn't attempted
  in Phase 15 because at the time `cube_dbt` still owned this code and extending its behavior
  meant either forking it or working around it per-column; owning `manifest.py` directly
  (Phase 16) is what made a real fix for this actually low-cost, not just theoretically
  possible. Implemented as a small regex matching a wrapping `nullable(...)`/
  `lowcardinality(...)` and recursing into the captured inner type -- confirmed this
  correctly resolves nested combinations like `LowCardinality(Nullable(String))` too, not
  just a single wrapping layer.
- **`meta.cube.type` stays documented and available**, now scoped to genuinely-unrecognized
  types rather than being the primary ClickHouse story -- the README's example switched from
  a real ClickHouse type (now natively handled) to an explicitly fictional one, so the
  documented escape hatch doesn't silently go stale/misleading now that the type it used to
  illustrate is recognized on its own.

### Verification

- Directly ran `infer_dimension_type` against every type string from the real reported error
  (`Date32`, `UInt8`, `UInt16`) plus `UInt32`/`UInt64`, `DateTime64(3)`, several
  `Nullable(...)`/`LowCardinality(...)` combinations (including the nested case), `Bool`,
  `UUID`, `FixedString(16)`, `Enum8(...)`, `IPv4` -- confirmed every one resolves to the
  correct Cube type before writing any test.
- Full library suite (`uv run pytest tests/`, `hatch run test.core:run`, `hatch run
  test.fusion:run`) -- **55/55 passed** on all three (up from 53 -- one existing test
  updated to use a genuinely-fictional unrecognized type instead of the now-recognized
  `Date32`/`DateTime64`, two new tests added: native ClickHouse recognition across 8
  representative types, and Nullable/LowCardinality unwrapping including the nested case).

### Assumptions

- The `Nullable(...)`/`LowCardinality(...)` unwrap regex assumes a single, simple wrapping
  pattern (`word(...)`) rather than needing genuine balanced-paren parsing -- confirmed
  sufficient for every case that matters here (ClickHouse only ever uses these two as
  wrappers around dbt-declared `data_type` strings), not assumed to handle arbitrary
  parenthesized expressions in general.

## Phase 18 — `extends`-resolved fields available to `get_cube_asset_spec`

### Problem

User request: a cube using Cube's own `extends` (typically introduced via a merge patch, to
reuse another cube's fields) should have the parent cube's fields -- `description`, `meta`,
anything else `get_cube_asset_spec` reads -- available when building its `AssetSpec`, not
just whatever the cube's own (possibly sparse) patch fragment declares. The generated/
promoted YAML must keep `extends:` as a literal field regardless -- Cube resolves it itself
at its own runtime, and this library has never touched that.

Verified Cube's actual `extends` semantics directly against its docs (fetched, not assumed)
before implementing: reuses all declared members of the parent (list-like fields like
`measures`/`dimensions` are unioned by name, the child's own entries added alongside/
overriding by name); scalar fields (`sql_table`, `description`, etc.) the child doesn't
redeclare are inherited from the parent. This is the *exact same shape* of merge this
library's own `$mergeStrategy`-based patch application already implements (name-keyed list
merge, deep dict merge, child/patch wins on conflicts) -- so resolving `extends` reuses
`StrategicMerger` directly rather than needing new merge logic.

### Decisions

- **New `merge.resolve_extends(entities)`**: returns every cube's fully resolved fields
  (parent's fields with the cube's own -- `extends` aside -- folded on top), following
  multi-level `extends` chains recursively, keyed by name (including cubes with no `extends`
  at all, returned as-is, for uniform lookup). Purely read-only/side-channel: never mutates
  its input, and its result is never what gets written to disk or handed to a promoter --
  only what `get_cube_asset_spec` receives.
- **`component.py`'s `build_defs_from_state`** computes `resolve_extends(cubes)` once and
  passes each cube's *resolved* dict into `get_cube_asset_spec` -- not a new parameter, the
  existing `cube` argument itself, since that's the simplest way to make parent fields
  "easily available" (exactly as asked) without changing the method's signature at all.
  `write_entities`/the promoter still operate on the original, unresolved `cubes` list
  (captured separately via closure in `_cube_assets`), so the real output is unaffected.
- **An `extends` target not found among the component's own generated/patched cubes is left
  unresolved**, not an error -- `extends` can legitimately point at a hand-authored cube
  living entirely outside this pipeline's visibility (a different part of the same Cube
  project), which Cube itself will still resolve correctly at its own runtime regardless of
  what this library could see.
- **New `CircularExtendsError`** if an `extends` chain cycles back on itself -- consistent
  with this codebase's established stance (contract enforcement, missing/unrecognized
  column types) of raising a clear error over producing something silently wrong, rather than
  e.g. silently truncating the cycle.
- **Deep-copies both sides before every merge step**, confirmed necessary by reading
  `deepmerge`'s own dict-merge strategy source directly: it mutates and returns `base` in
  place. Without copying, resolving a multi-level chain (or multiple cubes extending the same
  parent) would risk corrupting an already-cached resolved parent, or worse, the original
  `cubes` list itself -- which absolutely must stay untouched, since that exact list is what
  gets written to the real Cube YAML output afterward.
- **A cube's `code_version` now changes when an ancestor's fields change too**, not just its
  own -- an accepted, correct side effect of resolution rather than something to work around,
  since the cube's *effective* definition did change either way, and
  `GENERATED_ASSET_AUTOMATION_CONDITION` should still re-run it once.

### Verification

- `resolve_extends` checked directly against Cube's own documented `extends` example
  (`order_facts`/`extended_order_facts`) before writing any test -- reproduced their exact
  expected result (`sql_table` inherited, `count` measure reused, `double_count` added).
  Also verified a multi-level chain, a cube extending something not in the local cube set
  (left unresolved, not an error), a genuine cycle (raises), and that the original input list
  is provably untouched after resolution.
- Full library suite (`uv run pytest tests/`, `hatch run test.core:run`, `hatch run
  test.fusion:run`) -- **62/62 passed** on all three (up from 55 -- 7 new `resolve_extends`
  unit tests in `test_merge.py`, plus one integration test proving `get_cube_asset_spec` sees
  resolved fields end-to-end through the real component while the cached/promoted document
  keeps `extends:` literal).
- Real end-to-end re-verification against `dagster-cube-dbt-tests` (`dg check yaml`, `dg
  utils refresh-defs-state`, materializing all 6 cube/view assets through the real
  `dagster_cube_dbt_tests.definitions.defs`) -- unaffected, since that project doesn't use
  `extends` at all; confirms this change is additive and doesn't disturb the non-`extends`
  path.

### Assumptions

- Extends resolution is scoped to cubes only (matching the request, and Cube's own docs,
  which document `extends` under the cube reference, not the view one) -- `get_view_asset_spec`
  still receives the view's own unresolved fields. Revisit if views turn out to support
  `extends` too and someone actually needs it.

## Phase 19 — column schema metadata, not column lineage

### Problem

User request, framed as "column level lineage... key `dagster/column_lineage`... value of
type `TableColumnLineage`" -- but the actual described goal (show each asset's column name,
type, and description; explicitly *not* tracing which upstream dbt columns fed each one,
called out as overkill) doesn't match what that metadata key/type actually holds. Read
Dagster's own source directly rather than assume: `TableColumnLineage.deps_by_column` is
`Mapping[str, Sequence[TableColumnDep]]` -- purely a graph of column-to-column dependency
edges onto *specific upstream asset columns* (`TableColumnDep(asset_key, column_name)`). It
has no `type`/`description` fields anywhere; it structurally cannot express what was asked
for. What actually models "name, type, description" is `dagster/column_schema`
(`TableSchema`/`TableColumn`, the latter has exactly `name`/`type`/`description`/
`constraints`/`tags`) -- confirmed by reading that source too, and empirically verified
(`TableSchema`/`TableColumn` used directly as `AssetSpec` metadata, materialized in-process,
confirmed the metadata round-trips through `resolve_asset_graph()` correctly) before
committing to it.

Also raised directly with the user (not assumed): whether this should be static `AssetSpec`
metadata (visible even for a never-materialized cube asset, consistent with how every other
aspect of these virtual assets already works) or attached only via `MaterializeResult`
(closer to the literal "when the multi-asset is materialized, it returns metadata" framing
used to describe it). Chose static, per the user's own selection.

### Decisions

- **New `_column_schema(cube)`** (`component.py`): builds a `TableSchema` from the cube's
  `dimensions` and `measures`, one `TableColumn` each -- `name`, `type` (Cube's own dimension/
  measure type, e.g. `string`/`time`/`count`, not a warehouse SQL type), `description` where
  present, and a `dagster_cube_dbt/member_type: dimension|measure` tag so the two are
  distinguishable in the Dagster UI (measure `type` values like `count`/`sum` live in a
  different vocabulary than dimension types, so without a tag they'd look like an
  inconsistent type system rather than two different kinds of column).
- **Merged into `get_cube_asset_spec`'s existing `metadata=`** alongside the pre-existing
  `dagster_cube_dbt/yaml` metadata, not a new mechanism -- and built from the same
  extends-resolved `cube` dict `get_cube_asset_spec` already receives (Phase 18), so a cube
  extending another one gets the parent's dimensions/measures reflected in its own column
  schema too, for free.
- **Scoped to cubes only, not views** -- views don't declare their own `dimensions`/
  `measures` directly (those come from whichever cubes they `include`, resolved by Cube
  itself at query time), so there's no straightforward column list to build without also
  resolving view composition -- explicitly out of scope, matching the same reasoning that
  ruled out real column-level lineage.

### Verification

- Directly inspected `TableColumnLineage`/`TableColumn`/`TableSchema`'s actual source before
  writing any code, rather than relying on memory of the API shape.
- Full library suite (`uv run pytest tests/`, `hatch run test.core:run`, `hatch run
  test.fusion:run`) -- **63/63 passed** on all three (up from 62 -- one new test asserting
  the exact `TableColumn` set, types, descriptions, and member-type tags for a real
  dbt-derived-plus-patched cube, including that the `meta.cube.dimension: false`-excluded
  column is absent).
- Real end-to-end check against `dagster-cube-dbt-tests`: loaded the actual
  `dagster_cube_dbt_tests.definitions.defs`, inspected `journey_samples`' resolved
  `dagster/column_schema` metadata directly -- correct columns, types, and member-type tags,
  with `journey_type` correctly absent (removed by that project's own merge patch).

## Phase 20 — primary-key constraints on column schema metadata

### Problem

Follow-up question: was `TableColumn.constraints`/`TableSchema.constraints` (Phase 19 left
both at their defaults) being populated at all, with the user's own guess that primary-key
marking would be the one actually worth adding. Checked `TableColumnConstraints`/
`TableConstraints`'s real fields directly rather than assume: column-level constraints are
only `nullable`/`unique`/`other: list[str]` (no dedicated `primary_key` boolean); table-level
constraints are only `other: list[str]` (free text; "a constraint defined in terms of
multiple columns... cannot be expressed" any other way, per its own docstring). So there's no
first-class "primary key" field anywhere in this API -- it has to be represented via the
existing `unique`/`nullable`/`other` fields, and the interesting design question was doing
that *correctly* for a composite key, since this library already supports multi-column
primary keys (`Model.primary_key` returning more than one column, generation already setting
`dimension["primary_key"] = True` on each -- exactly mirroring how Cube's own schema
represents a composite key, one `primary_key: true` per member dimension).

### Decisions

- **A single-column primary key** gets column-level `nullable=False`, `unique=True`,
  `other=["primary key"]`.
- **A composite primary key's individual dimensions get `nullable=False` but *not*
  `unique=True`** -- no single column in a composite key is unique on its own, only the
  tuple of all of them together is; setting `unique=True` per-column would misrepresent that.
  Instead, the composite relationship is stated as a table-level constraint
  (`TableSchema.constraints`, e.g. `"primary key: (customer_id, order_date)"`), the only place
  this API can actually express a multi-column relationship at all.
- Whether a key is composite is decided by counting how many dimensions in the *same* cube
  have `primary_key: true` -- `> 1` triggers the composite path, matching how the constraint
  actually needs to be represented, not an arbitrary threshold.
- No table-level constraint is added for a single-column key -- the column-level `other:
  ["primary key"]` already says everything there is to say, so a redundant table-level
  statement was left out rather than added "for consistency."

### Verification

- Full library suite (`uv run pytest tests/`, `hatch run test.core:run`, `hatch run
  test.fusion:run`) -- **64/64 passed** on all three (up from 63 -- the existing column-
  schema test extended with primary-key assertions on `journey_samples`' single-column key,
  plus one new dedicated test for a synthetic composite-key cube introduced via a merge
  patch, checking both dimensions get `nullable=False`/no `unique`/`other: ["primary key"]`
  and the table-level constraint names both columns).
- Real end-to-end check against `dagster-cube-dbt-tests`'s actual `journey_samples` cube --
  `journey_sample_key` (single-column key, detected via unique+not_null tests, same as the
  library's own fixture) correctly shows `unique=True`, `nullable=False`,
  `other=["primary key"]`, every other column at defaults, table-level constraints empty.

## Phase 21 — bug: column-level `primary_key` constraints weren't detected

### Problem

User-reported real case: a `dates` model with `config: {contract: {enforced: true}}` and a
`date_key` column declaring its primary key inline, dbt-style --
`columns: [{name: date_key, constraints: [{type: primary_key}]}]` -- came out of generation
with no `primary_key: true` anywhere, so the Phase 20 constraint metadata never fired for it.

Root cause: `model_primary_key_names()` only ever read `model.get("constraints", [])`, i.e.
the **model-level** declaration style (`constraints: [{type: primary_key, columns: [...]}]`,
usually written for composite keys). dbt's manifest keeps model-level and column-level
constraints in genuinely separate fields -- a column-level `constraints:` entry never gets
rolled up into the model's own `constraints` list -- so a primary key declared per-column, as
in the user's `dates` model, was invisible to the existing check. This wasn't a fixture gap;
re-reading `model_primary_key_names`'s own logic against the user's real YAML was enough to
spot it directly, no reproduction needed.

### Fix

`model_primary_key_names()` now unions two constraint sources before falling back to the
tags/tests heuristic: the existing model-level `constraints` scan, plus a new scan over each
column's own `constraints` list for a `type: primary_key` entry. Model-level and column-level
declarations are additive (a project could in principle mix both, however unlikely), not
either/or.

### Verification

- New test `test_primary_key_detected_from_column_level_constraint` in `test_generation.py`,
  mirroring the user's actual `dates` model shape (`date_key` with an inline
  `constraints: [{type: primary_key}]`, alongside an ordinary `date_day` column) -- asserts
  `date_key` gets `primary_key: true` and `date_day` does not.
- Full suite: `uv run pytest tests/`, `hatch run test.core:run`, `hatch run test.fusion:run`
  -- **65/65 passed** on all three.
- README's primary-key detection paragraph updated to name both declaration styles
  explicitly, instead of implying only the model-level form is recognized.

## Phase 22 — correction: model-level primary key wins outright, doesn't merge with column-level

### Problem

User empirically tested (against real dbt output, not just docs) a model with *both* a
model-level `primary_key` constraint and a separate column-level `primary_key` constraint on
a different column, and reported: dbt disregards the column-level constraint entirely once a
model-level one is present -- it does not combine the two. Phase 21's fix unioned both tiers
unconditionally, which is wrong under this real behavior: it would report *two* primary-key
columns (one from each declaration) where dbt itself recognizes only the model-level one.

The dbt docs fetched for the previous answer don't cover this case explicitly -- they only
say column-level is for single-column keys and model-level is required for composite ones --
so this precedence had to be taken on the user's direct empirical report rather than
documented dbt behavior, consistent with this project's standing preference for verified
behavior over assumption.

### Fix

`model_primary_key_names()` reworked from "union both tiers" to strict, short-circuiting
priority: model-level constraint columns returned immediately if non-empty; only if empty is
the column-level tier consulted; only if that's also empty does the tags/`unique`+`not_null`
test fallback run. Tier 2 (column-level) still exists and still fixes the original Phase 21
report (a model with *only* a column-level primary key, no model-level constraints at all) --
the correction is specifically that tier 1 now excludes tier 2 rather than merging with it.

### Verification

- New test `test_model_level_primary_key_constraint_takes_priority_over_column_level`:
  a model with a model-level `primary_key` constraint on `other_key` *and* a column-level
  `primary_key` constraint on a different column `date_key` -- asserts only `other_key` is
  recognized, `date_key` is not.
- Full suite: `uv run pytest tests/`, `hatch run test.core:run`, `hatch run test.fusion:run`
  -- **66/66 passed** on all three.
- README's primary-key paragraph corrected to state the strict priority order and explain
  that a model-level constraint suppresses a column-level one rather than combining with it.

## Phase 23 — correction to Phase 22, and a real gap: `config.primary_key` (ClickHouse)

### Problem

Phase 22's "model-level suppresses column-level" claim was based on the user's own testing,
but on a ClickHouse project -- and ClickHouse is not a representative adapter for this
question, since it has no SQL primary-key constraint concept at all. The user flagged this
themselves ("maybe ignore my testing... do your own testing with duckdb") and gave the actual
ClickHouse mechanism: `{{ config(primary_key="date_key") }}` on the model, not `constraints:`.

Did the requested independent verification with dbt-core + DuckDB (a real constraint-
supporting adapter) rather than trust either the docs (silent on this) or the ClickHouse
report (misleading for this specific question): scaffolded a throwaway dbt project with a
model declaring a model-level `primary_key` constraint on one column and a column-level one
on a different column, ran `dbt parse`. Result: dbt's parser hard-errors --

    Primary key constraint error: (models\dates.sql)
    Primary key constraints defined at the model level and the columns level. Primary keys
    can be defined at the model level or the column level, not both.

This is a dbt-core parser check (raised before any adapter connection), so it should apply
identically regardless of warehouse. Conclusion: **a valid dbt manifest can never contain
both** a model-level and column-level `primary_key` constraint for the same model -- the
"which one wins" question from Phase 22 doesn't actually arise in practice. The Phase 22 code
change (strict priority instead of union) is harmless and stays as defensive-only.

Separately -- and this is the part that actually matters for the user's ClickHouse project --
scaffolded a second throwaway project using the real `dbt-clickhouse` adapter (installed
ephemerally via `uv run --with dbt-clickhouse`, `dbt parse` needs no live warehouse
connection) with `{{ config(primary_key="date_key") }}` on the model. Inspected the resulting
manifest.json directly: `node["constraints"]` and every column's `constraints` stayed `[]` --
completely empty -- while `node["config"]["primary_key"]` held `"date_key"`. Retested with a
list value (`primary_key=["date_key", "date_day"]`) and confirmed the manifest represents it
as a plain list. So `config.primary_key` is a wholly separate manifest field from
`constraints`, and for ClickHouse models it is the *only* place a primary key ever shows up --
our existing three tiers (model constraint, column constraint, tags/tests fallback) were
structurally blind to it, independent of any precedence question.

### Fix

Added a fourth detection tier to `model_primary_key_names()`, consulted only when both
constraint tiers found nothing: `model.get("config", {}).get("primary_key")`, handling both
the string and list manifest shapes. Placed after the two constraint tiers (more authoritative
where available) and before the tags/tests heuristic (an explicit adapter-native declaration
beats a heuristic).

### Verification

- New tests: `test_primary_key_detected_from_config_primary_key_string`,
  `test_primary_key_detected_from_config_primary_key_list`,
  `test_config_primary_key_only_consulted_when_constraints_are_absent` (confirms a genuine
  `constraints` declaration still wins over `config.primary_key` when both happen to be
  present on the same model).
- Full suite: `uv run pytest tests/`, `hatch run test.core:run`, `hatch run test.fusion:run`
  -- **69/69 passed** on all three.
- README's primary-key paragraph rewritten as an explicit four-tier priority list, naming the
  DuckDB and dbt-clickhouse verification for tiers 1/2 and tier 3 respectively.
- Both throwaway scratch dbt projects (DuckDB and dbt-clickhouse) deleted after verification
  -- neither was meant to be a lasting fixture.

## Phase 24 — `config.order_by` fallback for ClickHouse models with no explicit primary key

### Problem

User follow-up, prompted by their own domain knowledge of ClickHouse: MergeTree tables don't
require an explicit `PRIMARY KEY` at all -- if one isn't set, the table's `ORDER BY` clause
*is* the primary key. A ClickHouse dbt model using only `config(order_by=...)`, with no
`primary_key` config, still has a real, well-defined primary key that Phase 23's fix would
have missed entirely (falling through to the tags/tests heuristic, which finds nothing for a
column with no dbt-level `unique`/`not_null` tests).

Verified the underlying claim against ClickHouse's own docs (not dbt's -- this is warehouse
behavior, not a dbt feature) before implementing: "If no primary key is defined (i.e.
`PRIMARY KEY` was not specified), ClickHouse uses the sorting key as primary key." Confirmed
this is the *entire* sorting key, not a prefix of it (a prefix relationship only applies in
the other direction: an explicit `PRIMARY KEY` must be a prefix of `ORDER BY`, not the reverse).

Also checked the manifest shape empirically the same way as Phase 23 (throwaway
`dbt-clickhouse` scratch project, `dbt parse`, no live warehouse connection needed): confirmed
`config.order_by` is present independently of `config.primary_key`, in the same string-or-list
shape.

### Decisions

- Added as tier 4 (of 5), consulted only when `config.primary_key` found nothing --
  `config.primary_key` is authoritative when present since it's an explicit statement, not an
  inferred fallback.
- `order_by` can hold an arbitrary SQL expression, not just plain column names (e.g. a
  function call like `toStartOfMonth(event_time)` for a monthly-partition-friendly sort) --
  unlike `config.primary_key`, this is common in real ClickHouse usage. Added a shared
  `_as_name_set()` helper that intersects the raw string/list value against the model's actual
  column names for *both* the `primary_key` and `order_by` tiers, silently dropping anything
  that isn't a real column rather than returning a name Cube would never recognize as a
  dimension. (This doesn't change `config.primary_key` tier behavior in practice --
  expression-valued `primary_key` config is not a realistic case -- but keeps both tiers
  consistent and correct under the same shared logic.)
- If `order_by` resolves to nothing usable (e.g. purely a function expression with no matching
  columns), falls through to the tags/tests heuristic rather than stopping -- gives the
  fallback a chance to still find something real instead of silently returning no primary key.

### Verification

- New tests: `test_primary_key_falls_back_to_config_order_by_when_no_primary_key_is_set`,
  `test_config_primary_key_takes_priority_over_config_order_by`,
  `test_config_order_by_expression_that_matches_no_column_is_ignored` (an expression-only
  `order_by` falls through all the way to the `unique`+`not_null` test-based fallback).
- Full suite: `uv run pytest tests/`, `hatch run test.core:run`, `hatch run test.fusion:run`
  -- **72/72 passed** on all three.
- README's primary-key list extended to a fifth tier with the same ClickHouse-docs citation
  and the expression-matching caveat.
- Scratch dbt-clickhouse project (used only to confirm the manifest shape) deleted after
  verification.

## Phase 25 — bug: `Int16`/`Int32` missing from ClickHouse `TYPE_MAPPINGS`

### Problem

Real production error from the user's `dbt refresh-defs-state` run: 6 columns
(`statistical_area_1_id`, `urban_rural_id`, etc., all `Int32`) failed with
`UnrecognizedColumnTypeError`. Checked `TYPE_MAPPINGS` (Phase 17's ClickHouse addition)
directly: it added `int128`/`int256` and the full `uint8` through `uint256` family, but never
added plain `int16`/`int32`. `int8`/`int64` happened to already work, purely by accident --
`int8` collides harmlessly with Redshift's same-spelled (but semantically different, 8-*byte*
not 8-*bit*) type, and `int64` was already present for BigQuery -- both resolve to `number`
either way, so the collision never surfaced a problem, but it also meant nobody noticed
`int16`/`int32` had no equivalent accidental coverage from another vocabulary.

### Fix

Added `"int16": "number"` and `"int32": "number"` to `TYPE_MAPPINGS`. Also audited the
adjacent `float32`/`float64` pair while in there -- both already present, no further gaps
found in the sized-numeric-type families.

### Verification

- Extended the existing `test_clickhouse_types_are_natively_recognized` test (rather than
  adding a new one -- this is precisely the kind of column an "all the common ClickHouse
  sized types" test should have already covered) with `Int16`/`Int32` columns.
- Full suite: `uv run pytest tests/`, `hatch run test.core:run`, `hatch run test.fusion:run`
  -- **72/72 passed** on all three (same count -- existing test extended, not a new one).

## Phase 26 — `geo` dimensions rejected outright, not silently broken

### Problem

Real error report: a ClickHouse `Point` column failed `UnrecognizedColumnTypeError`, and the
message's suggested fix listed `geo` as a valid `meta.cube.type` override target. User asked
a genuine design question first -- exclude geometry types, or keep erroring and point users at
`meta.cube.dimension: false`?

Checked Cube's own docs before answering (not assumed): a `geo` dimension takes
`latitude`/`longitude` SQL sub-expressions *instead of* a single `sql` field -- "Requires
`latitude` and `longitude` sub-parameters instead of `sql`." Every dimension this library
generates (`_build_dimension` in generation.py) unconditionally sets a single `sql` field, so
`geo` is structurally impossible to build correctly here, for *any* column, geometry or not --
not just risky, actually impossible without warehouse-specific knowledge (a ClickHouse
`Point`'s coordinates come from tuple accessors; a `Polygon`/`MultiPolygon` doesn't even have
a single lat/long point to extract).

This surfaced a second, more serious, pre-existing bug while investigating: `manifest.py`
already maps BigQuery's `GEOGRAPHY` data_type to `"geo"` in `TYPE_MAPPINGS`, and `"geo"` is a
member of `VALID_DIMENSION_TYPES` -- so a BigQuery `GEOGRAPHY` column was silently building a
broken `sql`+`type: geo` dimension with **no error at all**, unlike the ClickHouse case which
at least failed loudly. No test covered this at all before now.

### Decisions

- `generate_cubes()` now explicitly checks `dimension_type == "geo"` (after both the
  unrecognized-type and missing-data-type checks) for *every* column, whether the type came
  from inference (`GEOGRAPHY`) or an explicit `meta.cube.type: geo` override -- single
  enforcement point covers both paths uniformly.
- New dedicated `UnsupportedGeoDimensionError`, not lumped into `UnrecognizedColumnTypeError`
  -- `geo` isn't actually unrecognized (it's mapped/valid), it's recognized-but-unbuildable,
  and that distinction matters for what the error message should say: the geo-specific
  message explains *why* (the `sql` vs. `latitude`/`longitude` shape mismatch) rather than
  the generic "can't be mapped" framing, which would be misleading here.
  `UnrecognizedColumnTypeError`'s own message dropped `geo` from its list of valid override
  targets, since setting it never actually worked.
  - `TYPE_MAPPINGS["geography"]` stays mapped to `"geo"` (not removed) -- it's not wrong, Cube
    really does call this type `geo` -- but got a comment explaining the mapping exists for
    completeness/documentation purposes, since generation always rejects the result.
- Recommended path for geometry columns, in the README: `meta.cube.dimension: false` to
  exclude (the right call if raw geometry isn't needed as a dimension), or hand-author a real
  `geo` dimension via a merge patch with actual `latitude`/`longitude` SQL for the warehouse
  in question -- already fully general, no code change needed, since merge patches can add
  arbitrary hand-written dimensions to the generated cube.

### Verification

- New tests: `test_geography_data_type_raises_unsupported_geo_dimension_error` (closes the
  previously-uncovered silent-success gap for BigQuery `GEOGRAPHY`),
  `test_meta_cube_type_geo_override_raises_unsupported_geo_dimension_error`. Existing
  `test_unrecognized_data_type_raises_with_all_offenders_listed` extended with an assertion
  that `"geo"` no longer appears anywhere in that error's message.
- Full suite: `uv run pytest tests/`, `hatch run test.core:run`, `hatch run test.fusion:run`
  -- **74/74 passed** on all three.
- README: new "`geo` columns aren't generated" section explaining the `sql` vs.
  `latitude`/`longitude` shape mismatch and both recommended paths, linking Cube's actual
  docs page.

## Phase 27 — `meta.cube` promotion extended to models (`public`, `title`)

### Problem

User question: dimension-level `meta.cube` already promotes `order`/`mask`/`public` onto the
generated dimension (Stage/Phase from earlier in the project) -- does the same exist at the
model level, for cube-level attributes like `public` and `title`? Checked `_build_cube()`
directly: no, it only ever read `name`/`description`/`sql_table` from the model, with no
`meta.cube` handling at all.

Verified the manifest shape empirically before assuming it mirrors columns (a throwaway
DuckDB scratch project, `dbt parse`, deleted after): a model's `config: {meta: {cube: {...}}}`
compiles to *both* `node["meta"]` (top-level, exactly where column `meta` already lives) and
`node["config"]["meta"]` -- confirming `model.get("meta")` is the right read, consistent with
how `column.get("meta")` already works.

### Decisions

- `_cube_meta()` (previously typed for `ColumnNode` specifically) generalized to accept
  `Mapping[str, Any]` and renamed its parameter to `node` -- `ColumnNode`/`ModelNode` are both
  already just aliases for the same `Mapping[str, Any]` type in `manifest.py`, and the helper
  itself never did anything column-specific, so this is a pure rename/broadening, not new
  logic.
- New `PROMOTED_CUBE_MODEL_META_KEYS = ("public", "title")`, promoted in `_build_cube()` using
  the exact same consume-and-fall-through pattern `_build_dimension()` already uses for
  `PROMOTED_CUBE_DIMENSION_META_KEYS` (renamed from the former `PROMOTED_CUBE_META_KEYS` for
  clarity now that there are two, dimension- and model-scoped, promoted-key sets): promoted
  keys become real top-level cube attributes; anything else under `meta.cube`, or under
  `meta` outside the `cube` namespace, is left in the cube's own `meta:` rather than dropped.
- Scoped to exactly what was asked (`public`, `title`) rather than every other cube-level
  attribute Cube supports (`data_source`, `sql_alias`, etc.) -- easy to extend later since the
  mechanism is a plain tuple of key names, not a design that needs revisiting per key.
- No `component.py` change needed: `get_cube_asset_spec` already dumps the full generated
  cube dict (whatever keys it has) into `dagster_cube_dbt/yaml` metadata via `_yaml_metadata`,
  so `public`/`title` show up there automatically once generation emits them.

### Verification

- New tests: `test_promoted_meta_keys_become_top_level_cube_attributes`,
  `test_unrecognized_cube_meta_and_non_cube_meta_still_pass_through_on_the_cube` -- mirroring
  the existing dimension-level tests for the same promotion mechanism.
- Full suite: `uv run pytest tests/`, `hatch run test.core:run`, `hatch run test.fusion:run`
  -- **76/76 passed** on all three.
- README: new "Setting cube attributes from dbt model meta" section mirroring the existing
  dimension-level one.
- Scratch DuckDB project (used only to confirm the manifest shape) deleted after verification.

## Phase 28 — renaming a cube via `meta.cube.name` / `meta.cube.suffix`

### Problem

User request: support renaming the generated cube, via a full `meta.cube.name` override or a
`meta.cube.suffix` appended to the dbt model's own name, mutually exclusive. Motivation: a
common Cube pattern -- a `public: false`, suffixed (e.g. `_base`) base cube, with a separate
hand-authored `extends:` cube exposing it publicly under a plain name.

Before implementing, traced how deep `cube["name"] == model["name"]` is actually assumed
across the codebase, since this is the join key for several things at once: merge-patch
matching (`merge.py`'s `DEFAULT_MERGE_KEY = "name"`), `extends` resolution
(`resolve_extends`), and -- the one that actually mattered -- `get_cube_asset_spec` computing
`deps` via `self._dbt_model_asset_key_or_none(cube["name"])`. Read `DbtProjectComponent
.asset_key_for_model`'s real source directly rather than assume: it matches purely by
`value["name"] == model_name` against the dbt manifest, with no notion of Cube at all. So a
naive rename would silently drop the cube asset's dependency on its real dbt model --
`_dbt_model_asset_key_or_none` would just fail its lookup and return `None`, not point at the
wrong asset, but still break the freshness/lineage propagation `is_virtual` assets rely on
(a virtual asset with no deps has nothing to look through).

Renaming *has* to happen before merge-patch application (inside `generate_cubes`, not later)
-- otherwise the merge-patchable "base" document wouldn't yet reflect the final name, and a
user authoring a patch to extend the renamed cube would have no consistent name to target
that also matches what's actually in the promoted YAML files.

### Decisions

- New `_resolve_cube_name(model)` in generation.py: `meta.cube.name` if set (verbatim
  override), else `model['name'] + meta.cube.suffix` if a suffix is set (plain string
  concatenation -- no separator inserted, so it's included in the suffix value itself), else
  the model's own name unchanged. Returns `None` if both are set, treated by the caller as a
  violation to collect -- new `ConflictingCubeNameError`, following the same
  collect-across-the-run-then-raise-once pattern every other generation error already uses.
  Checked right after the contract-enforcement check (before any per-column work), since it's
  a model-level structural issue, not a per-column one.
- `name`/`suffix` are **control flags** consumed by `_build_cube` (popped from `cube_meta`
  before the `PROMOTED_CUBE_MODEL_META_KEYS` promotion loop, mirroring exactly how
  `dimension`/`type` are already consumed as column-level control flags rather than promoted
  attributes) -- they determine what `cube["name"]` *is*, they don't become literal `name`/
  `suffix` keys on the output.
- The real dbt model dependency for a renamed cube is tracked via a **new sibling key**,
  `cube_source_models: {cube_name: model_name}`, returned alongside `cubes` by
  `generate_cubes()` -- not embedded inside each cube dict, since that would leak into the
  real promoted Cube YAML (`write_entities` writes whatever's on the cube dict verbatim, with
  no filtering). Survives `merge_documents()` untouched (patches never define this key, and
  deepmerge's dict-merge strategy leaves a base-only key alone), so it round-trips through the
  cached state file for free.
- `build_defs_from_state` reads `cube_source_models` and injects it into the already-separate,
  already-internal-only `resolved_cubes` dict (built by `resolve_extends`, deep-copied,
  documented as never flowing into the real promoted YAML) under a double-underscore-prefixed
  key, `__dagster_cube_dbt_dbt_model_name` -- reusing an *existing* "derived, Dagster-facing
  only" data structure rather than inventing a new channel, and keeping `get_cube_asset_spec`'s
  public single-argument signature unchanged (changing how many arguments it's *called* with
  would break any subclass override, regardless of default values on the base definition).
  `get_cube_asset_spec` pops that key back out (`cube = dict(cube); cube.pop(...)`) before
  building `_yaml_metadata`/`_column_schema`/`_code_version`, so it never appears in the
  asset's own displayed metadata or affects its `code_version` hash.

### Verification

- generation.py unit tests: `test_meta_cube_name_overrides_the_cube_name_outright`,
  `test_meta_cube_suffix_appends_to_the_model_name`,
  `test_meta_cube_name_and_suffix_together_raises_with_all_offenders_listed`,
  `test_cube_source_models_maps_unrenamed_cubes_too`.
- Component-level test, `test_get_cube_asset_spec_resolves_dbt_model_dependency_after_a_rename`:
  runs a real `write_state_to_path` against the fixture dbt project, then rewrites the cached
  state to simulate a renamed `dates` -> `dates_base` cube (deliberately not editing the
  shared fixture's `schema.yml` itself, which many other tests also depend on the exact shape
  of) -- confirms the resulting `AssetSpec.deps` still resolves to the *real* `dates` dbt
  model asset key, and that the internal tracking key never leaks into the asset's own
  `dagster_cube_dbt/yaml` metadata.
- Full suite: `uv run pytest tests/`, `hatch run test.core:run`, `hatch run test.fusion:run`
  -- **81/81 passed** on all three.
- Real end-to-end check against `dagster-cube-dbt-tests`: temporarily added
  `meta.cube.suffix: "_base"` / `public: false` to its `dates` model, ran a real
  `dg utils refresh-defs-state`, confirmed via the cached state file and `dg list defs --json`
  that the cube renamed to `dates_base` (`public: false`), `cube_source_models` correctly
  mapped `dates_base -> dates`, and the real Dagster asset graph showed `cube/dates_base`
  depending on `dates` (the real dbt model asset) -- not a nonexistent `dates_base` one, and
  not nothing. Reverted the schema.yml change and re-ran the refresh afterward to leave the
  example project's cached state clean.
- README: new "Renaming the generated cube with `meta.cube.name` / `meta.cube.suffix`"
  section with the full base-cube/`extends`-cube pattern worked through.

## Phase 29 — a bug found via real usage, then a design correction from the user

### Problem (part 1: a real bug)

User posed a real, more elaborate version of the base-cube pattern to sanity-check: one
suffixed `_base` cube extended by *three* separate hand-authored public cubes (not just one).
Traced it through by hand rather than assuming Phase 28 already handled it: `build_defs_from_
state` only injected `_DBT_MODEL_NAME_KEY` into `resolved_cubes` *after* calling
`resolve_extends(cubes)` -- landing on the base cube's own already-finalized entry, but never
propagating to any child that extends it, since those children were already fully resolved
(deep-copied) by the time the injection ran. Two of the three extension cubes in the user's
example (`origin_locations`/`destination_locations`) would have silently gotten `deps: []` --
only the third happened to work, purely by coincidence (its own cube name happened to equal
the real dbt model's name). First fix: inject the key *before* calling `resolve_extends`
instead, letting the existing strategic-merge inheritance (the same mechanism that already
propagates `description`/`meta` from parent to child) carry it down through `extends` chains
too. Verified against a real `dg utils refresh-defs-state` run (dagster-cube-dbt-tests, a
temporary suffixed `dates`/`dates_a`/`dates_b` patch, reverted after) that this fix alone
produced correct deps -- all three depending directly on the real `dates` dbt model.

### Problem (part 2: a real design question)

Before treating that fix as final, the user asked directly: should an `extends`-child of a
dbt-derived cube depend on the dbt model asset (what the part-1 fix produced), or on the
*cube* it extends -- with the dbt model dependency still reachable transitively through that
cube? They said they preferred the latter.

This is the better design, and not just because the user prefers it: these are `is_virtual`
assets, and Dagster's own staleness engine already looks straight through a *chain* of virtual
assets to the nearest real ancestor (confirmed earlier in this project, DECISIONS.md's
`is_virtual`/`get_non_virtual_ancestor_keys` phase) -- so a cube-to-cube edge doesn't lose any
freshness/lineage propagation at all, it just makes the graph one hop longer, and that hop
mirrors the *real* relationship (`origin_locations`' actual SQL/dimensions come from
`journey_samples_base`, not from the dbt model directly). A flat fan-out where every extending
cube independently points at the same dbt model is less accurate and needlessly duplicates
what a single `extends` edge on the base cube already establishes.

### Decisions

- Reworked entirely: `get_cube_asset_spec`'s single dependency is now the `extends` parent's
  own cube asset when the raw (unresolved) cube has an `extends` field pointing at *another
  generated cube* -- checked via a new `_EXTENDS_PARENT_KEY` internal key, same
  double-underscore-prefixed/strip-before-display convention as `_DBT_MODEL_NAME_KEY`. Falls
  back to the direct dbt-model lookup (`_DBT_MODEL_NAME_KEY`/`cube_source_models`) only when
  there's no `extends`, or its target isn't among what this component generated (e.g. a
  hand-authored cube extending something entirely external) -- mirroring how `resolve_extends`
  itself already treats an unresolvable `extends` target.
- This *removed* the part-1 fix's pre-`resolve_extends` injection entirely -- no longer
  needed, since propagation now happens via one cube-to-cube edge per `extends` hop plus
  Dagster's own virtual-asset staleness lookthrough, not by inheriting dbt-model info through
  the merge machinery. `_DBT_MODEL_NAME_KEY` is looked up directly per-cube from
  `cube_source_models` again (a cube either is in that mapping or it isn't -- no inheritance
  involved), and `_EXTENDS_PARENT_KEY` is read straight from each cube's own raw `extends`
  field in the `cubes` loop, not from the extends-*resolved* view (which never carries
  `extends` at all -- `resolve_extends` always pops it, confirmed by re-reading its source
  before relying on it here).
- The pre-existing `journey_samples_extended` extends test (Phase 18) previously asserted
  nothing about `deps` at all -- under the *old* logic it would have gotten `deps: []` (no dbt
  model named `journey_samples_extended` exists), silently disconnected from the graph. Now it
  correctly depends on the `journey_samples` cube asset -- a real improvement to existing
  behavior the user's question surfaced, not just new behavior for the new rename feature.

### Verification

- `test_hand_authored_extends_children_of_a_renamed_cube_depend_on_the_parent_cube` (renamed
  and rewritten from Phase 29 part 1's version): asserts each child's `parent_keys` is the
  *base cube's* asset key, not the dbt model's, and separately asserts via
  `get_non_virtual_ancestor_keys` that freshness still resolves through to the real dbt model
  transitively -- both properties verified together, since only checking one could hide a
  regression in the other.
- `test_get_cube_asset_spec_sees_extends_resolved_fields` (Phase 18's original test) extended
  with a `parent_keys` assertion, closing the coverage gap that let the old, disconnected
  behavior go unnoticed in the first place.
- Full suite: `uv run pytest tests/`, `hatch run test.core:run`, `hatch run test.fusion:run`
  -- **82/82 passed** on all three (same count as the part-1 fix -- one test renamed/rewritten
  rather than added, one existing test extended).
- Real end-to-end re-verification against `dagster-cube-dbt-tests` with the exact multi-child
  pattern (`dates_base` extended by `dates_a` and `dates_b`, temporary, reverted after): `dg
  list defs --json` confirmed `cube/dates_a` and `cube/dates_b` depend on `cube/dates_base`
  (not `dates` directly), and `cube/dates_base` depends on the real `dates` dbt model asset.
- README's renaming section rewritten around a 3-cube example (one base extended by two
  differently-joined public cubes, matching the user's actual `origin_locations`/
  `destination_locations`-style scenario) and its `deps` claim corrected; the `extends` and
  asset specs section gained a note that `deps` is deliberately *not* flattened through
  `extends` chains the way other fields are.

## Phase 30 — a Dagster concurrency pool for the promotion op

### Problem

Follow-up from building a real `GitCubeFilePromoter` (in the user's own project, outside this
repo): that resource keeps a persistent local git checkout and resets it on every call --
explicitly documented as unsafe for two concurrent runs to touch at once. The user connected
this to a broader point: nearly every real `CubeFilePromoter` mutates *some* shared external
state (a git checkout, a fixed output directory, an S3 prefix under active use elsewhere) that
concurrent promotion would race on, so this isn't specific to the git case -- it's a property
of the promotion op in general, and should be addressed once, here, rather than left to every
promoter author to remember.

Checked `dg.multi_asset`'s actual signature (not assumed) and confirmed it already accepts a
`pool: str | None` parameter directly -- Dagster's newer "concurrency pools" feature. Verified
against Dagster's own docs (fetched, not recalled from training) that assigning a pool by
itself changes nothing: a pool with no configured limit behaves identically to no pool at all,
and the actual max-concurrency-1 enforcement is configured separately, per pool name, in the
Dagster UI (Deployment > Concurrency) -- meaning assigning a pool by default is a genuinely
zero-cost, no-behavior-change default until someone opts into a real limit.

### Decisions

- New `promotion_pool: str | None` component field (default `None`, meaning "auto-derive").
  `_cube_assets`'s `@dg.multi_asset(...)` now always passes `pool=self.promotion_pool or
  f"{self.dbt_project.name}_cube_promotion"` -- scoped per dbt project by default (mirroring
  the existing `f"{dbt_project.name}_cubes"` op-name/sensor-name convention), so multiple
  `CubeDbtProjectComponent`s in one project don't get needlessly serialized against each
  other's *independent* promoters. An explicit override lets someone deliberately share one
  pool across components that share the same underlying promoter/destination instead.
- No "explicitly disable pooling" config value was added -- redundant given the "assigning a
  pool changes nothing without a configured limit" property confirmed above; simply never
  setting a limit for that pool name achieves the identical effective behavior.

### Verification

- New tests: `test_cube_assets_get_a_default_promotion_pool`,
  `test_promotion_pool_is_configurable` -- both read `.op.pool` directly off the built
  `AssetsDefinition` in `defs.assets` (confirmed this is how a pool assignment surfaces, by
  checking a throwaway `@dg.multi_asset(pool=...)` directly first, rather than guessing the
  attribute name).
- Full suite: `uv run pytest tests/`, `hatch run test.core:run`, `hatch run test.fusion:run`
  -- **84/84 passed** on all three.
- Real end-to-end check against `dagster-cube-dbt-tests`: built the real component the way
  `dg` does and confirmed the live multi-asset's op reports
  `pool == "cube_dbt_fixture_cube_promotion"`.
- README: new "Concurrency: the `promotion_pool`" section (with a verified-live link to
  Dagster's concurrency-pools docs page, fetched directly rather than guessed) explaining why
  this exists, that it's zero-cost until a limit is set, and when to override it.

## Phase 31 — `GitCubeFilePromoter` ships with the library

### Background

Built entirely in the user's own project (`dagster-nz-gov-elt`) across several turns, then
moved into this library once it had matured -- this phase records the whole arc in one place,
since none of it was written up here while it lived elsewhere.

1. **Initial build**: a `CubeFilePromoter` subclass keeping a *persistent* local git checkout
   (`local_repo_dir`), reset to match the remote on every `promote()` call.
2. **Redesigned to a fresh clone per run**, user-prompted: "if you just reset the clone to
   match the remote, why not just use a temporary directory each run?" Right question --
   `local_repo_dir` bought nothing but avoiding a full `git clone`, and `--depth 1` (this
   resource never needs history, only ever adds one new commit on the current tip) recovers
   nearly all of that for free while removing the actual hazard: a shared local directory two
   concurrent runs could corrupt. Verified via a real bare-repo test harness across first-push
   (into a genuinely empty repo -- `git clone --branch` fails outright against one, discovered
   by testing it, not assumed), no-op, changed-content, and dirty-checkout-recovery scenarios.
3. **K8s deployment context** ("no need to worry about laptop wifi, this deploys to a k8s
   cluster"): confirmed the temp-dir redesign was doubly correct -- a "persistent" checkout
   path likely wouldn't even survive between runs in a container-per-run deployment anyway.
   Checking the user's real Dockerfile surfaced a genuine, concrete bug: `git`/`openssh-client`
   were installed in the build stage (for private git deps at build time) but never in the
   final runtime stage `python:3.12-slim` actually ships as -- the first real promotion in the
   cluster would have failed with a bare "command not found." Fixed. Separately switched
   `ssh_key_path` (a file path) to `ssh_private_key` (file contents) to match this project's
   own established convention of injecting every other credential as an env var
   (`CH_PASSWORD`, `PROXYHOPPER_API_KEY`, ...) rather than requiring a new Secret-volume-mount
   pattern just for this one resource -- the key is written to a private temp file for the
   duration of one `promote()` call and removed when it returns.
4. **"Is pushing to a repo Cube syncs from the typical Cube Cloud CI/CD pattern?"** -- checked
   Cube's own docs directly rather than assume: no, not quite. Cube Cloud's actual documented
   patterns are pushing straight to a Cube-Cloud-hosted git remote ("Deploy with Git") or
   `cubejs-cli deploy`, neither of which involves an intermediate repo Cube Cloud polls on its
   own. The "push to a repo, something else syncs it" pattern the resource was actually built
   for is specifically the self-hosted **Cube Core** convention (a manually-configured
   `kubernetes/git-sync` sidecar, confirmed by the user -- there's no Cube-native feature for
   this at all).
5. **"Can we support both?"** -- yes, and it turned out to need real new logic, not just a
   config tweak: checked Cube Cloud's git-deploy docs again specifically for its
   authentication mechanism (`git config credential.helper store`) and confirmed it's
   **HTTP username+token**, not SSH -- a genuinely different transport/auth path than the
   deploy-key case. Added `http_username`/`http_token` as a second, mutually-exclusive
   credential pair (enforced via a pydantic `model_validator`), authenticated via a
   per-invocation `-c http.extraHeader="Authorization: Basic ..."` rather than a
   credential-embedded URL -- deliberately, since git sometimes echoes a credential-embedded
   URL back verbatim in its own error output, which a header-based approach avoids entirely.
   Added explicit redaction of the token *and* its base64-encoded header form (an encoding,
   not encryption -- equally sensitive) from any error message this resource raises, and
   verified it by forcing a real failure and confirming neither leaked. Also corrected
   `cubes_subdir`/`views_subdir` defaults from a made-up `cubes`/`views` to Cube's own actual
   documented project convention, `model/cubes`/`model/views` (checked, not assumed) -- more
   important now that one of the two targets is Cube Cloud itself, which expects exactly that
   layout.
6. **"Doesn't pip support `package[extra]` -- can't we ship git optionally that way? Or ship
   the git binary via an extra?"** -- clarified a real mechanism mismatch: pip extras only
   ever pull in more *Python* packages; there's no PyPI package that installs a working `git`
   CLI (it isn't a Python package), so extras have nothing to gate here regardless -- this
   resource has zero additional required Python dependencies at all (pure stdlib + pydantic,
   already required transitively). Then the user asked the sharper follow-up: "how does that
   work for `psycopg[binary]`, then?" -- checked directly rather than wave it away:
   `psycopg[binary]` genuinely does bundle a precompiled `libpq`/`libssl` *inside* the wheel
   itself (a small, staticly-linkable client library, built per-platform) -- a real, different
   pattern from what `git` would need (a large, complex, standalone multi-file toolchain, not
   a shared library meant for linking into a Python C extension). Conclusion: ship the
   `GitCubeFilePromoter` *code* unconditionally (it costs nothing -- no extra deps, no
   import-time side effects, matching how `LocalFileCubeFilePromoter` already ships
   unconditionally), and treat `git`/`ssh` as a documented runtime prerequisite -- the same
   category of thing as `dbt` needing a working CLI or a Postgres resource needing `libpq` --
   rather than something pip can or should try to install. Added a `shutil.which("git")` check
   at the top of `promote()` raising a clear, specific error immediately if it's missing,
   instead of a confusing failure deep inside a subprocess call.

### Decisions

- Shipped as `dagster_cube_dbt.git_promoter.GitCubeFilePromoter`, exported at the package top
  level alongside `CubeFilePromoter`/`LocalFileCubeFilePromoter`. Imports `CubeFilePromoter`
  from the `dagster_cube_dbt.resources` submodule directly (not the package `__init__`), to
  avoid a circular import now that `__init__.py` itself imports from this new module.
- The user's own project (`dagster-nz-gov-elt`) had its local copy deleted and its resource
  binding switched to import from the shared library instead -- an editable path install, so
  the change was picked up with no reinstall needed. Its now-unnecessary `.gitignore` entry
  (added when the resource was still private/evolving) was removed too.

### Verification

- New `tests/test_git_promoter.py`: first-push-into-an-empty-repo, no-op-when-unchanged,
  changed-content-pushes-a-new-commit, and the same for HTTP auth mode too -- all against real
  local bare git repos (a local-path clone bypasses the transport but exercises the identical
  clone/write/add/commit/push code paths). Plus: HTTP token never leaks into a raised error
  (forces a real failure against an unreachable host, not just asserting the redaction logic
  in isolation); both/neither/partial credential combinations raise clearly at construction;
  a missing `git` binary raises a clear error before any subprocess is even attempted
  (`monkeypatch`s `shutil.which`).
- Full suite: `uv run pytest tests/`, `hatch run test.core:run`, `hatch run test.fusion:run`
  -- **93/93 passed** on all three.
- README: the old illustrative `GitSyncCubeFilePromoter` code sketch replaced with real usage
  of the shipped resource; new "Git-based promotion: `GitCubeFilePromoter`" section covering
  both auth modes, the runtime `git`/`ssh` prerequisite, and linking Cube's own docs for both
  the Cube Cloud and Cube Core patterns (all fetched and verified during this phase, not
  guessed).

## Phase 32 — two real production bugs, found via the user's actual k8s-bound deployment

The user filled in real SSH-deploy-key credentials in `dagster-nz-gov-elt` (still running
locally on Windows at this point, ahead of the eventual k8s deployment) and hit two genuine
bugs in `GitCubeFilePromoter` in quick succession -- neither caught by the existing test
suite, since both only manifest against conditions the tests didn't happen to exercise.

1. **`git clone` failed with `Load key "...": error in libcrypto` / `Permission denied
   (publickey)`.** Not a bad key or a wrong deploy-key setup -- `_build_auth` wrote the SSH
   private key with `Path.write_text(self.ssh_private_key)`, whose default text-mode write
   translates every `\n` to the platform line separator. On Windows that's `\r\n`, and a
   CRLF-terminated PEM file fails to parse in OpenSSL; `ssh` surfaces that as the opaque
   "error in libcrypto" rather than anything mentioning line endings, then falls through to a
   generic publickey rejection since no usable key ever actually loaded. Fixed by writing with
   `newline="\n"` (no platform translation) and ensuring a trailing newline. Added a
   regression test (`test_ssh_key_file_is_written_without_crlf_translation`) that writes a
   multi-line key and asserts on the raw bytes written to disk -- `b"\r\n" not in written` --
   rather than only exercising the happy path against a same-platform local repo, which is
   what let this ship in the first place (the existing SSH-mode tests all pointed `repo_url`
   at a local bare repo, so `git clone` never actually shelled out to `ssh` at all).
2. **`git push` was rejected: `! [rejected] dev -> dev (fetch first)`.** A real logic bug, not
   an environment quirk. `_clone` never passed `--branch` to the initial `git clone` call --
   and `--depth` implies `--single-branch`, so an *unpinned* clone only ever fetches the
   remote's *default* branch (this repo's was `main`). The user's `branch="dev"` already had
   independent commits pushed by hand, but since a single-branch shallow clone never even
   creates the `origin/dev` remote-tracking ref for a branch it didn't fetch, the existence
   check (`git rev-parse --verify --quiet origin/dev`) always came back false -- not because
   `dev` didn't exist, but because the clone was structurally incapable of seeing it. This
   silently forked a fresh local `dev` off of `main`'s tip and pushed, which GitHub correctly
   rejected as non-fast-forward (a safe failure, not silent data loss -- but still wrong).
   Fixed by pinning `--branch self.branch` on the *first* clone attempt, so a non-default
   branch that already exists is actually fetched; only fall back to the old
   unpinned-clone-then-`checkout -b` path when that specific attempt fails, which still
   correctly covers both "branch genuinely doesn't exist yet" and "repo has no commits at all
   yet" (both fail identically against a pinned branch name). `_remote_branch_exists` -- the
   now-provably-unreliable check -- was removed entirely rather than patched, since a failed
   pinned-clone attempt *is* the check. Added a regression test that seeds a non-default `dev`
   branch with independent history via a real second clone before promoting, confirming the
   push now fast-forwards instead of forking.

Both fixes verified against the full local suite (95/95 passing) and, per the user's own
follow-up, against the real local Dagster deployment that originally hit them.

## Phase 33 — GitHub Actions CI/CD: Conventional Commits, automated changelog, PyPI Trusted Publishing

**User's request:** "we need github actions and ci/cd pipeline to publish the package to
Pypi. We need to do it properly, including preview packages etc. What is the modern way to
deploy python packages with github actions, automating version numbers, change logs, etc?"
Explicit follow-up goal, when asked to choose a preview-package channel: "Go with the option
that is industry standard, I want this project to be adopted if people find it useful."

Recommended and confirmed (via `AskUserQuestion`) rather than assumed: the two real open
design axes were (a) where preview/prerelease builds should live -- real PyPI as SemVer
prereleases (the actual industry-standard pattern for adoption-focused OSS libraries: numpy,
pandas, mypy, black, etc. all publish `rc`/`beta` versions to the *same* real index rather
than a separate TestPyPI channel, since TestPyPI is conventionally only used to test the
publishing pipeline itself, not as an ongoing user-facing preview channel) -- and (b) how
version bumps get decided (Conventional Commits, fully automatic, vs. a manual version-bump
PR). User picked "industry standard" (real-PyPI prereleases) and "fully automatic" for both.

Researched rather than guessed, since getting a real publishing pipeline wrong is expensive
to debug after the fact: current `python-semantic-release` (v10.6.1) GitHub Action syntax and
outputs, its `[tool.semantic_release.branches.*]` prerelease-branch config keys
(`match`/`prerelease`/`prerelease_token`), its `directory` action input (needed since this
repo's `pyproject.toml` lives at `python_modules/dagster-cube-dbt/`, not the repo root), the
`major_on_zero` default (confirmed `true` -- a breaking-change commit on a `0.x` version jumps
straight to `1.0.0` unless overridden), current `astral-sh/setup-uv`/`actions/checkout`
versions, and the official `pypa/gh-action-pypi-publish` Trusted Publishing workflow shape
from `packaging.python.org` itself. Also directly verified that `"Framework :: Dagster"` --
an initially-added classifier that seemed reasonable -- is **not** a real PyPI trove
classifier (checked against the canonical list rather than assumed), which would have failed
upload validation; removed it before it ever shipped.

### Decisions

- **Conventional Commits → `python-semantic-release`**, computing the next version from
  commit messages since the last release, writing it back into `pyproject.toml`'s
  `project.version` (the single source of truth -- no separate dynamic-versioning-from-git-tag
  mechanism, since the build backend here is `uv_build`, not `hatchling`, so `hatch-vcs`-style
  plugins don't apply), generating `CHANGELOG.md`, committing, tagging, and creating a GitHub
  Release.
- **`major_on_zero = false`**, explicitly overriding PSR's default. This library is still
  `0.x` and breaking changes are a normal, frequent part of early iteration (e.g. the
  `ssh_key_path` → `ssh_private_key` rename earlier this session) -- treating every one of
  them as "declare 1.0 now" would make that decision by accident, via a commit message,
  rather than deliberately. Left a comment explaining exactly how/when to flip it.
- **Branch-based preview channel**: `main` produces stable releases; `next` produces SemVer
  prereleases (`X.Y.ZrcN`) published to the *same* real PyPI project -- `pip install
  dagster-cube-dbt` never resolves to one by construction (PEP 440 excludes prereleases from
  plain resolution), only `pip install --pre` or an exact pin does. No separate TestPyPI setup
  needed as an ongoing channel.
- **PyPI Trusted Publishing (OIDC)** via `pypa/gh-action-pypi-publish`, not a stored API token
  -- `id-token: write` permission scoped to a dedicated `pypi` GitHub Environment, matching
  the Trusted Publisher registered on PyPI's side (project name + owner + repo + workflow file
  + environment name). Zero long-lived publishing credentials anywhere in the repo.
- **Two-workflow split**: `ci.yml` is a reusable (`workflow_call`) test-matrix workflow, called
  by both `pr.yml` (every PR) and `release.yml` (as a required job before `release`/`publish`
  can run) -- the exact same tests gate both a PR merge and an actual PyPI publish, not two
  different check sets.
- **`publish` rebuilds from the tagged commit** in its own job/runner (`uv build` again, after
  checking out `needs.release.outputs.tag`) rather than passing build artifacts through from
  the `release` job -- simpler than cross-job artifact upload/download, and guarantees what
  gets published is built from exactly what got tagged, not from a pre-tag intermediate state.
- Real package metadata added for the first time: `description`, `classifiers`, `license` /
  `license-files` (LICENSE copied into the package directory itself, since the build backend's
  project root is `python_modules/dagster-cube-dbt/`, not the repo root the existing LICENSE
  lived at -- the root copy alone would never have been packaged), and `project.urls`
  (Homepage/Repository/Issues/Changelog, all pointing at the real GitHub repo).

### Verification

- `uv build` succeeds with the new metadata; `twine check dist/*` -- **PASSED** on both the
  wheel and sdist, confirming the metadata is genuinely valid for a real PyPI upload, not just
  syntactically present in `pyproject.toml`. Directly inspected the built wheel's contents and
  confirmed the license file is actually bundled at
  `dagster_cube_dbt-0.1.0.dist-info/licenses/LICENSE` (PEP 639), not just declared.
  `dist/` cleaned up afterward (already gitignored).
- All three new workflow YAML files parse successfully (`yaml.safe_load`).
- One-time, maintainer-only setup this can't complete unattended -- documented in the README's
  new "Releasing" section rather than silently assumed done: creating the `pypi` GitHub
  Environment, and registering the PyPI pending publisher (project name, owner, repo, workflow
  file, environment name) at `https://pypi.org/manage/account/publishing/`.

## Phase 34 — the pipeline's first real run, and two more bugs it actually caught

The user set up the PyPI pending publisher and the `pypi` GitHub Environment, then pushed.
Two real failures came back from the first live run -- exactly the kind of thing local
verification can miss and only a real run surfaces.

1. **`Unable to resolve action astral-sh/setup-uv@v9, unable to find version v9`.**
   `astral-sh/setup-uv` turned out not to publish floating major-version tags (`v9`, `v10`)
   the way `actions/checkout` or `python-semantic-release` do -- only full versions like
   `v10.0.1`. Verified against the repo's actual `/tags` page before picking a replacement
   (`v10.0.1`) rather than guessing a second time, and spot-checked `actions/checkout@v7`,
   `python-semantic-release@v10.6.1`, and (later) `actions/setup-python@v7` the same way, since
   one wrong assumption was reason enough to stop trusting the others.
2. **The dry-run version computed `1.0.0`, not `0.1.0`.** Caught locally, before ever pushing
   -- ran `semantic-release version --print --noop` against the real two-commit history first,
   specifically because a version published to PyPI can never be reused even after deletion,
   and this seemed worth verifying rather than trusting. Root cause: `allow_zero_version`
   (whether the *first ever* release is allowed to land below `1.0.0` at all) is a distinct
   setting from `major_on_zero` (how *later* breaking changes behave once already on `0.x`) --
   the earlier phase had only set the latter. `allow_zero_version` defaults to `false`. Fixed
   by setting it explicitly to `true`, re-verified locally (now computes `0.1.0`), and folded
   in a second, unrelated fix noticed along the way: `changelog.changelog_file` is a deprecated
   config location, moved to `changelog.default_templates.changelog_file`.

Also, separately, the user asked a plain design question -- "does our test matrix need to
cover multiple Python versions?" -- noticing on their own that `classifiers` already claimed
3.12 *and* 3.13 support while the Hatch test matrix only ever ran 3.12, an unverified claim
sitting in already-shipped metadata. Added `python = ["3.12", "3.13"]` as a second axis in
`[[tool.hatch.envs.test.matrix]]` (crossed with the existing `dbt-engine` axis, 2x2 = 4 CI
jobs), and `actions/setup-python` per matrix leg in `ci.yml` to provision each interpreter --
not relying on Hatch or `uv` to auto-provision a missing one, since that behavior wasn't
verified and `actions/setup-python` is the well-established mechanism for this. The exact
Hatch-generated env names needed for the workflow's `hatch run test.<name>:run` step
(`test.py3.12-core`, `test.py3.13-fusion`, ...) were read directly off `hatch env show`'s real
output, not inferred from Hatch's naming convention secondhand.

### Verification

- `hatch env show` confirms the real matrix env names before wiring them into `ci.yml`.
- `hatch run test.py3.12-core:run` (the renamed env) actually executed locally --
  **95/95 passed** -- confirming the matrix restructuring (moving `python` out of the env's
  static config and into the matrix) didn't silently break the existing dbt-core leg.
  Python 3.13 isn't installed on this dev machine, so that leg is verified structurally
  (`hatch env show` lists it correctly) and will get its first real execution in CI.
- All workflow YAML re-validated (`yaml.safe_load`) after each edit.

## Phase 35 — a docs site: MkDocs + Material + mkdocstrings on Read the Docs

**User's request:** "The next step is to create static docs and host them on a docs site.
What would usually be the goto for a python library like this?" Recommended MkDocs + Material
+ mkdocstrings (the current default across most modern Python tooling -- FastAPI, Typer,
httpx, Ruff, uv itself) over Sphinx + Read the Docs (the older, more traditional default,
stronger autodoc but a rougher Markdown-authoring experience), and Read the Docs over GitHub
Pages as the host specifically because it handles multi-version docs (v0.1 vs v0.3 vs latest)
natively -- which now actually matters, given the pipeline built in Phase 33 is cutting real
versioned releases. User agreed to both.

Verified rather than guessed at every step, having already been burned once on an unverified
GitHub Action tag in Phase 34: fetched Read the Docs' own MkDocs guide and `python.install`
config-file reference for the exact `.readthedocs.yaml` shape (confirming `extra_requirements`
can reference a `pyproject.toml` extra directly, avoiding a second requirements file), and
`mkdocstrings`' own docs for its Python-handler package name, `paths` option, and `::: module.
Class` directive syntax.

### Decisions

- **One source of truth, not a fork.** `docs/index.md` and `docs/changelog.md` don't contain
  prose of their own -- they pull in `README.md`/`CHANGELOG.md` verbatim at build time via
  `pymdownx.snippets`'s block-include syntax, so the doc site can never drift out of sync with
  what GitHub/PyPI already show. Only `docs/reference.md` (API reference, generated from the
  library's own docstrings via `mkdocstrings`) has content that doesn't exist anywhere else.
- `mkdocs.yml`/`docs/` live at the **repo root**, not inside `python_modules/dagster-cube-dbt/`
  -- `mkdocstrings`' Python handler needs a `paths: [python_modules/dagster-cube-dbt/src]`
  entry regardless of where the config lives, and Read the Docs always looks for
  `.readthedocs.yaml` at the repo root anyway.
- Doc-build tooling (`mkdocs`, `mkdocs-material`, `mkdocstrings[python]`) added as a
  `[project.optional-dependencies] docs` **extra**, not a `[dependency-groups]` entry like the
  existing `dev` group -- deliberately, so Read the Docs' well-documented `extra_requirements`
  config option can reference it directly via a plain `pip install`, rather than depending on
  newer, less universally-supported PEP 735 dependency-group tooling in RTD's build image.
- Added a `docs` job to the shared `ci.yml` (`mkdocs build --strict`) so a broken link/anchor/
  snippet reference fails PRs the same way a failing test would, not just discovered later on
  a live RTD build.
- Found and fixed, empirically rather than by inspection: three of the library's own class
  docstrings used Sphinx/RST `.. code-block::` directives (leftover from before this session
  ever considered a Markdown-based doc tool) that `mkdocstrings` doesn't interpret --
  cross-referencing `component.py`/`resources.py` confirmed this wasn't isolated. Converted to
  plain indentation, *without* Markdown triple-backtick fences -- a first attempt using fences
  rendered literal ` ```python ` text inside an already-syntax-highlighted block, since
  `griffe`'s docstring parser treats an indented block following a `:`-terminated line as a
  verbatim code block on its own, regardless of Markdown fence syntax inside it. Caught by
  actually inspecting the rendered HTML output, not assumed correct from the diff.
- README also had four genuinely broken relative links (`../dagster-cube-dbt-tests/...`,
  `CHANGELOG.md`, `../../.github/workflows/release.yml`) and one heading-anchor link that
  didn't match Python-Markdown's actual generated slug (double- vs single-hyphen, for a
  heading with a `/` between two backtick-wrapped code spans) -- all pre-existing, silently
  correct-looking on GitHub's own README rendering, invisible until MkDocs' `--strict` mode
  had a reason to actually check them. Fixed the four links to absolute GitHub URLs (correct
  from both the raw README and the embedded doc-site copy); fixed the anchor to the slug MkDocs
  actually generates (verified by inspecting the built HTML's `id` attribute, not re-derived
  by hand a second time).
- Found and fixed a real naming bug in the CI setup itself while touching these workflows
  again: `release.yml`'s job calling the reusable `ci.yml` is named `test`, but `pr.yml`'s was
  named `ci` -- GitHub Actions prefixes a reusable workflow's check names with the *calling*
  job's name, so PR runs would have produced checks named `ci / test (py3.12, dbt-core)`,
  never matching the `test / test (py3.12, dbt-core)` names the user had just configured as
  required in their branch ruleset (pulled from `release.yml`'s own runs). Renamed `pr.yml`'s
  job to `test` to match.

### Verification

- `mkdocs build --strict` run locally from the repo root (matching where Read the Docs' own
  build executes from -- running it from the package subdirectory instead was tried first and
  failed, since `pymdownx.snippets`' `base_path` resolves against the *working directory*, not
  `mkdocs.yml`'s location) -- clean, zero warnings, after fixing the docstring/link/anchor
  issues above.
- Directly inspected the built HTML: confirmed 5 real `mkdocstrings`-rendered class sections on
  the reference page (not empty stubs), and confirmed the previously-broken code examples now
  render as genuine syntax-highlighted blocks with no literal fence-marker text leaking through.
- Full test suite re-run after the docstring edits (source files, not just docs) --
  **95/95 passed**.
- One-time, maintainer-only setup this can't complete unattended -- documented in the README's
  new "Documentation" section: importing the repo on Read the Docs (auto-detects
  `.readthedocs.yaml`), and confirming the resulting project slug matches the badge/`site_url`
  already written assuming `dagster-cube-dbt`.

## Phase 36 — a real design bug in `build_defs_from_state`: it silently needed the live dbt project

The user hit `dagster_dbt.errors.DagsterDbtProjectNotFoundError` deploying `CustomDbtProjectComponent`
(their own subclass of `CubeDbtProjectComponent`) to k8s, after replacing their original
`dagster_dbt.DbtProjectComponent` usage with it. The path in the error --
`.../.venv/lib/python3.12/site-packages/spatialytics_dbt` -- didn't exist.

**Misdiagnosis, corrected by the user directly.** Initial investigation traced the *mechanics*
of that specific path correctly (their own `[tool.hatch.build.targets.wheel] force-include`
hack for `pyproject.toml`, combined with a non-editable install, made `dg`'s
`discover_config_file` resolve `{{ context.project_root }}` to `site-packages` instead of the
real project root) and then a genuine Dockerfile gap (`spatialytics_dbt/` was never `COPY`'d in
at all) -- both real, both in `dagster-nz-gov-elt`, both correctly found. But the recommended
*fix* was wrong in kind: it pointed at the user's deployment config, when the actual defect
was in this library. The user pushed back directly: **"If your implementation of
dagster-cube-dbt has to rely on the project existing at run/deployment time, you have done
something wrong"** -- correctly citing `dagster_dbt`'s own documented state-backed design:
the manifest is meant to be built once (via `dg utils refresh-defs-state`) and shipped inside
`.local_defs_state/`; the live dbt project is never expected to exist at deploy time at all.

**Root cause, once actually verified against `dagster_dbt`'s source (not assumed):**
`DbtProjectComponent.build_defs_from_state` (the correct, state-aware path) calls
`self._project_manager.get_project(state_path)` -- but `DbtProjectComponent.dbt_project` (a
`@cached_property` used by `self.asset_key_for_model` and multiple other methods) always calls
`self._project_manager.get_project(None)` instead, which for a real (non-`DbtProject`-literal)
project config resolves straight to the *original* configured directory, completely bypassing
whatever `write_state_to_path` cached. `CubeDbtProjectComponent.build_defs_from_state` used
`self.dbt_project`/`self.asset_key_for_model` in **three separate places** -- resolving a
cube's dbt-model dependency (`_dbt_model_asset_key_or_none`), and naming the generated
multi-asset op/pool and the automation-condition sensor -- all of which would need the live
project directory to exist at deploy time, defeating the entire point of being state-backed.
Existing tests never caught this because every one of them constructs the component with a
pre-built `DbtProject` *instance* (routing through `NoopDbtProjectManager`, whose
`get_project(state_path)` and `get_project(None)` are always identical), never exercising the
real `DbtProjectArgsManager` used by an actual `project: "path"` YAML config.

### Decisions

- `build_defs_from_state` now computes a state-aware `project`/`manifest` itself
  (`self._project_manager.get_project(state_path)`, mirroring exactly what
  `super().build_defs_from_state` already does internally), stored as transient instance
  attributes (`_state_aware_project`/`_state_aware_manifest`, same lifecycle-scoped-to-one-
  build-call pattern this file already used for `_defs_dir`) rather than threaded as new
  parameters through `get_cube_asset_spec` -- that method is `@public` and documented as
  overridable with its current single-argument signature, so changing it would be an
  unnecessary breaking change for downstream subclasses like the user's.
- `_dbt_model_asset_key_or_none` now replicates `asset_key_for_model`'s exact lookup logic
  (same `ASSET_RESOURCE_TYPES` filter, same translator call) against this state-aware data,
  instead of calling the live method at all.
- The op/pool/sensor `name`s (previously `self.dbt_project.name`) now read
  `state_aware_project.name` instead -- the same class of bug, just for a project *name*
  rather than its manifest, caught only once the regression test below actually ran against
  the fix and still failed on this second, independent occurrence.

### Verification

- New `test_build_defs_from_state_does_not_need_the_live_dbt_project_directory` in
  `test_component_integration.py`, using a real `DbtProjectArgsManager` (not the
  `NoopDbtProjectManager` every other test in the file uses) against a *throwaway copy* of the
  fixture project -- refreshes state, deletes that copy entirely, then builds defs from state
  alone and asserts the cube's dependency on its source dbt model still resolves correctly.
- Iterated on the test itself twice before trusting it, each time by deliberately reverting
  the fix and confirming the test actually failed against the broken code -- not just that it
  passed against the fixed code, which two earlier versions of this same test did *without*
  actually exercising the bug: the first computed its expected-key assertion by calling the
  live `asset_key_for_model` on the same component instance before deleting the directory,
  silently warming that method's own cache; the second still shared one component instance
  between `write_state_to_path` and `build_defs_from_state`, letting `write_state_to_path`'s
  own legitimate `self.dbt_project` access (building cube data, where the live project
  correctly does exist) warm the same cache for the rest of that instance's life -- neither
  mirrored a real deployed process, which never calls `write_state_to_path` at all. The final
  version uses two separate component instances, confirmed to fail against the reverted code
  with the exact same `DagsterDbtProjectNotFoundError` the user hit in production, then pass
  against the fix.
- Full suite: **96/96 passed** (95 existing + the new regression test).

## Phase 37 — Phase 36's own fix broke `extends`-cube dependencies for a subclass renaming keys

Immediately after Phase 36, the user reported: "Seems your fix may have broken dependency
between cubes that extend a parent?" My first response defended the fix -- `git diff` showed
the `extends` branch of `get_cube_asset_spec` was byte-for-byte untouched -- and asked for a
concrete failing case rather than assuming either way. The user then diagnosed it directly:
**"The deps rely on the asset key not being updated during the `get_cube_asset_spec` function,
which I overrode. The original dagster-dbt package supports changing the asset key by
overriding `get_asset_spec` without breaking the deps chain. You should see how dagster-dbt
manages this."** Right again -- this was a real, pre-existing bug (not introduced by Phase 36,
just newly surfaced by the user actually deploying their renaming override against it), and
the fix required understanding a `dagster_dbt` mechanism this library had never replicated.

**Verified directly against `dagster_dbt`'s own source, not assumed:**
`DagsterDbtTranslator.get_asset_spec` computes a node's `deps` by *recursively* calling
`self.get_asset_spec(manifest, upstream_id, project)` for each upstream unique_id -- not a
separate, narrower key-only function. Combined with `create_component_translator_cls`'s shim
(`DbtProjectComponent.translator` wraps `self`, the component, in a translator whose
`get_asset_spec`/`get_asset_check_spec` delegate to the component's own method *if the
component's class has overridden it*), that recursive `self.get_asset_spec(...)` call resolves,
at runtime, to whatever the most-derived override is -- for both a node's own identity and how
every dependent references it. This is exactly why `dagster_dbt` supports the documented
pattern of overriding `get_asset_spec` wholesale (`super().get_asset_spec(...)
.replace_attributes(key=...)`) without breaking dependency edges: the override is never
bypassed, no matter which node the framework is currently resolving.

`CubeDbtProjectComponent.get_cube_asset_spec`'s `extends` branch never had this property: an
`extends`-parent's dependency key was computed via `self.asset_key_for_cube(parent_cube_name)`
-- a separate, narrow method that recomputes the *default* key from scratch, completely
bypassing whatever a subclass's `get_cube_asset_spec` override (renaming the key via
`replace_attributes`, `CustomDbtProjectComponent`'s exact pattern) would have produced for that
same cube. (The dbt-model-dependency path, `_dbt_model_asset_key_or_none`, was already fine --
it goes through `self.translator.get_asset_spec(...)`, which is the same
`create_component_translator_cls` shim `dagster_dbt` itself uses, so a subclass's *dbt-model*
key-renaming override was already respected there. Only the cube-to-cube edge was missing the
equivalent mechanism.)

### Decisions

- Added `_cube_asset_spec_by_name`: a memoized, `self.get_cube_asset_spec`-dispatched lookup
  (not `asset_key_for_cube` directly) -- mirroring `DagsterDbtTranslator.get_asset_spec`'s own
  `_resolved_specs` memoization exactly. `get_cube_asset_spec`'s `extends` branch now calls
  this instead of `asset_key_for_cube`, so a subclass override applies consistently whether a
  cube is being built for its own sake or looked up as another cube's `extends` target.
- All cube specs (not just `extends` targets) now get built through this same memoized path --
  `build_defs_from_state` first builds a `{name: augmented_cube_dict}` map, then resolves every
  top-level spec through `_cube_asset_spec_by_name` too, so a cube referenced both directly and
  as a dependency is only ever built once.
- No new cycle-guard needed: `resolve_extends` (called earlier in the same method) already
  raises `CircularExtendsError` for any `extends` cycle, and only runs successfully before
  `_cube_asset_spec_by_name`'s own recursion ever begins -- by the time it can run at all,
  every `extends` chain it might walk is already guaranteed acyclic.
- `get_cube_asset_spec`'s docstring updated to explicitly document this guarantee for future
  subclass authors, citing the exact `dagster_dbt` pattern it now mirrors.

### Verification

- New `test_get_cube_asset_spec_override_renaming_the_key_is_reflected_in_extends_deps`: a
  `RenamingComponent` subclass overriding `get_cube_asset_spec` with the user's exact pattern
  (`super().get_cube_asset_spec(cube).replace_attributes(key=...)`), asserting an `extends`
  child's `parent_keys` reflects the *renamed* parent key, not the un-renamed default.
- Confirmed the same way as Phase 36's test: reverted just the fix, watched this new test fail
  with the wrong (un-renamed) dependency key, restored the fix, watched it pass.
- Full suite: **97/97 passed** (96 + this new regression test).

## Phase 38 — `GENERATED_ASSET_AUTOMATION_CONDITION` fired on a cube's own definition change even while its dbt model dep was missing or in progress

The user reported: "I think we may have the logic for `GENERATED_ASSET_AUTOMATION_CONDITION`
wrong. We don't want it to fire if it has a new code version, but deps missing or deps in
progress? Right now if I update a cube definition and haven't ran the dbt model, the cube def
will start running right away, but the table it needs to actually work isn't even in the
database yet."

Confirmed by reading the condition (built back in Phase 7/8): the `missing()` branch was
correctly gated with `& ~any_deps_missing() & ~any_deps_in_progress()`, but the
`code_version_changed()` branch, OR'd in alongside it, had no such gate at all -- it fired
purely on the cube/view's own generated YAML hash changing, regardless of whether the dbt
model backing it had ever materialized or was mid-run. The block comment above it explained in
detail why `code_version_changed()` doesn't need the `newly_true()`/`since_last_handled()`
wrapping the `missing()` branch needs (it "stays true... until the tick it's actually
evaluated as part of a request," so it isn't lost like a raw edge-triggered condition would
be) -- but never actually applied a deps-readiness gate to it, and didn't claim to. That
omission is exactly the bug: editing a cube's definition (e.g. a merge-patch edit changing its
`title`) before its backing dbt model had ever run fired a request for the cube asset anyway,
against a table that doesn't exist yet.

### Decisions

- Factored the deps-ready gate (`~any_deps_missing() & ~any_deps_in_progress()`) out to
  `_DEPS_READY` and applied it to *both* branches: `(missing() & _DEPS_READY).newly_true()
  .since_last_handled()` as before, and now also `code_version_changed() & _DEPS_READY`.
- Applying the gate to `code_version_changed()` by a plain trailing `&` (not wrapped inside a
  `newly_true()` alongside it, the way `missing()` needed) is deliberate, not an oversight
  paralleling the earlier `missing()`-gate mistake documented in Phase 7/8's comment: the
  reason that trailing-AND pattern loses the transition for `missing()` is that `missing()`'s
  own `newly_true()` pulse is what's being tracked, and once that one-tick pulse is consumed
  (or blocked at exactly the wrong tick), it doesn't recur just because the deps gate later
  opens. `code_version_changed()` isn't pulsed that way -- per the existing comment, it holds
  true continuously until actually consumed by a request -- so a request blocked one tick by
  `_DEPS_READY` still fires the very next tick the gate opens, without needing the transition
  to be captured inside a wrap. This was verified against a real tick sequence, not assumed
  from the reasoning alone (see Verification).

### Verification

- New `test_generated_asset_automation_condition_does_not_fire_on_code_version_change_while_dep_missing`:
  edits a cube's definition (title merge-patch) while its dbt model has never materialized,
  asserts zero requests across several ticks with the dep still missing, then materializes the
  dbt model and asserts the cube is requested exactly once right after -- i.e. the pending
  `code_version_changed()` isn't lost while blocked, and fires as soon as the gate opens.
- Confirmed the same way as Phases 36-37: reverted just the fix (kept the new test), watched it
  fail (`1 == 0` at the tick where the definition changes, before the model has materialized --
  the exact bug the user reported), restored the fix, watched it pass.
- Full suite: **98/98 passed** (97 + this new regression test).

## Phase 39 -- `landing_check`: an optional post-promotion poll against Cube's own REST API

Scoped ahead of the planned Superset sync work (see `SUPERSET_SYNC_PLAN.md`) after the user
flagged a real gap: `promoter.promote()` returning success only means the generated cube/view
YAML was *handed off* -- whether a running Cube instance has actually picked it up (hot-reload,
Cube Cloud propagation, ...) is invisible to Dagster. A cube/view asset can show as
materialized in Dagster while still not being queryable in Cube yet. The user's own proposed
mechanism -- stamp each cube/view's `code_version` into its own metadata before promotion, then
poll Cube's API until it echoes that value back -- is exactly what got built, after verifying
two things against Cube's actual docs rather than assuming them:

- `GET /v1/meta` echoes a cube/view's custom `meta:` block verbatim (confirmed against Cube's
  REST API reference; also cross-checked a since-closed GitHub issue, `cube-js/cube#7740`,
  reporting this was *missing* in an older Cube version -- current docs show it present, but
  this means the feature has an implicit minimum-Cube-version assumption worth documenting, not
  something to treat as universally true).
- Cube's REST API auth is a bare token in the `Authorization` header, **not** a `Bearer <token>`
  scheme (confirmed via Cube's own auth docs and a GitHub issue discussing exactly this
  difference from convention) -- typically a JWT signed with `CUBEJS_API_SECRET`. The resource
  accepts a pre-built token string rather than signing one itself, deliberately: security-context
  claims requirements vary per deployment, and guessing a claims shape would repeat the same
  mistake this project has already been burned by twice (Phases 36-37) -- depending on assumed
  behavior instead of a documented, stable contract.

### Decisions

- `meta.dagster_cube_dbt.code_version` is the injection point -- namespaced to avoid colliding
  with whatever a user already put in a cube's `meta` via `meta.cube.meta` (an existing,
  pre-Phase-39 pass-through), merged in rather than overwritten (`with_landing_check_meta`).
- The stamped value is `AssetSpec.code_version` itself (already computed once, via
  `_code_version`, for each cube/view's `AssetSpec`) -- not a fresh hash of the raw
  promoted-YAML dict. Those two would actually differ: the `AssetSpec`'s hash is computed over
  the `extends`-*resolved* dict (so an ancestor's field change still bumps a child's
  `code_version`, per Phase 18/Phase 37's design), while the promoted YAML keeps `extends:`
  literal for Cube to resolve itself. Using the same value Dagster already shows for that
  asset's `code_version` means what Cube's meta panel displays and what Dagster's UI displays
  are always the same number -- one canonical source of truth, not two hashes of different
  content that happen to usually agree.
- Off by default (`landing_check: CubeLandingCheck | None = None`) -- it needs Cube API
  credentials and adds latency to every promotion, and plenty of deployments don't need it.
  When unset, promoted YAML is byte-identical to pre-Phase-39 output (verified by a dedicated
  regression test) -- no injected `meta.dagster_cube_dbt` key at all, not even an empty one.
- Polling is scoped to the assets *actually selected* for the run, not every generated
  cube/view (`promoter.promote` itself still ships the full generated set, unchanged) -- matches
  what the op yields `MaterializeResult`s for.
- On timeout: fails the run outright (`dg.Failure`, raised before any `MaterializeResult` is
  yielded -- the same contract `CubeFilePromoter.promote` already documents for its own
  failures), naming exactly which cube(s)/view(s) never landed. Rejected logging a warning and
  materializing anyway -- that would silently reintroduce the exact problem this feature exists
  to close. Since a failed run leaves the asset's `code_version` unchanged, the next automation
  evaluation just retries the whole promote-then-poll cycle -- no special retry bookkeeping
  needed, `code_version_changed()`'s own persistence (Phase 38) already covers it.
- `requests` is now a base (not optional-extra) dependency -- unlike the still-hypothetical
  Superset integration, every user of this library already necessarily talks to a Cube
  deployment, so gating basic HTTP access to *that same* deployment behind an extra buys
  little. Confirmed via `uv.lock` that this added zero new packages -- `requests` was already
  present transitively (through `dagster`/`dagster-dbt`), just not previously a direct
  dependency.
- `CubeApiClient` is an abstract base (like `CubeFilePromoter`) with `CubeRestApiClient` as the
  one concrete implementation, rather than a single concrete class -- keeps a seam for a test
  double (`fetch_meta` is trivial to fake) and for a future non-REST way of checking Cube's
  state, without committing to needing one yet.

### Verification

- New `tests/test_landing_check.py` (6 tests): `with_landing_check_meta`'s merge-not-overwrite
  behavior (including on an entity with no prior `meta`), `CubeRestApiClient.fetch_meta`'s
  request shape (bare token header, trailing-slash-stripped URL) against a mocked
  `requests.get`, and `wait_for_landing`'s polling loop (returns once all entities match;
  raises `dg.Failure` naming only the still-pending ones on timeout).
- New tests in `test_component_integration.py`: `test_landing_check_disabled_by_default_...`
  (promoted YAML has no injected key when the feature is off), `test_landing_check_stamps_...`
  (promoted YAML carries the exact `AssetSpec.code_version`; a fake `CubeApiClient` returning a
  stale value on its first call and the matching one on its second proves this isn't a
  single-poll implementation), `test_landing_check_timeout_fails_the_run_...` (no
  `MaterializeResult` on timeout).
- Confirmed both integration tests actually catch real bugs, the same way as every prior phase:
  temporarily disabled the meta-injection branch alone (`test_landing_check_stamps_...` failed,
  `test_landing_check_disabled_by_default_...` still correctly passed -- proving it isn't just
  testing "the feature is off"), restored it, then temporarily disabled the polling branch alone
  (`test_landing_check_timeout_...` failed as expected), restored it.
- Full suite: **107/107 passed** (98 + 3 integration + 6 unit).

## Phase 40 -- `landing_check` broke under a subclass renaming the last key path segment (real production bug, first real usage of the feature)

Found immediately on the first real deployment of Phase 39's `landing_check`, via the same
production project (`nz-data-exploration/dagster-nz-gov-elt`) that surfaced Phases 36 and 37:
`KeyError: 'geographic_areas'` inside `_cube_assets`, at
`code_version_by_name[cube["name"]]`. The user's `CustomDbtProjectComponent.get_cube_asset_spec`
override computes a wholly new key from a `group`/`name` pair --
`dg.AssetKey(["cube", group, f"{name}_cube"])` -- rather than prepending to the default key the
way every prior test's renaming override did (Phase 37's `RenamingComponent`:
`AssetKey(["renamed", *base_spec.key.path])`).

Phase 39's `code_version_by_name` was built as `{spec.key.path[-1]: spec.code_version for spec
in specs}`, on the documented assumption that "a cube/view's own name is always its asset key's
last path segment... regardless of any subclass renaming the rest of the key." That assumption
is simply false for this (entirely reasonable) override shape: the last segment is
`f"{name}_cube"`, not `name`, so the lookup at `code_version_by_name[cube["name"]]` (using the
cube dict's real, un-renamed `name` field) never finds a matching key. The equivalent `expected
= {spec.key.path[-1]: spec.code_version for spec in specs if ...}` used for polling had the
identical bug one level down -- it would have built an `expected` dict keyed by
`"geographic_areas_cube"`, which would never match Cube's own `/v1/meta` response (Cube only
ever knows the entity by its real `name`, `"geographic_areas"`), so even past the `KeyError` the
polling step would have silently timed out on every run for a project using this override shape.

This is the exact class of mistake [[feedback_override_safe_deps]] already documents from
Phase 37 -- reintroduced in brand new code within the same session that memory was written,
because the new code derived an entity's identity by *parsing a subclass-computed value*
(`spec.key`) instead of carrying the real identity (the cube/view dict's own `name` field)
through directly. Phase 37's fix pattern (resolve dependency keys by recursively calling the
same overridable method) doesn't directly apply here -- this isn't a dependency-key lookup, it's
matching a Dagster `AssetSpec` back to the raw generated dict it came from -- but the underlying
principle is the same: never assume a shape or invariant about a value a subclass override is
free to change.

### Decisions

- `code_version_by_name` and its inverse, `name_by_key` (`AssetKey -> name`, needed to recover
  a selected entity's real name from `context.selected_asset_keys`, which only has keys), are
  now built by **positional pairing** with `cube_names`/`views` -- `cube_specs = [self.
  _cube_asset_spec_by_name(name) for name in cube_names]` and `view_specs = [self.
  get_view_asset_spec(view) for view in views]` are each built in the same order as their
  corresponding name list, then zipped together. Neither dict is ever built by inspecting
  `spec.key` to guess an entity's name -- the real `name` always comes from the cube/view dict
  itself, which no key-renaming override can touch without also changing `cubes`/`views`.

### Verification

- New `test_landing_check_works_when_a_subclass_renames_the_keys_last_path_segment`: a
  `RenamingComponent` whose `get_cube_asset_spec` override matches the user's real pattern
  (`key=AssetKey(f"{cube['name']}_cube")`), with `landing_check` configured, materializing the
  renamed cube asset and asserting the promoted YAML's stamped `code_version` matches the
  `AssetSpec`'s.
- Confirmed the same way as every prior phase: reverted just the fix (kept the new test),
  watched it reproduce the exact reported `KeyError`, restored the fix, watched it pass.
- Full suite: **108/108 passed** (107 + this new regression test).

## Phase 41 -- `CubeRestApiClient.verify_tls`: an escape hatch for self-signed/internal-CA certs

Small, user-requested addition: a `verify_tls: bool = True` field on `CubeRestApiClient`, passed
straight through as `requests.get(..., verify=self.verify_tls)`. Needed for deployments the
resource can't otherwise reach with a certificate that validates against the system trust
store (a self-hosted Cube instance behind a self-signed or internal-CA cert being the obvious
case). Defaults to `True` (verify, matching `requests`' own default) -- opting out is explicit,
not something a project falls into by omission.

### Verification

- New unit tests in `test_landing_check.py`: `verify_tls` defaults to `True` and is passed as
  `verify=True` when unset; setting it `False` passes `verify=False` through to the mocked
  `requests.get` call. Existing request-shape test updated to expect the new `verify=True`
  kwarg alongside the others.
- `mkdocs build --strict` clean after documenting it in the README's landing-check section.
- Full suite: **110/110 passed** (108 + 2 new unit tests).

## Phase 42 -- `CubeSupersetSyncComponent` and `SupersetResource`: the Superset dataset sync

The feature scoped in `SUPERSET_SYNC_PLAN.md` (written up ahead of implementation, after Phase
39 deliberately deferred it in favor of `landing_check` first). Implemented in stages, each
committed separately: extract `cube_state.py` (pure refactor, full suite unchanged before
anything Superset-specific landed), `SupersetResource`, `CubeSupersetSyncComponent`, docs.

### Decisions

- **Component chaining, confirmed working exactly as the plan sketched it**:
  `context.load_component(self.dbt_cube_component, CubeDbtProjectComponent)` plus
  `DefinitionsLoadContext.get().state_path(sibling.defs_state_config, DefsStateStorage.get(),
  context.project_root)` (a context manager, not a plain call -- state-reading has to happen
  inside the `with` block, since `VERSIONED_STATE_STORAGE`/`LEGACY_CODE_SERVER_SNAPSHOTS` state
  paths live in a `TemporaryDirectory` that's cleaned up on exit). One thing the plan didn't
  anticipate, verified only by reading the installed `dagster` package directly: neither
  `DefinitionsLoadContext` nor `DefsStateStorage` is re-exported from `dagster`'s public
  surface (`dagster`/`dagster.components`) as of the pinned `dagster~=1.13.0` -- imported from
  their real, private-module locations (`dagster._core.definitions.definitions_load_context`,
  `dagster._core.storage.defs_state.base`) instead, the same modules
  `StateBackedComponent.build_defs` itself imports internally to implement this exact contract.
  Fragile against a future dagster release moving those modules, but there's no public
  alternative to depend on instead, and it's literally the base class's own real implementation,
  not a guess at one.
- **No `[superset]` extra** -- the plan's original rationale (keep `requests` out of the base
  install) was written before Phase 39 made `requests` a base dependency for `landing_check`;
  Phase 39's own writeup already flagged this as a "still-hypothetical" future non-issue for
  Superset. Confirmed via `uv sync` (no extras) needing zero new packages beyond what Phase 39
  already added.
- **Revisited: `CubeSupersetSyncComponent`/`SupersetResource` *are* re-exported from the
  top-level `dagster_cube_dbt` module after all**, reversing this phase's initial decision to
  keep them out. Caught by the user, testing this branch against a real consuming project via a
  git dependency, asking why a full internal dotted path (`dagster_cube_dbt.components.
  cube_superset_sync.component.CubeSupersetSyncComponent`) was needed in `defs.yaml` when every
  other public symbol here is a plain top-level import. The original "keep the module's import
  surface minimal" reasoning didn't actually hold once traced through: the only thing that
  surface was ever protecting against was a hard `requests` dependency, and that protection had
  already evaporated one bullet above, at the moment `SupersetResource` was written -- it just
  wasn't followed through to this decision too. Left as originally written, it bought worse
  ergonomics (an undiscoverable internal path in every `defs.yaml`) for zero remaining benefit.
  Lesson: when a plan's stated rationale depends on a precondition, and a later decision in the
  same phase invalidates that precondition, revisit every earlier decision that rationale fed
  into -- not just the one bullet that happened to restate it.
- **`SupersetResource`'s request/response shapes verified against a real, working reference
  implementation** (`ponderedw/dbt-to-cube`'s `SupersetConnector`), fetched and read directly
  (both via `WebFetch` and a raw `curl`, to see the literal source rather than a paraphrase) --
  not guessed from Superset's own REST API reference docs alone, the same standard Phase 39 set
  for the Cube-side API. Two deliberate deviations from that reference, not blind copying:
  - It sleeps a fixed 2 seconds after `PUT .../refresh` and hopes columns are populated by then.
    `SupersetResource` polls instead (bounded by `refresh_timeout_seconds`/
    `refresh_poll_interval_seconds`), mirroring the "poll, don't guess a sleep" shape
    `landing_check.wait_for_landing` already established for the Cube-side propagation problem
    -- directly resolving the plan's own "Open questions" note that flagged the fixed sleep as
    something to fix, not leave as a TODO.
  - The reference computes a Cube-dimension-type -> Superset-SQL-type mapping table but, reading
    closely, never actually sends it in the column-update payload -- Superset's own table
    introspection is authoritative for a column's real SQL type after `refresh`, so there was
    nothing for this resource to override there either. Dropped the mapping table entirely
    rather than keeping unused code around "for documentation."
- **No real Superset instance in CI** (same constraint `landing_check` has for Cube) --
  `SupersetResource` is unit-tested against a scripted fake `requests.Session` (swap the
  private `_session` `PrivateAttr` for a `MagicMock` with a `side_effect` list per method,
  mirroring `test_landing_check.py`'s scripted-response style). This means the exact Rison/JSON
  `q`-param encoding, the `rel_o_m` filter operator, and the read-only-field-stripping list are
  only as trustworthy as the reference implementation they came from -- real, working code, but
  unverified against a live Superset instance by this project itself. Worth flagging to a user
  trying this feature for the first time, not something to claim more confidence in than earned.
- **View-member resolution (`_resolve_view_members`)**: `generate_cubes` never produces views at
  all -- only cubes; views are entirely hand-authored via merge patches, referencing member
  cubes through `cubes: [{join_path, includes, excludes}]`. Superset needs the *resolved* column
  list, so this walks that same declaration against the sibling's `resolve_extends`-flattened
  cubes, supporting `includes: "*"`, an explicit `includes` list, and `excludes`. Cube's
  `prefix`/member-aliasing option isn't handled -- not exercised by this project's own
  generation output or any fixture/production case so far; a view using it will under-resolve.
  `join_path.split(".")[0]` (a multi-hop join path's first segment, not necessarily the actual
  target cube for a two-hop join) deliberately mirrors `get_view_asset_spec`'s own existing
  single-hop assumption for the same lookup, rather than inventing separate -- and possibly
  diverging -- multi-hop handling that the rest of this codebase doesn't have either.
- **Dependency and `code_version` come from the sibling's own `get_view_asset_spec(view)`**, not
  `asset_key_for_view(name)` plus a freshly recomputed hash -- the exact override-safe pattern
  `get_cube_asset_spec`'s own docstring documents (Phase 37/40's lesson: never derive identity
  from a guessed `AssetKey` shape, always go through the same overridable method). Also means a
  subclass override of `get_view_asset_spec` that changes what "the view's definition" even
  means (e.g. folding in extra metadata) is automatically reflected in when the Superset dataset
  re-syncs, with no extra work.
- **`build_defs` split into `build_defs_from_sibling_state`** (mirroring
  `CubeDbtProjectComponent.write_state_to_path`/`build_defs_from_state`'s own split) --
  deliberately, so tests can exercise the real spec/deps/op logic against a directly-constructed
  sibling instance and state path, the same way every existing test in this codebase already
  tests `CubeDbtProjectComponent` (none of them go through `context.load_component` or a real
  on-disk defs tree either). **Known, deliberate coverage gap**: `context.load_component` itself
  and the `state_path()` context manager are not exercised end-to-end by this project's test
  suite -- standing up a real `ComponentTree`/on-disk defs tree for that (`ComponentTree.
  for_project`/`from_module`, real `defs.yaml` files, an importable temp package) is
  meaningfully heavier machinery than this codebase's testing style has ever used, and the
  higher-value, project-specific risk (state-backed-ness, override-safety) is fully covered
  without it. `context.load_component`/`state_path()` are dagster's own public/quasi-public
  API, not something this project needs to re-verify to this depth.

### Verification

- New `tests/test_superset_resource.py` (9 tests): full create/reuse-existing dataset flows
  against the scripted session, database-not-found raising `dg.Failure`, session/login reuse
  across multiple `sync_dataset` calls, read-only-field stripping, case-insensitive column
  matching, the refresh-poll loop actually polling (not just accepting an empty first response),
  and `_verbose_name`'s title-vs-generated-name fallback.
- New `tests/test_cube_superset_sync_component.py` (11 tests), including the two chaining-shaped
  regression tests the plan called for: `test_build_defs_from_sibling_state_does_not_need_the_
  live_dbt_project_directory` (real `DbtProjectArgsManager` pointed at a throwaway copy of the
  fixture project, deleted before defs are built -- mirrors Phase 36's own test exactly) and
  `test_a_subclass_renaming_the_view_key_is_reflected_in_the_dataset_deps` (mirrors Phase 37/40's
  `RenamingComponent` pattern, overriding `get_view_asset_spec` instead of `get_cube_asset_spec`).
  Plus a real `dg.materialize` test proving the multi-asset op actually calls `sync_dataset` with
  the right `table_name`/`schema`/resolved dimension and measure names, and direct unit tests of
  `_resolve_view_members`'s `"*"`/list-`includes`/`excludes`/unknown-cube/dotted-join-path
  behavior against small hand-built dicts (not coupled to the shared fixture project's exact
  shape).
- Full suite: **130/130 passed** (110 + 9 resource unit tests + 11 component tests).

## Phase 43 -- documenting the Superset-side database connection setup

The README's Superset section covered `CubeSupersetSyncComponent`/`SupersetResource` config but,
by design, treated "a Superset database connection pointed at Cube's SQL API already exists" as
a bare prerequisite -- same as `CubeFilePromoter`'s docs treat "a running Cube instance" as
assumed. The user asked for the walkthrough anyway, correctly judging that Superset's own
connection-string format for Cube's SQL API isn't something most users would already know
(unlike "a Cube instance exists," which is self-evident to anyone using this library at all).

Verified against Cube's own docs before writing anything down (`docs.cube.dev/reference/
core-data-apis/sql-api`, fetched directly), not assumed from general Postgres-driver knowledge:
`CUBEJS_PG_SQL_PORT` enables the SQL API on self-hosted Cube Core (off by default; Cube Cloud
has it on already, port `5432`); auth is `CUBEJS_SQL_USER`/`CUBEJS_SQL_PASSWORD` (or a custom
`checkSqlAuth`); the wire protocol is real Postgres, so Superset's own PostgreSQL engine
connects directly, and the SQL-level "database" name in the connection string is an arbitrary
string (`cube` is the convention, matching Cube's own example). One genuinely non-obvious
gotcha worth calling out explicitly in the new section: `CubeSupersetSyncComponent.database_name`
is looked up by the **Superset connection's own display name** (via Superset's REST API,
`GET /api/v1/database/?q={"filters":[{"col":"database_name","opr":"eq","value":...}]}`) -- a
completely different string from the SQL-level database name in the connection URI. Conflating
the two would be an easy first-time mistake; naming the Superset connection exactly `"Cube"`
(the component's own default) sidesteps needing to think about it at all.

### Verification

- `mkdocs build --strict` clean.
- No code changes -- README-only addition, so no test suite re-run needed beyond the existing
  130/130 baseline.
## Phase 44 -- component-managed resources for `landing_check` and `CubeSupersetSyncComponent`

The user asked a design question after testing the Superset sync feature: `CubeFilePromoter`
being externally-bound (a resource the user constructs and binds under a key, rather than the
component building it from config) was a deliberate choice -- but `CubeApiClient`/
`CubeRestApiClient` (`landing_check`) and `SupersetResource` don't share the reason that choice
was made for `CubeFilePromoter` (a genuine `ABC` with several real destination-specific
implementations, needing runtime credentials the component author can't anticipate), so should
they be handled differently?

### Decision

Yes -- confirmed against real precedent, not just general Dagster style, before agreeing: `dagster_dbt.DbtProjectComponent` itself constructs its own `DbtCliResource(project)` directly
from its own attributes, rather than requiring one bound externally -- the base class this
project already extends already does exactly what was proposed. Dagster's own `Component`
docstring's canonical example (`DatabaseTableComponent`) does the same (`database_url: str` as
a plain attribute). The real distinguishing question is "genuine extension point, or config for
one canonical implementation?" -- `CubeFilePromoter` is the former, `CubeApiClient`/
`SupersetResource` are the latter (the former's `ABC`-ness exists mainly as a testing seam, not
user-facing pluggability, per Phase 39).

Implemented for both, with a single rule the user specified: if the inline connection fields
are set (`landing_check.api_url`, `CubeSupersetSyncComponent.base_url`), the component builds
and owns the resource itself; if not, it falls back to the existing external-resource-by-key
behavior, unchanged -- a fully backwards-compatible, additive change (no `BREAKING CHANGE`
footer needed, no forced major/minor bump beyond a normal `feat:`).

- `CubeLandingCheck` gained `api_url`/`api_token`/`verify_tls` fields and a
  `build_managed_client()` method: returns `None` (fall back to `resource_key`) if `api_url`
  isn't set; builds a `CubeRestApiClient` directly if both `api_url` and `api_token` are set;
  raises a clear `DagsterInvalidDefinitionError` (not a confusing raw `pydantic` error) if
  `api_url` is set but `api_token` isn't -- that combination unambiguously means the managed
  path was intended but left incomplete, not that the external-resource path was meant instead.
- `CubeSupersetSyncComponent` gained the mirrored `base_url`/`username`/`password` fields and
  `build_managed_resource()`, same shape: `None` without `base_url`, a built `SupersetResource`
  with all three set, a clear error if `base_url` is set but `username`/`password` aren't.
- In both components' op-building code, `required_resource_keys` now conditionally excludes the
  `resource_key`/`superset_resource_key` entry whenever the managed path is in play -- otherwise
  every managed-mode project would still have to bind a pointless resource under a key nothing
  actually reads, defeating the point. Verified this actually matters via revert-and-confirm-
  fail (see Verification).
- `resource_key`/`superset_resource_key` **keep their current defaults** (`"cube_api_client"`/
  `"superset"`) for now, deliberately -- the user's own stated forward plan: a future release
  removes those defaults, forcing anyone using the external-resource path to set the key
  explicitly. Once that lands, an incomplete config with **no** managed-path fields set and
  **no** explicit resource key becomes unambiguous: the user must have intended the managed
  path (since external mode now requires an explicit opt-in key) but didn't finish configuring
  it, so the right error becomes "missing configuration," not "missing resource." Not
  implemented yet -- deliberately deferred, since removing a default *is* a breaking change
  (unlike everything else in this phase), and doing it now would force exactly the kind of
  semver bump this phase was scoped to avoid.

### Verification

- New tests in `test_component_integration.py` (4): `CubeLandingCheck.build_managed_client`
  returns `None`/builds a real `CubeRestApiClient`/raises clearly when `api_token` is missing,
  and an integration test proving the multi-asset op materializes successfully with `api_url`/
  `api_token` set and **no** `cube_api_client` resource bound at all (mocking `requests.get`,
  same pattern `test_landing_check.py` already uses).
- New tests in `test_cube_superset_sync_component.py` (4): the mirrored `build_managed_resource`
  unit tests, plus an integration test proving the dataset-sync op materializes successfully
  with `base_url`/`username`/`password` set and **no** `superset` resource bound at all
  (patching `SupersetResource.sync_dataset` directly, since the manually-constructed instance
  isn't reachable through `context.resources`).
- Both new "needs no external resource bound" tests confirmed to actually catch a regression:
  temporarily made `required_resource_keys` unconditional again in each component, watched both
  new tests fail with Dagster's own "resource ... was not provided" error, then restored the
  fix.
- Every existing test using the external-resource path (`resource_key`/`superset_resource_key`,
  no inline fields set) passes unchanged -- confirms this is additive, not a behavior change for
  anyone already using the library.
- README updated for both features: two side-by-side `defs.yaml` examples (managed vs
  external) per feature, plus a note that the resource-key defaults are staying for backwards
  compatibility only for now.
- `mkdocs build --strict` clean.
- Full suite: **138/138 passed** (130 + 4 landing_check + 4 Superset sync).

## Phase 45 -- two real production bugs in `get_view_asset_spec`'s dependency resolution, and a docs bug in `dbt_cube_component`'s path semantics

Both found by the user via real usage: their first hand-authored view (multi-cube joins off one
fact cube) came back with no upstream dependencies at all, and a git-installed test of the
Superset sync component against a real consuming project showed `dbt_cube_component: "../dbt_ingest"`
(this project's own documented example) failing to resolve, while the same value without `../`
worked.

### The path-resolution docs bug

`context.load_component`'s relative-path resolution is anchored to `context.defs_module_path`
-- confirmed by reading `ComponentLoadContext.load_component`'s real source (`ComponentPath.
from_resolvable(self.defs_module_path, defs_path)`) and by reproducing both forms directly
against the `dg`-runnable example project (`python_modules/dagster-cube-dbt-tests/`): a sibling
directory (`defs/dbt_ingest/` next to `defs/superset_sync/`) resolves as `"dbt_ingest"`, not
`"../dbt_ingest"` -- the latter literally looks one level *above* the top-level `defs/`
directory and fails with "No component found for loc ...defs/../dbt_ingest". `defs_module_path`
is the whole tree's root, fixed for every component regardless of its own position in the tree
-- not the calling component's own directory, which is what the original `"../dbt_ingest"`
examples (written before this was ever tested against a real multi-component project) assumed.
Fixed everywhere this appeared: the component's own docstring/field description, README.md
(both `defs.yaml` examples plus the attributes table), and `SUPERSET_SYNC_PLAN.md` (left with a
note explaining the correction rather than silently rewritten, matching this doc's own
practice elsewhere).

### The dependency-resolution bugs

Both bugs live in `CubeDbtProjectComponent.get_view_asset_spec`'s `deps` computation (and were
mirrored into `cube_superset_sync/component.py`'s `_resolve_view_members`, which was written to
deliberately match this method's behavior -- Phase 42's own writeup flagged the single-hop
assumption as a known limitation, not realizing it was actually just wrong):

- **Multi-hop `join_path` collapsed onto the wrong cube.** A view member's `join_path` is a
  dot-separated chain of cube names (e.g. `"route_calculated_fct.dates"`, Cube's own syntax for
  "reach `dates`'s members by joining through `route_calculated_fct`") -- the cube that entry's
  `includes`/`excludes` actually apply to is the *last* segment, not the first. The old code
  took the first segment (`str(member["join_path"]).split(".")[0]`), so a view with several
  members chained off one fact cube (the user's real `auckland_commutes` view: four `cubes:`
  entries, one bare `route_calculated_fct` and three two-hop ones reaching `dates`/`times`/
  `routes`) collapsed all four onto the single name `"route_calculated_fct"` -- silently
  dropping the dependency on every dimension cube. Not a single-hop "limitation" as Phase 42
  described it; a straightforwardly wrong assumption about which segment of the path is the
  actual member cube, caught the first time a real project used a multi-hop join_path at all
  (this project's own fixture data only ever exercised single-segment paths).
- **Override-unsafe key derivation** -- the exact class of bug DECISIONS.md Phase 37/40 already
  fixed twice for cube-to-cube (`extends`) and `landing_check` name lookups, but never applied
  to a view's dependency on its *member cubes*: `deps=[self.asset_key_for_cube(member_name)
  ...]` called `asset_key_for_cube` directly -- the un-renamed default key shape -- instead of
  resolving through `self._cube_asset_spec_by_name`/`get_cube_asset_spec`, the one overridable,
  memoized path every other cube-identity lookup in this file already goes through. The user's
  own real `CustomDbtProjectComponent` overrides `get_cube_asset_spec` to compute a completely
  different key (`AssetKey(["cube", group, f"{name}_cube"])`) -- so even with the multi-hop bug
  fixed, the view's `deps` would still have pointed at asset keys that don't exist anywhere in
  their real graph, which is consistent with "no upstream dependencies" being what they actually
  observed (a dependency on a nonexistent key doesn't show up as a real edge to anything).

### Decision

Fixed both in one pass, since they compound in the user's real case (multi-hop *and* a renamed
key): `member_cube_names` now takes `split(".")[-1]`; each dependency is resolved via
`self._cube_asset_spec_by_name(member_name).key`, with a clear `DagsterInvalidDefinitionError`
(naming the view and the missing cube) if a `join_path` segment doesn't match any generated
cube, rather than a bare `KeyError`. `_resolve_view_members` in `cube_superset_sync/component.py`
got the same one-line fix (first segment -> last segment) and an updated docstring pointing at
`get_view_asset_spec`'s own docstring for the full story, rather than re-explaining it.

**Second regression, caught by the test suite immediately after the fix above**: resolving a
view's cube dependency through `self._cube_asset_spec_by_name` requires `self.
_cube_dicts_by_name`/`self._cube_spec_cache` (and, transitively, `self._state_aware_manifest`/
`self._state_aware_project` for a cube with no `extends` parent) -- all previously populated
only inline inside `build_defs_from_state`. `CubeSupersetSyncComponent.build_defs_from_sibling_
state` calls `sibling.get_view_asset_spec(view)` directly on a sibling obtained via `context.
load_component` (Phase 42) or, in tests, a bare constructor -- neither path ever calls that
sibling's own `build_defs_from_state`, so none of that state existed on it, and every test in
`test_cube_superset_sync_component.py` broke with an `AttributeError` the moment the fix above
landed. This wasn't a test-only artifact: `context.load_component` genuinely only loads/resolves
a component (runs `Component.load()`), it does not build its defs -- a real project using
`CubeSupersetSyncComponent` would have hit the exact same `AttributeError` in production.
Fixed by extracting the whole "populate cube identity/dependency lookup from cached state"
block out of `build_defs_from_state` into a new method, `prepare_state_aware_lookup(state_path)`
-- callable on any `CubeDbtProjectComponent` instance regardless of whether its own
`build_defs_from_state` has run, returning the parsed `cube_dbt_state.json` content so callers
don't re-read it. `CubeSupersetSyncComponent` now calls `sibling.prepare_state_aware_lookup(
state_path)` before touching `sibling.get_view_asset_spec`; `build_defs_from_state` calls the
same method for its own defs, unchanged in behavior. A good example of why testing the actual
cross-component chaining path (not just each component in isolation) matters -- this would not
have been caught by `test_component_integration.py` alone, since nothing there ever calls
`get_view_asset_spec` on a component that skipped `build_defs_from_state`.

### Verification

- Two new regression tests in `test_component_integration.py`: a multi-hop `join_path`
  (`"journey_samples.dates"`, added to the existing fixture's `journeys_overview` view by
  rewriting cached state, the same technique Phase 37/40's tests use) resolves a dependency on
  `dates`, not just `journey_samples`; and a `RenamingComponent` overriding `get_cube_asset_spec`
  (mirroring Phase 40's exact override shape) shows the view depending on the *renamed* cube
  keys, not the defaults.
- Updated `test_resolve_view_members_uses_only_the_first_segment_of_a_dotted_join_path` (Phase
  42's own test, which had encoded the bug as if it were an intentional limitation) into
  `test_resolve_view_members_uses_the_last_segment_of_a_dotted_join_path`, asserting the correct
  behavior instead.
- Both new integration tests, and the updated unit test, confirmed to actually catch the
  regression: reverted each fix in turn, watched the corresponding test(s) fail with a clear
  diff (wrong/missing asset keys), restored the fix.
- Path-resolution behavior confirmed directly against `python_modules/dagster-cube-dbt-tests/`
  (a real `dg check defs` run, not just read from source): `dbt_cube_component: "dbt_cubes"`
  (sibling directory, no `../`) validates cleanly; `"../dbt_cubes"` fails with "No component
  found for loc ...defs/../dbt_cubes".
- `mkdocs build --strict` clean.
- Full suite: **140/140 passed** (138 + 2 new regression tests).

## Phase 46 -- `SupersetResource.verify_tls`, mirroring `CubeRestApiClient`'s escape hatch

User-requested, small: the user was "having trouble" with TLS on their Superset deployment and
asked for the same escape hatch `CubeRestApiClient.verify_tls` (Phase 41) already has, surfaced
on `CubeSupersetSyncComponent`'s component-managed config path too.

### Decisions

- Named `verify_tls`, not the user's original suggestion `ignore_tls` -- matches
  `CubeRestApiClient.verify_tls` exactly (same name, same `bool = True` default, same
  `requests`-`verify=`-passthrough semantics), so a project mixing both resources doesn't have
  to remember two different names/polarities for the same concept. Flagged this substitution
  rather than silently doing it.
- Applied once, on the `requests.Session` itself (`self._session.verify = self.verify_tls`,
  set at the top of `_ensure_authenticated`, which every `sync_dataset` call already runs
  first), rather than passed as a `verify=` kwarg on each of the several `self._session.*`
  calls scattered across the login/CSRF/find/create/refresh/update flow. `requests.Session.verify`
  applies to every request made through that session unless a call overrides it, so this is
  both less code and impossible to forget on some future new call site.
- Surfaced on `CubeSupersetSyncComponent` as a plain `verify_tls: bool = True` field, passed
  straight through in `build_managed_resource()` -- only meaningful on the component-managed
  path (`base_url` set); the external-resource path already lets a user set `verify_tls`
  directly when constructing their own bound `SupersetResource`, same as always.

### Verification

- Two new tests in `test_superset_resource.py`: `verify_tls` defaults to `True` on the session;
  `verify_tls=False` sets `session.verify = False`. Confirmed via revert-and-confirm-fail
  (temporarily no-opped the assignment, watched both fail, restored it).
- One new test in `test_cube_superset_sync_component.py`: `build_managed_resource` passes
  `verify_tls` through to the constructed `SupersetResource`.
- `mkdocs build --strict` clean.
- Full suite: **143/143 passed** (140 + 3 new tests).

## Phase 47 -- `landing_check` retries through a failing `/meta` poll instead of failing on the first one; `CubeApiClient` ABC removed

The user reported a real production scenario: a `git-sync`-style sidecar loads a bad cube/view
file, Cube starts serving `5xx` errors for its own schema until a fix propagates -- and the
window right after that fix actually lands is exactly when `landing_check`'s poll tends to be
running. `wait_for_landing` called `client.fetch_meta()` with no exception handling at all, so a
single transient 500 during that window killed the run outright, defeating the whole point of a
feature meant to tolerate propagation lag.

### The ABC question

The user then asked a sharper design question: why was `CubeApiClient` an `ABC` at all, given
`CubeRestApiClient` is the only real implementation? Checked Phase 39's actual stated
rationale rather than assuming it still held: "keeps a seam for a test double... and for a
future non-REST way of checking Cube's state, without committing to needing one yet." Both
turned out weak on inspection -- a test double never needed a formal base class (Python duck
typing already makes any object with a compatible `fetch_meta` substitutable, and the existing
tests proved this by construction, not by argument), and "a future non-REST way" is exactly the
kind of speculative future requirement this project's own conventions say not to design for.
Contrast with `CubeFilePromoter`, which earns its `ABC`: genuinely multiple real
implementations (`LocalFileCubeFilePromoter`, `GitCubeFilePromoter`), each needing
destination-specific runtime credentials no component author could anticipate. `CubeApiClient`
never had that -- there's only one way to talk to Cube's own REST API. Removed the ABC;
`CubeRestApiClient` is now a plain concrete `dg.ConfigurableResource`. The external-resource
path on `CubeLandingCheck` (bind something under `resource_key` instead of `api_url`/
`api_token`) stays exactly as useful as before -- that design (Phase 44) was never actually
about the ABC, it was about sharing one resource instance or substituting a test double, both
still fully supported via duck typing with no formal base class needed.

### The retry fix

Collapsing to one concrete implementation is what actually unlocked doing this properly, as the
user pointed out: with `wait_for_landing` now able to assume it's always talking to `requests`,
it can distinguish *why* a poll failed, not just that it did. `requests.HTTPError` with a `5xx`
status, or the request failing to complete at all (`requests.RequestException` -- connection
refused, a timeout, Cube mid-restart), is treated the same as "not landed yet" and retried,
bounded by the existing `timeout_seconds`. A `4xx` response (a bad `api_url`, an invalid/expired
`api_token`) is a permanent misconfiguration more polling won't fix, so it's re-raised
immediately instead of only surfacing once the timeout elapses -- this is the precision a
blanket `except Exception: keep polling` (my own first-pass proposal, before the user's ABC
question) couldn't offer, since that would have also silently retried a genuine 401 for the
full timeout window before failing with a message that didn't explain why. The last error seen
(if any) is included in the eventual timeout `dg.Failure` message, so a *persistent* 5xx/
connection failure -- distinct from "the content genuinely never landed" -- is still
diagnosable rather than looking identical to ordinary propagation lag.

### Verification

- Four new tests in `test_landing_check.py`, against a real `CubeRestApiClient` with mocked
  `requests.get` (not a fake client -- the new logic depends on real `requests` exception/
  status-code shapes): retries through a `5xx` response and a connection error, each followed
  by a successful match; fails immediately (asserted both by exception type -- `requests.
  HTTPError`, not `dg.Failure` -- and by wall-clock time, with a `timeout_seconds`/
  `poll_interval_seconds` long enough that a retry-then-timeout path would have taken much
  longer) on a `4xx` response; includes "last error while polling" in the timeout message for a
  persistent `5xx`.
- All three retry/last-error tests confirmed to actually catch the regression: temporarily
  restored the old unconditional `client.fetch_meta()` call (no try/except), watched them fail
  with the raw `requests.HTTPError`/`ConnectionError` propagating unhandled, restored the fix.
- `CubeApiClient` removed from `dagster_cube_dbt/__init__.py`'s exports, `docs/reference.md`,
  and every docstring/README mention; existing scripted-client test doubles
  (`_client_with_responses`, `_scripted_cube_api_client`) just dropped the now-nonexistent base
  class -- no other change needed, proving the ABC was never load-bearing for them.
- `mkdocs build --strict` clean.
- Full suite: **144/144 passed** (140 + 4 new retry tests).

## Phase 48 -- worked around the release pipeline's GitPython/`python-semantic-release` breakage instead of continuing to wait on it

`release.yml` had been blocked since before Phase 44: `python-semantic-release/python-semantic-release@v10.6.1` (the official Docker action) fails on `AttributeError: type object 'Actor' has no attribute 'name_email_regex'`. Root cause confirmed upstream, not ours: GitPython 3.1.60 removed that attribute, `python-semantic-release`'s own `src/gh_action/requirements.txt` pins only `python-semantic-release == 10.6.1` with no GitPython constraint, and the action's Dockerfile rebuilds that image fresh (no lockfile) on every single run -- so every run resolves whatever GitPython is newest at that moment, unconditionally broken since the moment 3.1.60 was published. Upstream issues #1475/#1476 and fix PR #1477 all still open as of 2026-08-26. The user chose to wait for upstream twice already (see [[project_release_pipeline_blocked_gitpython]]); this time, prompted by a workaround the user found in the wild (`OpenJobDescription/openjd-model-for-python`'s commit `10859dfd`), asked to reconsider.

### Decisions

- OpenJobDescription's own fix -- pin `GitPython == 3.1.59` (the last version before the breaking removal) in their `requirements-release.txt` -- doesn't transplant directly: their release step installs `python-semantic-release` via plain `pip install -r`, ours goes through the Docker action, which builds from a `requirements.txt` we don't control and have no input to inject a constraint into.
- Fetched the action's own `action.yml`/`Dockerfile`/`action.sh` at the `v10.6.1` tag to check what it actually does under the hood, rather than guessing: `action.sh` turns out to be a thin wrapper that just runs `semantic-release [-v] version [--commit/--tag/--push/--changelog/--vcs-release ...]` with `GH_TOKEN` set and git committer config applied -- nothing container-specific, and with none of our optional inputs set, every one of those flags evaluates to the CLI's own defaults (i.e. our current invocation is functionally just `semantic-release -v version`).
- Replaced the Docker action step with a plain step: `uvx --from python-semantic-release==10.6.1 --with GitPython==3.1.59 semantic-release -v version`, run with `working-directory: python_modules/dagster-cube-dbt` (matching the action's `directory:` input) and `GH_TOKEN`/git committer config set to the same values the action was already passed -- the same workaround as OpenJobDescription's, adapted from their pip-based release step to our uv-based one.
- Confirmed `steps.release.outputs.released/tag/version` (consumed by the `publish` job's `if:` and `environment.url`) survive dropping the Docker action: `GITHUB_OUTPUT` is written directly by the CLI itself (`semantic_release/cli/github_actions_output.py`'s `write_if_possible`, keyed off the `GITHUB_OUTPUT` env var GitHub Actions sets on every step regardless of container vs. plain), not by `action.sh` or anything Docker-specific.
- Skipped `action.sh`'s `git config --system --add safe.directory "*"` -- that exists to work around a UID mismatch between the container's root user and the host filesystem owner, which doesn't apply once this runs as a plain step in the same user context as the checkout.

### Verification

- Read `action.yml`/`Dockerfile`/`action.sh`/`requirements.txt` directly from the `python-semantic-release/python-semantic-release` repo at the `v10.6.1` tag (via the GitHub REST API, unauthenticated) rather than assuming the action's internals -- confirmed the exact CLI invocation and output-writing mechanism before relying on either.
- Confirmed upstream issues #1475/#1476/PR #1477 are all still open before deciding this was worth doing now rather than continuing to wait.
- Confirmed working in a real CI run: after merging to `next` (PR #20), `release.yml` ran end to end and produced `0.3.0-rc.1` -- the release pipeline is unblocked.

## Phase 49 -- `SupersetResource.api_key`: authenticate without a `db`-provider username/password

The user hit a real `401 Unauthorized` from `POST /api/v1/security/login` on their own Superset deployment. Root cause turned out to be their account not being marked active (an account-side issue, not a library bug) -- but investigating it surfaced a genuine gap: `_ensure_authenticated` always POSTs `{"provider": "db"}`, which only ever works for an account whose password Superset itself manages. An account authenticated via LDAP/OIDC SSO has no such password this resource could log in with at all, regardless of correctness. The user separately pointed at Superset's own API key support (https://superset.apache.org/admin-docs/security/#api-key-authentication) as the fix for that case, and asked for OIDC/SSO support "eventually."

### Decisions

- Read Superset's own API key docs directly (fetched, not assumed) before implementing: a key is a plain Bearer token, generated once through a user's own profile UI, used as `Authorization: Bearer <key>` -- no separate exchange step, unlike the existing login flow's `access_token`.
- Added `api_key: str | None = None` to `SupersetResource`, alongside making `username`/`password` optional (previously required `str` fields). `_ensure_authenticated` now branches: if `api_key` is set, skip the `/api/v1/security/login` POST entirely and set the `Authorization` header directly from it; otherwise, the existing DB-login flow runs unchanged. The CSRF-token fetch still runs either way -- nothing in Superset's docs suggested API-key-authenticated requests are exempt from CSRF, and it's one cheap extra `GET` either way.
- Enforced "set one or the other, not both" with a pydantic `model_validator(mode="after")` on `SupersetResource` itself (raises at construction time, not mid-run) -- consistent with the project's established preference for failing fast on config problems at the boundary rather than surfacing a confusing error deep in a call stack.
- Mirrored the same validation in `CubeSupersetSyncComponent.build_managed_resource()` (an `api_key` field alongside `username`/`password`, both the "both set" and "neither set" cases raising `DagsterInvalidDefinitionError` with an actionable message) -- the same "raise clearly instead of deferring to a confusing pydantic error" reasoning Phase 44 already established for this method.
- **Did not implement OIDC/SSO** -- driving an actual interactive SSO login flow (browser redirect, token exchange) is a fundamentally different, much larger piece of work than a config field, and nothing about the current request needs it: an API key generated from an already-SSO-authenticated account covers the user's actual use case. Documented this limitation explicitly in both `SupersetResource`'s docstring and the README, rather than leaving it to be discovered by a failed attempt.

### Verification

- Three new tests in `test_superset_resource.py`: `api_key` skips the login POST entirely and sets the `Authorization` header directly (confirmed via revert-and-confirm-fail -- temporarily forced the `api_key` branch off, watched the test fail on an unexpected `session.post` call, restored it); constructor raises with neither `api_key` nor `username`/`password` set; constructor raises with both set.
- Two new tests in `test_cube_superset_sync_component.py`: `build_managed_resource` builds a `SupersetResource` from `api_key` alone (leaving `username`/`password` as `None`); raises when `api_key` and `username`/`password` are both set.
- Full suite: **152/152 passed** (147 + 5 new tests).

## Phase 50 -- `SupersetResource` requests now surface the response body on failure

The user, past the auth fix from Phase 49, hit a real `400 Client Error: BAD REQUEST for url: .../api/v1/dataset/` from `_create_dataset` -- and the traceback showed nothing beyond that status line. `raise_for_status()` alone never includes the response body, so there was no way to know *why* Superset rejected the request (a duplicate-dataset conflict? a table-not-found from schema introspection? something else?) without a packet capture. Rather than guess, fixed the actual blind spot first.

### Decisions

- Added a module-level `_raise_for_status(response)` helper: calls `response.raise_for_status()`, and on `requests.HTTPError`, re-raises with the response body appended to the message (`f"{error} -- response body: {response.text}"`). Superset's REST API returns a JSON payload describing the actual validation failure on a 400 (and useful detail on most other error codes too), which is exactly the information needed to diagnose this without another round trip.
- Replaced all eight `response.raise_for_status()` call sites in the login/CSRF/find/create/refresh/update flow with this helper, not just the one that happened to fail in the report -- every one of them can 400/401/403/5xx with a body worth seeing, and leaving seven of the eight silently truncated would just relocate this same problem to the next failure.
- Didn't guess at the actual root cause of the user's specific 400 (duplicate dataset from a `_find_dataset_id` filter mismatch? table not yet visible to Superset's schema introspection? something else entirely) -- shipped the diagnostic fix first so the next report comes with Superset's own explanation attached, rather than spending a round trip on an unverified guess.

### Verification

- Two new tests in `test_superset_resource.py`, against a real `requests.Response` (not the `_FakeResponse` test double, which stubs `raise_for_status()` as a no-op and only exercises success paths) -- confirmed the JSON body's actual error text lands in the raised message on a 4xx, and that a 2xx response still doesn't raise at all. Confirmed via revert-and-confirm-fail: temporarily reverted `_raise_for_status` to a bare `response.raise_for_status()`, watched the body-surfacing test fail with just the generic status line, restored it.
- Full suite: **154/154 passed** (152 + 2 new tests).

## Phase 51 -- `SupersetResource` sends a `Referer` header: Flask-WTF's HTTPS CSRF check needs one, separately from the CSRF token itself

Phase 50's error-body fix paid off immediately: the same user's next retry showed exactly why the earlier 400 was happening -- `"400 Bad Request: The referrer header is missing."` (`error_type: GENERIC_BACKEND_ERROR`, issue code 1011). Not a guess this time, Superset's own error told us directly.

### Decisions

- Root cause is Flask-WTF's own CSRF protection, not Superset-specific: for any state-changing request over HTTPS, it checks the `Referer` header's origin against the request's own origin, *in addition to* validating the `X-CSRFToken` header -- two separate checks, and a correct CSRF token doesn't satisfy the Referer one. A plain `requests.Session` never sends a `Referer` header on its own the way a browser does, so every POST/PUT/DELETE this resource makes was failing this check, regardless of the CSRF token being fetched and set correctly.
- This never showed up on `/api/v1/security/login` (also a POST) because Flask-AppBuilder marks the login view CSRF-exempt entirely -- there's no token to check yet at that point, so Flask-WTF's referrer check doesn't run there either. It only surfaces on the *first* mutating call after login, which for this resource is always `_create_dataset` (or the update call, for an existing dataset) -- exactly where the user's report landed.
- Fix: set `self._session.headers["Referer"] = self.base_url` once, alongside the existing `verify_tls` assignment at the top of `_ensure_authenticated` (same reasoning as that one -- applies to every request through the session, needs setting exactly once, not per call site). Using `base_url` itself as the Referer is correct here: Flask-WTF's check only compares origins (scheme + host), not full paths, and `base_url` is definitionally the right origin for every request this resource makes.
- Documented this in `SupersetResource`'s own docstring (a genuinely non-obvious dependency a future reader wouldn't guess from the code alone) -- didn't add anything to the README, since it's not a config knob a user needs to know about, just an internal request-flow detail that now works transparently.

### Verification

- One new test in `test_superset_resource.py`: `sync_dataset` sets `session.headers["Referer"]` to `base_url`. Confirmed via revert-and-confirm-fail: temporarily removed the header assignment, watched the test fail with a `KeyError` (no `Referer` key set at all), restored it.
- Full suite: **155/155 passed** (154 + 1 new test).
