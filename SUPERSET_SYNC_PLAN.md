# Superset dataset sync -- development plan

**Status**: Implemented (`CubeSupersetSyncComponent` + `SupersetResource`, see DECISIONS.md
Phase 42 for what actually happened and what deviated from this plan along the way). This was a
large, separable addition on top of the `CubeDbtProjectComponent` work tracked in
[PLAN.md](PLAN.md)/[DECISIONS.md](DECISIONS.md); this file tracked it independently rather than
folding it into those, and stays as the design record.

## Goal

Sync each generated Cube **view** (see `CubeDbtProjectComponent`) into Apache Superset as a
dataset, so BI users get column descriptions, groupby/filter flags, and pre-defined metrics for
every view without anyone manually configuring datasets in Superset by hand. One Dagster asset
per Superset dataset, materialized on the same "definition changed" cadence the cube/view assets
themselves already use -- not on every dbt data refresh.

Shipped as an optional extra (`pip install dagster-cube-dbt[superset]`) so the base install
doesn't gain a hard dependency on `requests` (or whatever HTTP client the Superset resource
ends up using) for users who don't need this.

## Prior art: `dbt-to-cube`'s `SupersetConnector`

Investigated <https://github.com/ponderedw/dbt-to-cube/blob/main/dbt-cube-sync/dbt_cube_sync/connectors/superset.py>
as a reference for the actual Superset REST API flow, which is real, working knowledge worth
keeping even though we won't reuse its code directly (it's a standalone script, not a Dagster
integration, and it gets its schema data by regex-parsing raw generated `.js` Cube files, which
we don't need to do -- we already have structured, typed cube/view dicts).

**The API flow we're borrowing:**

1. `POST /api/v1/security/login` with `provider: "db"` -> JWT access token, set as a bearer
   `Authorization` header on the session for everything after.
2. `GET /api/v1/security/csrf_token/` -> CSRF token, set as `X-CSRFToken` header (required for
   the POST/PUT calls below).
3. `GET /api/v1/database/?q=...` filtered by `database_name` -> resolves a configured database
   *name* (e.g. `"Cube"`, a Superset database connection someone has already pointed at Cube's
   SQL API) to its internal database `id`. We don't create this database connection ourselves --
   it's a prerequisite the user sets up once in Superset, same as `CubeFilePromoter` assumes a
   running Cube instance already exists.
4. `GET /api/v1/dataset/?q=...` filtered by `table_name` + `schema` + `database` -> find an
   existing dataset for this view, if any (schema is always `"public"` for Cube's SQL API;
   `table_name` is the Cube view's name).
5. If none found: `POST /api/v1/dataset/` with just `{database, schema, table_name}` to create a
   bare dataset -- Superset doesn't know the columns yet at this point.
6. `PUT /api/v1/dataset/{id}/refresh` -- tells Superset to introspect the underlying table (via
   the SQL API) and populate its own column list. The reference implementation sleeps 2s after
   this; we should poll/retry instead of a fixed sleep (see Open questions).
7. `GET /api/v1/dataset/{id}` -> the freshly-introspected columns and any existing metrics.
8. `PUT /api/v1/dataset/{id}` with updated `columns` (merge our dimension metadata --
   `verbose_name`, `description`, `is_dttm`, `groupby`, `filterable` -- onto Superset's
   introspected column list, matched by name) and `metrics` (one per measure, expressed as
   `MEASURE(ViewName.measure_name)` -- Cube SQL API's own aggregation-pushdown syntax, not a
   raw SQL aggregate function, so Cube handles the actual aggregation logic).

Type mapping (Cube dimension type -> Superset/SQL column type) is a small fixed table:
`string -> VARCHAR`, `number -> NUMERIC`, `time -> TIMESTAMP`, `boolean -> BOOLEAN`. We'll need
this too, sourced from our own dimension dicts' `type` field (same field `_column_schema` in
`component.py` already reads).

## Architecture decision: component chaining, not subclassing

The open question going in was whether one Dagster component can read another already-loaded
component's state, or whether this has to be a subclass of `CubeDbtProjectComponent` that emits
extra assets from data it already has locally. Investigated directly against the installed
`dagster` package (not assumed):

- `ComponentLoadContext.load_component(defs_path, expected_type)` (`dagster/components/core/
  context.py`) loads another component elsewhere in the same defs tree by its path, resolved
  relative to the defs root, and registers a proper dependency edge in the component tree
  (`mark_component_load_dependency`) so the framework understands the ordering. This is
  dagster's own sanctioned composition primitive.
- `StateBackedComponent.build_defs` (`dagster/components/component/state_backed_component.py`)
  resolves its own cached state via `DefinitionsLoadContext.get().state_path(self.
  defs_state_config, state_storage, project_root)` -- and `defs_state_config` is a plain public
  property. Nothing about this is private to `DbtProjectComponent`; it's the general contract
  every `StateBackedComponent` (which `CubeDbtProjectComponent` is, transitively) implements.

Combining these: a new, standalone `CubeSupersetSyncComponent` can, inside its own
`build_defs_from_state`, at deploy time, with no live dbt project required:

```python
sibling = context.load_component(self.dbt_cube_component, CubeDbtProjectComponent)
state_path = DefinitionsLoadContext.get().state_path(
    sibling.defs_state_config, DefsStateStorage.get(), context.project_root
)
cubes, views, _ = read_cube_state(state_path)  # reads the same cube_dbt_state.json
                                                # CubeDbtProjectComponent.write_state_to_path
                                                # already writes -- see Package layout below
```

This is the mechanism that makes it a *separate* component rather than a subclass:

- No duplicated `project:`/`cube_select:`/merge-patch config between two `defs.yaml` blocks --
  the config-drift risk of two components independently re-running cube generation and
  silently disagreeing.
- `sibling.asset_key_for_view(name)` is called for `deps=[...]`, not a hardcoded key -- so a
  subclass of `CubeDbtProjectComponent` renaming view keys (the exact Phase 37 scenario, see
  DECISIONS.md) is still respected automatically, with no extra work on our part.
- Stays state-backed end to end: reads a cache file already shipped inside `.local_defs_state/`,
  never touches the live dbt project or re-invokes generation. Consistent with the Phase 36
  lesson (`[[feedback_state_backed_design]]` in the agent's own memory) -- must double check
  this holds once actually implemented, not just in this design sketch.

Rejected alternative: reconstructing `DbtProjectComponent`'s state key by hand (it's
`f"DbtProjectComponent[{self._project_manager.defs_state_discriminator}]"`,
`dagster_dbt/components/dbt_project/component.py`) so a second, independently-configured
component could compute the same `get_local_state_path` without going through
`load_component` at all. Rejected because `defs_state_discriminator` is a private property of
an internal manager class -- reimplementing it would repeat the exact mistake DECISIONS.md
Phase 36/37 already burned us on twice: depending on unexported internals instead of the
library's actual public contract.

## Scope decisions (confirmed)

- **Views only**, not cubes -- Cube's own convention is that views are the intended BI-facing
  query layer; cubes are building blocks. No `superset_select`-style filter in v1; if per-view
  opt-out/opt-in turns out to be needed later, it's a natural, additive follow-up (mirrors how
  `cube_select` already exists for the analogous cube-generation problem) rather than something
  to speculatively build now.
- **Automation condition**: reuse `GENERATED_ASSET_AUTOMATION_CONDITION` (`component.py`) as-is
  -- a Superset dataset only needs updating when the view's *schema* changes (its
  `code_version`), not on every dbt data refresh underneath it. Requires exporting that
  condition (or a shared constant/helper) from somewhere both components can import; currently
  module-private to `cube_dbt_project/component.py`.
- **Naming**: component is `CubeSupersetSyncComponent`; extra is `dagster-cube-dbt[superset]`.

## Package layout

Mirrors the existing `components/cube_dbt_project/` layout:

```
src/dagster_cube_dbt/
  components/
    cube_dbt_project/component.py          # existing, unchanged
    cube_superset_sync/
      __init__.py
      component.py                          # CubeSupersetSyncComponent
  superset_resource.py                      # SupersetResource (dg.ConfigurableResource)
  cube_state.py                             # NEW: extract CUBE_STATE_FILENAME read/write
                                             # into a shared module both components import,
                                             # instead of cube_dbt_project/component.py owning
                                             # a format cube_superset_sync also needs to parse
```

**Revised after initial implementation** (the user pushed back on real-world usability once
testing against a consuming project): `CubeSupersetSyncComponent` and `SupersetResource` *are*
re-exported from the top-level `dagster_cube_dbt/__init__.py`, the same as every other public
symbol in this library. The original plan here said not to, reasoning that the top-level module
is imported unconditionally by anything importing `dagster_cube_dbt` at all and shouldn't force
a hard `requests` dependency on projects that don't need Superset -- but `requests` was already
made a *base* dependency back when `landing_check` shipped (before `SupersetResource` was ever
written), so there was no dependency boundary left to protect. Left in place, the decision only
bought worse ergonomics (a full internal dotted path required in every `defs.yaml`) for no
actual benefit -- worth recording as a case where a plan's stated rationale should have been
revisited once its precondition (no base `requests` dependency) stopped being true, rather than
carried forward unquestioned into implementation.

```yaml
type: dagster_cube_dbt.CubeSupersetSyncComponent
attributes:
  dbt_cube_component: "../dbt_ingest"   # path to the CubeDbtProjectComponent's defs.yaml dir
  database_name: "Cube"
  superset_resource_key: "superset"
```

**To verify during implementation, not assumed here**: how `dg scaffold defs`/component-type
resolution and the `dagster_dg_cli.registry_modules` entry point (currently pointing at
`dagster_cube_dbt`) behave for a component that isn't re-exported at the top level -- confirm
`dg list components`/`dg scaffold defs <dotted-path>` actually finds it before assuming this
layout works end to end.

## `SupersetResource`

A `dg.ConfigurableResource` (not the `ABC` base + concrete-subclass pattern `CubeFilePromoter`
uses -- there's exactly one real target here, Superset's own REST API, not an open set of
possible destinations), owning the login/CSRF/session lifecycle and the find-or-create/update
dataset calls from the API flow above:

```python
class SupersetResource(dg.ConfigurableResource):
    base_url: str
    username: str
    password: str  # or a StringSource / EnvVar-backed field -- credentials shouldn't
                    # land in defs.yaml in plaintext; confirm the idiomatic Dagster pattern
                    # for secret resource config during implementation.

    def sync_dataset(self, database_name: str, schema: str, table_name: str,
                      dimensions: list[dict], measures: list[dict]) -> int: ...
```

Session/token setup happens once per resource instance (cached), not once per dataset synced --
`sync_dataset` is called once per view in the multi-asset op below, and re-logging-in for every
one of potentially dozens of views is wasteful and slower than necessary.

## Component design

```python
@dataclass
class CubeSupersetSyncComponent(dg.Component, dg.Resolvable):
    dbt_cube_component: str          # path to the CubeDbtProjectComponent, resolved relative
                                      # to defs root, passed straight to context.load_component
    database_name: str = "Cube"      # Superset database connection name
    superset_resource_key: str = "superset"

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        sibling = context.load_component(self.dbt_cube_component, CubeDbtProjectComponent)
        state_path = DefinitionsLoadContext.get().state_path(
            sibling.defs_state_config, DefsStateStorage.get(), context.project_root
        )
        if state_path is None:
            raise dg.DagsterInvalidDefinitionError(...)  # same "run refresh-defs-state first"
                                                            # error CubeDbtProjectComponent raises
        _, views, _ = read_cube_state(state_path)
        specs = [self._dataset_asset_spec(sibling, view) for view in views]
        # one multi-asset, mirroring _cube_assets in component.py -- one op per component
        # instance rather than one op per dataset, same pool/concurrency rationale
        ...
```

One `AssetSpec` per view: `key` derived from the view name (needs its own key-prefix, e.g.
`superset_dataset/<name>`, distinct from `cube_view/<name>`), `deps=[sibling.asset_key_for_view(
view["name"])]`, `automation_condition=GENERATED_ASSET_AUTOMATION_CONDITION`, `kinds={
"superset"}`. The op itself calls `SupersetResource.sync_dataset(...)` once per selected asset
in the multi-asset, same `can_subset=True` pattern `_cube_assets` already uses.

## Testing strategy

No real Superset instance in CI. Needs a fake HTTP layer (`requests_mock` or a hand-rolled
`requests.Session` stand-in matching just the endpoints in the API flow above) so
`SupersetResource.sync_dataset` can be exercised against realistic request/response shapes
without a live server -- same spirit as `NoopCubeFilePromoter` in the existing test suite, which
stands in for a real promotion destination.

Two things specifically worth a regression test, based on what's already bitten this project
once each:

- **Component chaining actually reads the sibling's state**, not a live re-generation --
  structure this test the same way `test_build_defs_from_state_does_not_need_the_live_dbt_project_directory`
  proves Phase 36: delete the live dbt project directory after `write_state_to_path`, before
  building the superset-sync component's defs, and confirm it still works.
- **A subclass renaming view keys is respected** in the sync component's `deps=[...]` -- same
  shape as `test_get_cube_asset_spec_override_renaming_the_key_is_reflected_in_extends_deps`
  (Phase 37), but asserting the *cross-component* dependency edge instead of an intra-component
  one.

## Open questions / risks (not yet resolved -- surface during implementation, don't guess now)

- ~~**Dataset refresh propagation**~~ -- **resolved upstream.** `CubeDbtProjectComponent` now
  has an optional `landing_check` (`dagster_cube_dbt/landing_check.py`, added ahead of this
  project): after promotion, it polls Cube's own REST API (`/v1/meta`) until the promoted
  content is actually visible there before considering the cube/view materialized, using a
  `code_version` marker stamped into the entity's own `meta` block. If a project has this
  turned on, a cube/view asset genuinely being `materialized` already means "landed in Cube" --
  so as long as this sync component's assets `deps=[...]` on the cube/view assets the normal
  way, step 6's `PUT .../refresh` can assume Cube already has the schema, no separate
  poll/retry needed on the Superset side. If a project *hasn't* turned `landing_check` on, this
  risk still applies -- worth a doc note when this actually gets built, not a runtime check
  (this component has no way to know whether the sibling has it enabled without reading its
  config, which is knowable via `context.load_component` if it turns out to matter).
- **Credential handling** for `SupersetResource.password`: confirm the idiomatic
  Dagster-resource pattern (likely `EnvVar`-backed config, resolved from `defs.yaml`'s Jinja
  templating the same way other secrets in this project are handled) before shipping a field
  that invites plaintext passwords in checked-in YAML.
- **Multiple `CubeSupersetSyncComponent` instances against the same Superset dataset**: if two
  components (or two runs) target the same `(database, schema, table_name)` concurrently, the
  find-or-create step has an obvious race. `CubeDbtProjectComponent` solves this class of
  problem for its own promoter with a concurrency `pool`; the sync component's op should
  probably default to one too, scoped per `dbt_cube_component` target the same way
  `promotion_pool` is scoped per dbt project.

## Staged implementation plan

1. Extract `cube_state.py`: move `CUBE_STATE_FILENAME` read/write (currently inlined in
   `cube_dbt_project/component.py`'s `write_state_to_path`/`build_defs_from_state`) into a
   small shared module with an explicit `write_cube_state`/`read_cube_state` pair. Update
   `CubeDbtProjectComponent` to use it. Pure refactor -- full existing suite must still pass
   unchanged before anything Superset-specific is added.
2. Export `GENERATED_ASSET_AUTOMATION_CONDITION` from a location `cube_superset_sync` can
   import without pulling in `cube_dbt_project`-specific internals (or confirm importing it
   from there directly is fine -- `cube_superset_sync` already needs `CubeDbtProjectComponent`
   itself for `context.load_component`'s `expected_type`, so this may not need to move at all).
3. Add the `superset` extra to `pyproject.toml` (`requests`, or whatever client the resource
   ends up using) and confirm base install (`uv sync` with no extras) still has zero new deps.
4. Implement `SupersetResource` against a fake HTTP layer -- login/CSRF/find-database/
   find-or-create-dataset/refresh/update-columns-and-metrics -- unit tested in isolation from
   any component/asset-graph concerns.
5. Implement `CubeSupersetSyncComponent`: `context.load_component` + state-path resolution +
   `read_cube_state` + one `AssetSpec` per view + the multi-asset op wired to
   `SupersetResource.sync_dataset`.
6. The two chaining-specific regression tests described above (live-project-not-needed,
   key-rename-respected-across-components), verified the same way Phases 36-38 were: revert
   just the fix/mechanism under test, confirm the test actually fails, restore it.
7. Docs: extend the package README/docs site with a "Superset" section mirroring the existing
   "Documentation"/promoter sections -- a `defs.yaml` example wiring both components together,
   and the resource binding for `SupersetResource`.
8. Update `PLAN.md`'s status line and add a `DECISIONS.md` phase once real design tradeoffs are
   hit during implementation (there will be some -- the propagation-lag and credential-handling
   items above are the likely candidates), following this project's established practice of
   recording *why*, not just *what*.
