"""`CubeDbtProjectComponent`: extends `dagster_dbt.DbtProjectComponent` to also generate a
Cube semantic-layer schema from the same dbt project's manifest, exposing each generated (and
merge-patched) cube or view as a virtual, pass-through Dagster asset.

See the package README for the full generation and merge-patch model this implements.
"""

import hashlib
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated, Any, Self

import dagster as dg
import yaml
from dagster._annotations import public
from dagster.components.resolved.model import Resolver
from dagster_dbt.components.dbt_project.component import DbtProjectComponent
from dagster_dbt.dagster_dbt_translator import validate_translator
from dagster_dbt.dbt_manifest import validate_manifest
from dagster_dbt.utils import ASSET_RESOURCE_TYPES

from dagster_cube_dbt.cube_state import CUBE_STATE_FILENAME, read_cube_state, write_cube_state
from dagster_cube_dbt.generation import generate_cubes
from dagster_cube_dbt.landing_check import CubeRestApiClient, wait_for_landing, with_landing_check_meta
from dagster_cube_dbt.merge import discover_patch_files, merge_documents, resolve_extends
from dagster_cube_dbt.output import write_entities

DEFS_YAML_FILENAME = "defs.yaml"
CUBE_KEY_PREFIX = "cube"
VIEW_KEY_PREFIX = "cube_view"

# Internal-only keys stashed on a resolved cube dict (never on what's written to real Cube
# YAML -- see build_defs_from_state) so get_cube_asset_spec can resolve a cube asset's real
# dependency without needing to change that method's public single-argument signature.
# Double-underscore-prefixed so they read unambiguously as "not a real Cube field" wherever
# displayed. Mutually exclusive in practice: a cube either extends another generated cube
# (_EXTENDS_PARENT_KEY) or, if not, may need its dbt model dependency resolved directly
# (_DBT_MODEL_NAME_KEY, for a cube renamed via `meta.cube.name`/`suffix`).
_EXTENDS_PARENT_KEY = "__dagster_cube_dbt_extends_parent"
_DBT_MODEL_NAME_KEY = "__dagster_cube_dbt_dbt_model_name"


@dataclass
class CubeSelect(dg.Resolvable):
    """Filters which dbt models generate a cube. Independent of `select`/`exclude`/`selector`,
    which control what dbt actually builds.
    """

    paths: Annotated[
        list[str],
        Resolver.default(
            description="Only include models whose dbt path starts with one of these prefixes."
        ),
    ] = field(default_factory=list)
    tags: Annotated[
        list[str],
        Resolver.default(description="Only include models tagged with all of these tags."),
    ] = field(default_factory=list)
    names: Annotated[
        list[str],
        Resolver.default(description="Only include models with one of these exact names."),
    ] = field(default_factory=list)


@dataclass
class CubeLandingCheck(dg.Resolvable):
    """Optional post-promotion step (off by default -- set this to turn it on): after
    `CubeFilePromoter.promote` returns, poll Cube's own REST API until every cube/view
    selected for this run is actually visible there, before considering it materialized. See
    `dagster_cube_dbt.landing_check` for the full rationale and the API contract this assumes.

    Needs a `CubeRestApiClient` to issue that poll through -- two ways to provide one, chosen by
    whether `api_url` is set:

    - **Set `api_url` (and `api_token`)** and this component builds and owns a
      `CubeRestApiClient` itself, directly from these attributes -- the common case, since
      there's only one real implementation.
    - **Leave `api_url` unset** and this falls back to looking up something bound under
      `resource_key` as an ordinary Dagster resource -- for a test double, or one resource
      instance shared across multiple components. Nothing needs to formally subclass
      `CubeRestApiClient` for this; any object with a compatible `fetch_meta` method works.
      `resource_key` currently still defaults to `"cube_api_client"` for backwards
      compatibility; a future release will remove that default, requiring `resource_key` to be
      set explicitly whenever this external-resource path is what you actually want, so an
      incomplete `api_url`-less, `resource_key`-less config is unambiguously reported as
      *missing configuration* rather than a *missing resource*.
    """

    api_url: Annotated[
        str | None,
        Resolver.default(
            description="Cube deployment's REST API base URL. Setting this (with api_token) "
            "means this component builds and owns its own CubeRestApiClient directly, instead "
            "of looking up a resource bound under resource_key.",
        ),
    ] = field(default=None, kw_only=True)
    api_token: Annotated[
        str | None,
        Resolver.default(
            description="Sent verbatim in the Authorization header -- see CubeRestApiClient's "
            "own docstring. Required whenever api_url is set.",
        ),
    ] = field(default=None, kw_only=True)
    verify_tls: Annotated[
        bool,
        Resolver.default(
            description="Passed straight through to the component-managed CubeRestApiClient's "
            "own verify_tls (only meaningful when api_url is set).",
        ),
    ] = field(default=True, kw_only=True)
    resource_key: Annotated[
        str,
        Resolver.default(
            description="Resource key of the CubeRestApiClient (or any object with a "
            "compatible fetch_meta method) this poll is issued through, when api_url is left "
            "unset -- bound like any other Dagster resource, doesn't need to be declared in "
            "this defs.yaml.",
        ),
    ] = field(default="cube_api_client", kw_only=True)
    timeout_seconds: Annotated[
        float,
        Resolver.default(
            description="How long to keep polling before failing the run with a clear "
            "timeout error, naming whichever cube(s)/view(s) never landed.",
        ),
    ] = field(default=60.0, kw_only=True)
    poll_interval_seconds: Annotated[
        float,
        Resolver.default(description="Delay between polls of Cube's `/meta` endpoint."),
    ] = field(default=2.0, kw_only=True)

    def build_managed_client(self) -> CubeRestApiClient | None:
        """Returns a `CubeRestApiClient` built directly from `api_url`/`api_token`/`verify_tls`
        if `api_url` is set (the component-managed path), `None` if it's unset (the caller
        should fall back to `resource_key` instead). Raises clearly, rather than deferring to a
        confusing `pydantic` error, if `api_url` is set but `api_token` isn't -- that
        combination unambiguously means the user intended the managed path but didn't finish
        configuring it, not that they meant the external-resource path instead.
        """
        if self.api_url is None:
            return None
        if self.api_token is None:
            raise dg.DagsterInvalidDefinitionError(
                "landing_check.api_url is set, which means this component should build and "
                "manage its own CubeRestApiClient directly, but landing_check.api_token is "
                "missing. Set both api_url and api_token, or remove api_url entirely (and set "
                "resource_key) to bind a resource externally instead."
            )
        return CubeRestApiClient(api_url=self.api_url, api_token=self.api_token, verify_tls=self.verify_tls)


def _yaml_text(entity: Mapping[str, Any]) -> str:
    return yaml.safe_dump(dict(entity), sort_keys=False)


def _yaml_metadata(entity: Mapping[str, Any]) -> dict[str, dg.MetadataValue]:
    return {"dagster_cube_dbt/yaml": dg.MetadataValue.md(f"```yaml\n{_yaml_text(entity)}\n```")}


def _column_schema(cube: Mapping[str, Any]) -> dict[str, dg.TableSchema]:
    """`dagster/column_schema` metadata (`TableSchema`) built from a cube's dimensions and
    measures -- name, type (Cube's own dimension/measure type, e.g. `string`/`time`/`count`,
    not a warehouse SQL type), and description where present. Deliberately *not*
    `dagster/column_lineage` (`TableColumnLineage`): that's a graph of per-column dependency
    edges onto specific upstream asset columns, which would mean tracing which dbt model
    column(s) fed each dimension and, for measures, which columns an aggregation like `sum`
    or `count` actually touches -- real column lineage, not just listing this cube's own
    columns, and out of scope here.

    The one constraint actually worth surfacing is which dimension(s) make up the cube's
    primary key (`dimension.primary_key: true`, set on possibly more than one dimension for a
    composite key, exactly mirroring how Cube's own schema represents one). A single-key
    dimension is genuinely `unique` on its own; for a composite key, no individual dimension
    is -- only the tuple of all of them together is -- so `unique` is only set when there's
    exactly one, and a table-level constraint spells out the composite case instead, where
    per-column constraints alone can't express it.
    """
    primary_key_names = {d["name"] for d in cube.get("dimensions", []) if d.get("primary_key")}
    is_composite_key = len(primary_key_names) > 1

    def _dimension_constraints(dimension: Mapping[str, Any]) -> dg.TableColumnConstraints | None:
        if dimension["name"] not in primary_key_names:
            return None
        return dg.TableColumnConstraints(
            nullable=False, unique=not is_composite_key, other=["primary key"]
        )

    columns = [
        dg.TableColumn(
            name=dimension["name"],
            type=dimension.get("type", "unknown"),
            description=dimension.get("description"),
            constraints=_dimension_constraints(dimension),
            tags={"dagster_cube_dbt/member_type": "dimension"},
        )
        for dimension in cube.get("dimensions", [])
    ]
    columns += [
        dg.TableColumn(
            name=measure["name"],
            type=measure.get("type", "unknown"),
            description=measure.get("description"),
            tags={"dagster_cube_dbt/member_type": "measure"},
        )
        for measure in cube.get("measures", [])
    ]

    table_constraints = None
    if is_composite_key:
        composite_key = ", ".join(sorted(primary_key_names))
        table_constraints = dg.TableConstraints(other=[f"primary key: ({composite_key})"])

    return {"dagster/column_schema": dg.TableSchema(columns=columns, constraints=table_constraints)}


def _code_version(entity: Mapping[str, Any]) -> str:
    """A cube/view's `code_version` is a hash of its own generated YAML text -- it changes
    exactly when the entity's *definition* changes (regeneration, a merge patch edit), not
    when the dbt model behind it re-materializes with new data. Paired with
    `GENERATED_ASSET_AUTOMATION_CONDITION` below, this is what lets these assets auto-run
    only when their definition actually changed, rather than on every upstream data update.
    """
    return hashlib.sha256(_yaml_text(entity).encode("utf-8")).hexdigest()[:16]


# Cube/view assets should only actually run once after their own generated definition
# changes (detected via `code_version`), not on every upstream data update -- unlike
# `AutomationCondition.eager()`, which triggers on every dependency update.
#
# The "never materialized yet" case needs `missing().newly_true().since_last_handled()`
# wrapped around the *whole* "ready to run" state (missing AND deps ready), not just around
# bare `missing()` ANDed with the deps gate afterwards -- those are not equivalent, and the
# difference matters:
#   - `missing().since_last_handled()` alone (no `newly_true()`) never stops being true while
#     the asset stays unmaterialized, so once a request finally goes out, it can start
#     re-requesting again a couple of ticks later, before the resulting run is even visible
#     as in_progress -- empirically confirmed to duplicate-request.
#   - `missing().newly_true().since_last_handled()` (i.e. `newly_missing().since_last_handled()`,
#     the literal pattern `eager()` itself uses) only stays true from the tick the asset
#     becomes missing to the tick a run is *requested* for it. If a request is blocked at
#     exactly that tick (e.g. a dep is also missing then), the transition it was tracking is
#     gone and it never becomes true again -- `eager()` avoids this because it has a *second*,
#     independent trigger (`any_deps_updated()`) to recover once the dep is ready; swapping
#     that for `code_version_changed()` (which only cares about our own generated content, not
#     dep updates) removes that recovery path.
#   - Wrapping `newly_true().since_last_handled()` around the conjunction (missing AND deps
#     ready) instead fixes both: the tracked transition now happens exactly when the asset
#     first becomes *actually able to run*, whenever that is, so it isn't lost if blocked
#     earlier, and it still only fires once per such transition.
# All three variants (and this real fix) were verified against a real
# `evaluate_automation_conditions` tick sequence -- see DECISIONS.md -- not assumed from
# reading dagster's own source or docs alone.
#
# `code_version_changed()` needs no `newly_true()`/`since_last_handled()` wrapping: unlike a
# one-tick pulse, it stays true from the tick the version changes until the tick it's
# actually evaluated as part of a request, so it isn't lost the same way a raw edge-triggered
# condition would be -- and, unlike `missing()`, applying the deps-ready gate to it afterwards
# (rather than inside a `newly_true()` wrap) is fine for the same reason: since it doesn't
# self-expire before being consumed, a request blocked by the gate one tick still fires the
# next tick the gate opens, instead of the transition being lost.
#
# But it still needs that deps-ready gate applied at all -- editing a cube/view's own
# definition (title, measures, ...) before its backing dbt model has ever run (or while a
# dbt run is still in progress) must not fire a request for it; the model's table won't
# exist yet, so the run would just fail. Without this, `code_version_changed()` alone fires
# regardless of dep state, which is the bug this was written to fix (see DECISIONS.md).
_DEPS_READY = ~dg.AutomationCondition.any_deps_missing() & ~dg.AutomationCondition.any_deps_in_progress()

GENERATED_ASSET_AUTOMATION_CONDITION = (
    (
        (dg.AutomationCondition.missing() & _DEPS_READY).newly_true().since_last_handled()
        | (dg.AutomationCondition.code_version_changed() & _DEPS_READY)
    )
    & ~dg.AutomationCondition.in_progress()
).with_label("cube_code_version_changed")


@dataclass
class CubeDbtProjectComponent(DbtProjectComponent):
    """Expose a dbt project to Dagster as a set of dbt-build assets (inherited from
    `DbtProjectComponent`), plus a generated Cube semantic-layer schema exposed as **virtual**
    assets (`AssetSpec(is_virtual=True)`, currently a Dagster preview feature) -- one per cube
    and one per view.

    Cube generation and merge-patch application happen once when component state is refreshed
    (`dg utils refresh-defs-state`), mirroring how compiling the dbt manifest is already
    handled by `DbtProjectComponent` -- the *result* (the merged cube/view data) is cached as
    part of that state, not recomputed on every defs load. Delivering that result to wherever
    your Cube deployment actually reads its schema from is a separate step that happens at
    *materialization* time, delegated to a `CubeFilePromoter` **resource** rather than an
    overridable component method -- promotion needs credentials and destination config (an S3
    bucket, a git remote, ...), which is what Dagster resources are for, and it keeps this
    component's own subclassing surface reserved for asset-shape customization
    (`get_cube_asset_spec`/`get_view_asset_spec`) rather than runtime dependencies.

    There's no default resource bound to `promoter_resource_key` (`cube_file_promoter` by
    default) -- materializing a cube/view asset without one configured fails clearly, since
    there's no deployment topology where a fixed on-disk path is reachable by both a Dagster
    run and an independently-running Cube instance (Dagster Cloud and most container-per-run
    setups give each run its own throwaway filesystem). Implement a `CubeFilePromoter`
    subclass (see its docstring for an S3 example) and bind it under that resource key
    anywhere in the project -- it doesn't need to be declared alongside this component; any
    `Definitions` merged into the same project supplies it:

        # e.g. defs/resources.py, auto-discovered like any other defs module
        import dagster as dg
        from dagster_cube_dbt import LocalFileCubeFilePromoter

        @dg.definitions
        def resources():
            return dg.Definitions(
                resources={
                    "cube_file_promoter": LocalFileCubeFilePromoter(
                        output_dir="cube_project/model/cubes",
                    )
                }
            )

    And the matching `defs.yaml`:

        # defs.yaml
        type: dagster_cube_dbt.CubeDbtProjectComponent
        attributes:
          project: "{{ project_root }}/path/to/dbt_project"
          cube_select:
            paths: ["marts"]

    `LocalFileCubeFilePromoter` (writes straight to a directory on disk) ships with this
    library for local development, where the Dagster process and the Cube process share a
    filesystem; beyond local dev, implement your own `CubeFilePromoter`.

    By default, a cube/view asset is considered materialized as soon as `promoter.promote`
    returns -- which only means the generated YAML was *handed off*, not that a running Cube
    instance has actually picked it up yet (hot-reload/propagation lag varies by deployment).
    Set `landing_check` (a `CubeLandingCheck`) to poll Cube's own REST API after promotion and
    block materialization until the freshly promoted content is actually visible there; see
    `dagster_cube_dbt.landing_check` for the full mechanism. Off by default -- it needs Cube API
    credentials and adds latency to every promotion.

    The cube/view multi-asset's op is assigned a Dagster concurrency `pool` (`promotion_pool`,
    defaulted to a name scoped to this project) so a max concurrency of 1 can be set for it in
    the Dagster UI (Deployment > Concurrency) without any code change -- most `CubeFilePromoter`
    implementations mutate shared external state (a git checkout, a fixed output directory,
    ...) that two concurrent runs touching at once would corrupt or race on. Assigning the pool
    doesn't itself impose a limit; nothing changes until you actually set one for that pool
    name in the UI, so this is a zero-cost default until you need it.

    Because the generated cube/view assets are `is_virtual`, Dagster's own staleness/freshness
    engine treats them as transparent: a downstream asset (e.g. a Cube pre-aggregation
    refresh) depending on a cube asset sees freshness computed by looking straight through it
    to the nearest non-virtual ancestor -- the dbt model the cube is derived from --
    recursively through chained virtual assets too (a view depending on a virtual cube
    resolves all the way back to the dbt model). The cube/view assets don't need to be
    materialized themselves for that propagation to work; they still can be (e.g. to run the
    promoter), and default to `GENERATED_ASSET_AUTOMATION_CONDITION`, which runs once when a
    cube/view's own generated content changes (its `code_version`), not on every dbt model
    data update the way `AutomationCondition.eager()` would.
    """

    cube_select: Annotated[
        CubeSelect,
        Resolver.default(
            description="Filters which dbt models generate a cube at all. `paths` matches "
            "against each model's manifest path, which is relative to dbt's model-paths root "
            "(`models/` by default) -- a model at models/marts/foo.sql has manifest path "
            "marts/foo.sql, so use paths: ['marts'], not paths: ['models/marts'].",
            examples=[{"paths": ["marts"]}, {"tags": ["cube"]}],
        ),
    ] = field(default_factory=CubeSelect, kw_only=True)
    promoter_resource_key: Annotated[
        str,
        Resolver.default(
            description="Resource key of the `CubeFilePromoter` this component delegates "
            "delivery of generated cube/view YAML to. The resource itself is bound like any "
            "other Dagster resource -- it doesn't need to be declared in this defs.yaml.",
        ),
    ] = field(default="cube_file_promoter", kw_only=True)
    promotion_pool: Annotated[
        str | None,
        Resolver.default(
            description="Dagster concurrency pool assigned to the cube/view multi-asset's "
            "promotion op. Defaults to a pool name scoped to this dbt project, so a max "
            "concurrency of 1 can be set for it in the Dagster UI (Deployment > Concurrency) "
            "without any code change -- most `CubeFilePromoter` implementations mutate some "
            "shared external state (a git checkout, a fixed output directory, ...) that isn't "
            "safe for two runs to touch at once. Set explicitly to share one pool across "
            "multiple components (e.g. if they're bound to the same promoter/destination).",
        ),
    ] = field(default=None, kw_only=True)
    landing_check: Annotated[
        CubeLandingCheck | None,
        Resolver.default(
            description="Optional post-promotion step: poll Cube's own REST API until "
            "promoted content actually lands there before considering a cube/view "
            "materialized. Off by default. See `CubeLandingCheck` for details.",
        ),
    ] = field(default=None, kw_only=True)

    @classmethod
    def load(cls, attributes, context: dg.ComponentLoadContext) -> Self:
        component = super().load(attributes, context)
        component._defs_dir = context.path  # noqa: SLF001
        return component

    def write_state_to_path(self, state_path: Path) -> None:
        super().write_state_to_path(state_path)

        manifest = validate_manifest(self.dbt_project.manifest_path)
        base = generate_cubes(
            manifest,
            paths=self.cube_select.paths,
            tags=self.cube_select.tags,
            names=self.cube_select.names,
        )

        exclude = [self._defs_dir / DEFS_YAML_FILENAME]
        patches = [
            yaml.safe_load(patch_file.read_text()) or {}
            for patch_file in discover_patch_files(self._defs_dir, exclude=exclude)
        ]
        merged = merge_documents(base, patches)
        write_cube_state(state_path, merged)

    def prepare_state_aware_lookup(self, state_path: Path | None) -> dict[str, Any]:
        """Populates everything `get_cube_asset_spec`/`get_view_asset_spec`/
        `_cube_asset_spec_by_name`/`_dbt_model_asset_key_or_none` need to resolve a cube/view's
        identity and dependencies -- entirely from cached state, no live dbt project needed --
        and returns the parsed `cube_dbt_state.json` content so callers don't need to read it
        twice.

        Called by `build_defs_from_state` for this component's own defs, and by
        `CubeSupersetSyncComponent` on a *sibling* instance obtained via
        `context.load_component` -- which loads and resolves a component but never calls
        `build_defs`/`build_defs_from_state` on it (that's a separate step), so without this,
        calling `get_view_asset_spec` on that sibling instance directly would raise: none of
        this state would have been populated on it yet. A real regression the first time this
        was tried -- caught by `CubeSupersetSyncComponent`'s own test suite, not by anything in
        this file's.
        """
        state_aware_project = self._project_manager.get_project(state_path)
        self._state_aware_manifest = validate_manifest(state_aware_project.manifest_path)
        self._state_aware_project = state_aware_project

        merged = read_cube_state(state_path)
        cubes = merged.get("cubes", [])
        # {cube_name: dbt_model_name} for cubes renamed via `meta.cube.name`/`suffix` -- not
        # part of Cube's own schema (see generate_cubes' docstring), so never written to the
        # real promoted YAML; only used below to resolve the correct dbt model dependency.
        cube_source_models = merged.get("cube_source_models", {})

        # Resolved (extends-flattened) purely for building asset specs -- so a cube that
        # only overrides a couple of fields via `extends` still gets its parent's
        # description/meta/etc. reflected in its AssetSpec. The real generated/promoted YAML
        # (`cubes`/`views` above) keeps `extends:` intact; Cube resolves that itself.
        resolved_cubes = resolve_extends(cubes)

        # A cube's `deps` is either the cube it `extends` (its own asset, not the dbt model
        # directly) or -- for a cube with no `extends`, or one whose `extends` target isn't
        # among the generated cubes -- the real dbt model behind it. `extends` is deliberately
        # *not* flattened through to the dbt model here: these are `is_virtual` assets, and
        # Dagster's own staleness engine already looks straight through a *chain* of virtual
        # assets to the nearest real ancestor, so a cube-to-cube edge already propagates
        # freshness back to the dbt model transitively -- one edge per `extends` link mirrors
        # the actual reuse relationship, rather than every cube in a chain independently
        # rediscovering the same dbt model.
        augmented_cubes_by_name: dict[str, dict[str, Any]] = {}
        for cube in cubes:
            name = cube["name"]
            resolved_cube = dict(resolved_cubes[name])
            parent_name = cube.get("extends")
            if parent_name is not None and parent_name in resolved_cubes:
                resolved_cube[_EXTENDS_PARENT_KEY] = parent_name
            elif name in cube_source_models:
                resolved_cube[_DBT_MODEL_NAME_KEY] = cube_source_models[name]
            augmented_cubes_by_name[name] = resolved_cube

        # `get_cube_asset_spec` is `@public`/overridable, and a subclass that renames a cube's
        # `key` -- e.g. via `replace_attributes` on the result, exactly the pattern
        # `dagster_dbt.DbtProjectComponent.get_asset_spec` itself documents -- needs an
        # `extends` dependency to see that *same* renamed key, not the un-renamed one
        # `asset_key_for_cube` alone would compute. Mirrors `DagsterDbtTranslator.get_asset_spec`
        # exactly: resolving a cube's spec (and therefore its dependents' `deps`) always goes
        # through this one memoized, `self.`-dispatched path, so an override applies
        # consistently everywhere the cube is referenced, not just to its own top-level spec.
        self._cube_dicts_by_name = augmented_cubes_by_name
        self._cube_spec_cache: dict[str, dg.AssetSpec] = {}
        return merged

    def build_defs_from_state(
        self, context: dg.ComponentLoadContext, state_path: Path | None
    ) -> dg.Definitions:
        dbt_defs = super().build_defs_from_state(context, state_path)

        # See prepare_state_aware_lookup's own docstring for why this (not self.dbt_project /
        # self.asset_key_for_model, dagster_dbt's own *live* lookups) is what everything below
        # -- _dbt_model_asset_key_or_none, the generated op's name/pool, the sensor's name --
        # must go through instead.
        merged = self.prepare_state_aware_lookup(state_path)
        state_aware_project = self._state_aware_project
        cubes = merged.get("cubes", [])
        views = merged.get("views", [])

        cube_names = list(self._cube_dicts_by_name)
        cube_specs = [self._cube_asset_spec_by_name(name) for name in cube_names]
        view_specs = [self.get_view_asset_spec(view) for view in views]
        specs = cube_specs + view_specs

        # Paired positionally with `cube_names`/`views` -- not derived from `spec.key` (e.g.
        # `spec.key.path[-1]`) -- because a subclass overriding `get_cube_asset_spec`/
        # `get_view_asset_spec` to rename the key is free to make the *last* path segment
        # something other than the cube/view's own name too (`["cube", group, f"{name}_cube"]`
        # is a real example that broke this the first time it was written), not just prepend
        # to it. This lookup (used below to stamp/poll for the *same* code_version Dagster
        # already computed for each asset) has to go by the entities' own `name` field, which
        # no override can change without also changing `cubes`/`views` themselves.
        code_version_by_name = {
            **dict(zip(cube_names, (spec.code_version for spec in cube_specs))),
            **{view["name"]: spec.code_version for view, spec in zip(views, view_specs)},
        }
        # Same reasoning, the other direction -- recovering an entity's own `name` from a
        # *selected* `AssetSpec.key` (context.selected_asset_keys only has keys, not names)
        # without assuming anything about what a subclass's renamed key looks like.
        name_by_key = {
            **{spec.key: name for name, spec in zip(cube_names, cube_specs)},
            **{spec.key: view["name"] for view, spec in zip(views, view_specs)},
        }

        landing_check = self.landing_check
        managed_client = landing_check.build_managed_client() if landing_check is not None else None
        required_resource_keys = {self.promoter_resource_key}
        if landing_check is not None and managed_client is None:
            required_resource_keys.add(landing_check.resource_key)

        @dg.multi_asset(
            specs=specs,
            name=f"{state_aware_project.name}_cubes",
            can_subset=True,
            required_resource_keys=required_resource_keys,
            pool=self.promotion_pool or f"{state_aware_project.name}_cube_promotion",
        )
        def _cube_assets(context: dg.AssetExecutionContext):
            promoter = getattr(context.resources, self.promoter_resource_key)
            # Only stamped when a landing check is actually configured -- keeps promoted YAML
            # byte-identical to today's output for anyone not opting into this feature.
            if landing_check is not None:
                cubes_for_promotion = [
                    with_landing_check_meta(cube, code_version_by_name[cube["name"]]) for cube in cubes
                ]
                views_for_promotion = [
                    with_landing_check_meta(view, code_version_by_name[view["name"]]) for view in views
                ]
            else:
                cubes_for_promotion, views_for_promotion = cubes, views
            with tempfile.TemporaryDirectory() as tmp_dir:
                cubes_dir = Path(tmp_dir) / "cubes"
                views_dir = Path(tmp_dir) / "views"
                write_entities(cubes_dir, "cubes", cubes_for_promotion)
                write_entities(views_dir, "views", views_for_promotion)
                promoter.promote(context, cubes_dir, views_dir)

            if landing_check is not None:
                expected = {
                    name_by_key[spec.key]: spec.code_version
                    for spec in specs
                    if spec.key in context.selected_asset_keys
                }
                client = managed_client or getattr(context.resources, landing_check.resource_key)
                wait_for_landing(
                    client, expected, landing_check.timeout_seconds, landing_check.poll_interval_seconds
                )

            # Multi-assets must yield in topological order. Cube specs always precede view
            # specs in `specs` (views can only depend on cubes, never the reverse), so
            # filtering that order down to what's selected -- rather than iterating
            # `context.selected_asset_keys` directly, an unordered set -- is always valid.
            for spec in specs:
                if spec.key in context.selected_asset_keys:
                    yield dg.MaterializeResult(asset_key=spec.key)

        # A custom sensor scoped to just these assets, rather than relying on the platform's
        # single default_automation_condition_sensor -- Dagster automatically excludes any
        # asset targeted by an explicit AutomationConditionSensorDefinition from the default
        # one, so this doesn't cause double evaluation. default_status=RUNNING because a
        # custom sensor otherwise starts STOPPED (unlike the always-on default sensor), which
        # would silently turn automation off for these assets until someone starts it by hand.
        cube_sensor = dg.AutomationConditionSensorDefinition(
            name=f"{state_aware_project.name}_cube_automation_condition_sensor",
            target=dg.AssetSelection.assets(*[spec.key for spec in specs]),
            default_status=dg.DefaultSensorStatus.RUNNING,
        )

        return dg.Definitions.merge(
            dbt_defs, dg.Definitions(assets=[_cube_assets], sensors=[cube_sensor])
        )

    @public
    def asset_key_for_cube(self, name: str) -> dg.AssetKey:
        return dg.AssetKey([CUBE_KEY_PREFIX, name])

    @public
    def asset_key_for_view(self, name: str) -> dg.AssetKey:
        return dg.AssetKey([VIEW_KEY_PREFIX, name])

    def _cube_asset_spec_by_name(self, name: str) -> dg.AssetSpec:
        """Resolves (and memoizes) a cube's `AssetSpec` by name, always through
        `self.get_cube_asset_spec` -- never `asset_key_for_cube` alone -- so a subclass
        override of `get_cube_asset_spec` (e.g. renaming `key`) is reflected consistently
        whether this cube is being built for its own sake or looked up as another cube's
        `extends` dependency. Safe from infinite recursion: `resolve_extends` (called before
        this is ever populated) already raises `CircularExtendsError` for any `extends` cycle,
        so by the time this runs, every `_EXTENDS_PARENT_KEY` chain is guaranteed acyclic.
        """
        if name not in self._cube_spec_cache:
            self._cube_spec_cache[name] = self.get_cube_asset_spec(self._cube_dicts_by_name[name])
        return self._cube_spec_cache[name]

    def _dbt_model_asset_key_or_none(self, name: str) -> dg.AssetKey | None:
        # Deliberately not `self.asset_key_for_model(name)` -- see the comment in
        # `build_defs_from_state` on why that would need the live dbt project directory to
        # exist at deploy time. Replicates its exact lookup logic, just against the
        # state-aware manifest/project set there instead.
        manifest = self._state_aware_manifest
        matching_model_ids = [
            unique_id
            for unique_id, value in manifest["nodes"].items()
            if value["name"] == name and value["resource_type"] in ASSET_RESOURCE_TYPES
        ]
        if not matching_model_ids:
            return None
        return validate_translator(self.translator).get_asset_spec(
            manifest, next(iter(matching_model_ids)), self._state_aware_project
        ).key

    @public
    def get_cube_asset_spec(self, cube: Mapping[str, Any]) -> dg.AssetSpec:
        """Builds the `AssetSpec` for one generated (and merge-patched) cube. Override this in
        a subclass to customize cube assets, e.g. to set `group_name` or `owners`.

        If you override this to change a cube's `key` (e.g. via `.replace_attributes(key=...)`
        on the returned spec, the same pattern `dagster_dbt.DbtProjectComponent.get_asset_spec`
        itself documents), that renamed key is automatically what a *dependent* cube's `deps`
        points at too when it `extends` this one -- every cube's spec is resolved through this
        same overridable method (recursively, memoized), never through `asset_key_for_cube`
        directly, exactly mirroring how `dagster_dbt`'s own `DagsterDbtTranslator.get_asset_spec`
        keeps a dbt model's renamed key consistent with how its dependents reference it.

        `cube` is extends-resolved: if it (or an ancestor, for a multi-level `extends` chain)
        has `extends: some_parent`, its fields already reflect `some_parent`'s fields folded
        in -- the same way Cube itself resolves `extends` at its own runtime -- so
        `cube.get("description")`/`meta`/etc. see the parent's values when the cube itself
        doesn't override them. This only affects what's visible here; the actual generated
        YAML still carries a literal `extends:` for Cube to resolve itself. One consequence:
        `code_version` (below) now changes whenever an ancestor's fields change too, not just
        the cube's own -- which is desired, since the cube's *effective* definition did
        change, and `GENERATED_ASSET_AUTOMATION_CONDITION` should still re-run it once.

        A cube's single dependency is either the cube it `extends` (its own asset, tracked via
        an internal `__dagster_cube_dbt_extends_parent` key) or, when it has no `extends` (or
        its `extends` target wasn't itself generated), the real dbt model behind it -- tracked
        via `__dagster_cube_dbt_dbt_model_name` for a cube renamed by `meta.cube.name`/
        `suffix`, since it then no longer shares its dbt model's name. Both are stripped here
        before the cube is ever displayed or hashed, so neither appears in this asset's own
        metadata/`code_version`.
        """
        cube = dict(cube)
        name = cube["name"]
        parent_cube_name = cube.pop(_EXTENDS_PARENT_KEY, None)
        source_model_name = cube.pop(_DBT_MODEL_NAME_KEY, name)
        if parent_cube_name is not None:
            # Not `self.asset_key_for_cube(parent_cube_name)` -- see `_cube_asset_spec_by_name`'s
            # docstring for why that would miss a subclass's own key-renaming override.
            dep_key = self._cube_asset_spec_by_name(parent_cube_name).key
        else:
            dep_key = self._dbt_model_asset_key_or_none(source_model_name)
        return dg.AssetSpec(
            key=self.asset_key_for_cube(name),
            deps=[dep_key] if dep_key else [],
            description=cube.get("description"),
            metadata={**_yaml_metadata(cube), **_column_schema(cube)},
            code_version=_code_version(cube),
            automation_condition=GENERATED_ASSET_AUTOMATION_CONDITION,
            kinds={"cube"},
            is_virtual=True,
        )

    @public
    def get_view_asset_spec(self, view: Mapping[str, Any]) -> dg.AssetSpec:
        """Builds the `AssetSpec` for one generated (and merge-patched) view. `deps` are the
        cube assets the view is composed of (its own `cubes:` list) -- a real dependency,
        unlike a cube's query-time `joins`. Override this in a subclass to customize view
        assets.

        A member's `join_path` is a dot-separated chain of cube names (e.g.
        `"orders.customers"`), not necessarily a single cube -- Cube itself uses this to
        express "reach `customers`'s members by joining through `orders`." The cube whose
        members that entry's `includes`/`excludes` actually apply to is the *last* segment,
        not the first -- a real production view (multiple `join_path` entries chained off one
        fact cube, e.g. `"fact.dates"`/`"fact.times"`) broke an earlier version of this that
        took the first segment instead, silently collapsing every multi-hop member onto just
        the fact cube and dropping the rest.

        But every segment, not just the last, is a real dependency: Cube's compiled SQL joins
        through *all* of them to reach the last one, so a cube that only ever appears as an
        intermediate hop (never as its own `join_path` entry) still needs its own edge here --
        a schema or definition change to it changes the view's query just as much as one to the
        terminal cube would, and the freshness/automation-condition propagation this module
        relies on (see `_DEPS_READY` above) only sees what `deps` actually lists.

        Each dependency is resolved through `self._cube_asset_spec_by_name` -- never
        `self.asset_key_for_cube` directly -- for the same override-safety reason
        `get_cube_asset_spec`'s own `extends` dependency is: a subclass renaming a cube's `key`
        must still be reflected here, or the view ends up depending on an asset key that no
        longer exists in the graph at all.
        """
        name = view["name"]
        member_cube_names = {
            segment
            for member in view.get("cubes", [])
            if "join_path" in member
            for segment in str(member["join_path"]).split(".")
        }
        deps = []
        for member_name in sorted(member_cube_names):
            if member_name not in self._cube_dicts_by_name:
                raise dg.DagsterInvalidDefinitionError(
                    f"View {name!r} references cube {member_name!r} (via a join_path) that "
                    "wasn't generated -- check for a typo, or that the cube isn't excluded by "
                    "cube_select."
                )
            deps.append(self._cube_asset_spec_by_name(member_name).key)
        return dg.AssetSpec(
            key=self.asset_key_for_view(name),
            deps=deps,
            description=view.get("description"),
            metadata=_yaml_metadata(view),
            code_version=_code_version(view),
            automation_condition=GENERATED_ASSET_AUTOMATION_CONDITION,
            kinds={"cube"},
            is_virtual=True,
        )
