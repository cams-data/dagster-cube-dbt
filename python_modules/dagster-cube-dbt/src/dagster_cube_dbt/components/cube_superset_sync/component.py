"""`CubeSupersetSyncComponent`: reads a sibling `CubeDbtProjectComponent`'s already-generated
cube/view state and syncs each Cube **view** into an Apache Superset dataset (column
descriptions, groupby/filter flags, and one metric per measure) via `SupersetResource`.

A standalone, chained component rather than a `CubeDbtProjectComponent` subclass -- see
SUPERSET_SYNC_PLAN.md's "Architecture decision" section for the full reasoning. In short: this
avoids duplicating `project:`/`cube_select:`/merge-patch config across two `defs.yaml` blocks,
and every dependency this component emits is resolved through the sibling's own overridable
methods (`get_view_asset_spec`, `asset_key_for_view`), never by guessing at its `AssetKey`
shape -- so a subclass renaming view keys (the exact scenario DECISIONS.md Phase 37/40 already
had to fix twice elsewhere in this codebase) is respected automatically here too.
"""

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated, Any

import dagster as dg
from dagster._annotations import public
from dagster._core.definitions.definitions_load_context import DefinitionsLoadContext
from dagster._core.storage.defs_state.base import DefsStateStorage
from dagster.components.resolved.model import Resolver

from dagster_cube_dbt.components.cube_dbt_project.component import (
    GENERATED_ASSET_AUTOMATION_CONDITION,
    CubeDbtProjectComponent,
)
from dagster_cube_dbt.merge import resolve_extends
from dagster_cube_dbt.superset_resource import SupersetResource

SUPERSET_DATASET_KEY_PREFIX = "superset_dataset"

# Cube's SQL API (what a Superset "Cube" database connection actually queries against) always
# exposes cubes/views under this fixed schema -- confirmed against the dbt-to-cube reference
# implementation investigated in SUPERSET_SYNC_PLAN.md, not configurable on Cube's side, so
# not exposed as a config field here either.
_CUBE_SQL_API_SCHEMA = "public"


def _resolve_view_members(
    view: Mapping[str, Any], resolved_cubes: Mapping[str, Mapping[str, Any]]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Resolves the dimensions/measures a view exposes by walking its own `cubes:`
    (`join_path`/`includes`/`excludes`) declarations against the referenced (extends-resolved)
    member cubes' own `dimensions`/`measures` -- the same `"*"` / list `includes` and list
    `excludes` shapes Cube's own view schema supports. Cube's `prefix`/member-aliasing option
    isn't handled here -- not exercised by this project's own generation output or any
    fixture/production case so far; a view using it will under-resolve, a natural follow-up if
    that turns out to matter.

    A member's `join_path` is a dot-separated chain of cube names (e.g. `"orders.customers"`),
    not necessarily a single cube -- the cube that entry's `includes`/`excludes` actually apply
    to is the *last* segment, matching `CubeDbtProjectComponent.get_view_asset_spec`'s own
    dependency resolution (see its docstring for the real production view that broke an
    earlier, first-segment version of both of these).
    """
    dimensions_by_name: dict[str, dict[str, Any]] = {}
    measures_by_name: dict[str, dict[str, Any]] = {}
    for member in view.get("cubes", []):
        join_path = member.get("join_path")
        if join_path is None:
            continue
        cube = resolved_cubes.get(str(join_path).split(".")[-1])
        if cube is None:
            continue
        includes = member.get("includes", "*")
        excludes = set(member.get("excludes") or [])
        for dimension in cube.get("dimensions", []):
            name = dimension["name"]
            if name in excludes or (includes != "*" and name not in includes):
                continue
            dimensions_by_name[name] = dimension
        for measure in cube.get("measures", []):
            name = measure["name"]
            if name in excludes or (includes != "*" and name not in includes):
                continue
            measures_by_name[name] = measure
    return list(dimensions_by_name.values()), list(measures_by_name.values())


def _dataset_metadata(
    view: Mapping[str, Any], resolved_cubes: Mapping[str, Mapping[str, Any]]
) -> dict[str, dg.MetadataValue]:
    dimensions, measures = _resolve_view_members(view, resolved_cubes)
    columns = [
        dg.TableColumn(
            name=dimension["name"],
            type=dimension.get("type", "unknown"),
            description=dimension.get("description"),
            tags={"dagster_cube_dbt/member_type": "dimension"},
        )
        for dimension in dimensions
    ] + [
        dg.TableColumn(
            name=measure["name"],
            type=measure.get("type", "unknown"),
            description=measure.get("description"),
            tags={"dagster_cube_dbt/member_type": "measure"},
        )
        for measure in measures
    ]
    return {"dagster/column_schema": dg.TableSchema(columns=columns)}


def _sync_pool_name(dbt_cube_component: str) -> str:
    slug = re.sub(r"[^0-9a-zA-Z_]+", "_", dbt_cube_component.strip("/")) or "default"
    return f"{slug}_superset_sync"


@dataclass
class CubeSupersetSyncComponent(dg.Component, dg.Resolvable):
    """For every view generated by the `CubeDbtProjectComponent` at `dbt_cube_component`,
    creates (or updates) a matching Apache Superset dataset: `verbose_name`/`description`/
    `groupby`/`filterable` on each column from the view's dimensions, and one metric per
    measure, expressed via Cube SQL API's own aggregation-pushdown syntax
    (`MEASURE(<view>.<measure>)`) so Cube -- not Superset -- performs the aggregation.

    Reads the sibling component's already-generated, cached state (the same
    `cube_dbt_state.json` `write_state_to_path` produces) rather than recomputing anything --
    no live dbt project needed at deploy time, and no `project:`/`cube_select:`/merge-patch
    config duplicated here. See this module's docstring for the full rationale.

    Needs a `SupersetResource` and a Superset database connection named `database_name`
    (`"Cube"` by default) already pointed at Cube's own SQL API -- this component doesn't
    create that connection, the same way `CubeDbtProjectComponent` doesn't provision a running
    Cube instance. Two ways to provide the resource, chosen by whether `base_url` is set:

    - **Set `base_url` (and either `username`/`password` or `api_key`)** and this component
      builds and owns a `SupersetResource` itself, directly from these attributes -- the common
      case, since there's only one real `SupersetResource` implementation:

            # defs.yaml
            type: dagster_cube_dbt.CubeSupersetSyncComponent
            attributes:
              dbt_cube_component: "dbt_ingest"  # relative to defs/, not to this file's own directory
              base_url: "https://superset.example.com"
              username: "{{ env.SUPERSET_USERNAME }}"
              password: "{{ env.SUPERSET_PASSWORD }}"

      Or, for an account authenticated via LDAP/OIDC SSO (which has no password this component
      could log in with) -- a Superset API key instead, see `SupersetResource`'s own docstring:

            # defs.yaml
            type: dagster_cube_dbt.CubeSupersetSyncComponent
            attributes:
              dbt_cube_component: "dbt_ingest"
              base_url: "https://superset.example.com"
              api_key: "{{ env.SUPERSET_API_KEY }}"

    - **Leave `base_url` unset** and this falls back to looking up a `SupersetResource` bound
      under `superset_resource_key` (`"superset"` by default) as an ordinary Dagster resource --
      for a test double, or one instance shared across multiple components:

            # defs.yaml
            type: dagster_cube_dbt.CubeSupersetSyncComponent
            attributes:
              dbt_cube_component: "dbt_ingest"  # relative to defs/, not to this file's own directory

            # e.g. defs/resources.py
            import dagster as dg
            from dagster_cube_dbt import SupersetResource

            @dg.definitions
            def resources():
                return dg.Definitions(
                    resources={
                        "superset": SupersetResource(
                            base_url="https://superset.example.com",
                            username=dg.EnvVar("SUPERSET_USERNAME"),
                            password=dg.EnvVar("SUPERSET_PASSWORD"),
                        )
                    }
                )

      `superset_resource_key` currently still defaults to `"superset"` for backwards
      compatibility; a future release will remove that default, requiring it to be set
      explicitly whenever this external-resource path is what you actually want, so an
      incomplete `base_url`-less, `superset_resource_key`-less config is unambiguously reported
      as *missing configuration* rather than a *missing resource*.

    Views only, not cubes -- Cube's own convention is that views are the intended BI-facing
    query layer. Reuses `GENERATED_ASSET_AUTOMATION_CONDITION`: a dataset only needs updating
    when the view's own generated definition changes (its `code_version`, read straight from
    the sibling's own `get_view_asset_spec`), not on every dbt data refresh underneath it.
    """

    dbt_cube_component: Annotated[
        str,
        Resolver.default(
            description="Path to the CubeDbtProjectComponent's defs.yaml directory, resolved "
            "relative to the project's top-level defs directory -- NOT relative to this "
            "component's own defs.yaml, so a sibling directory needs no leading '../' (e.g. "
            "'dbt_ingest', not '../dbt_ingest'). Passed straight to context.load_component, "
            "which resolves it this same way.",
        ),
    ]
    database_name: Annotated[
        str,
        Resolver.default(description="Name of the Superset database connection pointed at Cube's SQL API."),
    ] = field(default="Cube", kw_only=True)
    base_url: Annotated[
        str | None,
        Resolver.default(
            description="Superset deployment's base URL. Setting this (with either "
            "username+password or api_key) means this component builds and owns its own "
            "SupersetResource directly, instead of looking up one bound under "
            "superset_resource_key.",
        ),
    ] = field(default=None, kw_only=True)
    username: Annotated[
        str | None,
        Resolver.default(
            description="Superset username. Required whenever base_url is set, unless api_key "
            "is set instead.",
        ),
    ] = field(default=None, kw_only=True)
    password: Annotated[
        str | None,
        Resolver.default(
            description="Superset password. Required whenever base_url is set (unless api_key "
            "is set instead) -- use '{{ env.SOME_VAR }}' templating rather than a literal "
            "value in checked-in defs.yaml.",
        ),
    ] = field(default=None, kw_only=True)
    api_key: Annotated[
        str | None,
        Resolver.default(
            description="Superset API key (used directly as a Bearer token) -- alternative to "
            "username+password, for an account authenticated via LDAP/OIDC SSO. See "
            "SupersetResource's own docstring for the Superset-side setup required. Set this "
            "OR username+password, not both.",
        ),
    ] = field(default=None, kw_only=True)
    verify_tls: Annotated[
        bool,
        Resolver.default(
            description="Passed straight through to the component-managed SupersetResource's "
            "own verify_tls (only meaningful when base_url is set).",
        ),
    ] = field(default=True, kw_only=True)
    superset_resource_key: Annotated[
        str,
        Resolver.default(
            description="Resource key of the SupersetResource this component syncs through, "
            "when base_url is left unset -- bound like any other Dagster resource, doesn't "
            "need to be declared in this defs.yaml.",
        ),
    ] = field(default="superset", kw_only=True)
    sync_pool: Annotated[
        str | None,
        Resolver.default(
            description="Dagster concurrency pool assigned to the dataset-sync multi-asset's "
            "op. Defaults to a pool name scoped to dbt_cube_component, so a max concurrency of "
            "1 can be set for it in the Dagster UI (Deployment > Concurrency) if two runs "
            "syncing the same Superset dataset concurrently turns out to be a problem.",
        ),
    ] = field(default=None, kw_only=True)

    def build_managed_resource(self) -> SupersetResource | None:
        """Returns a `SupersetResource` built directly from `base_url` and either
        `username`/`password` or `api_key` if `base_url` is set (the component-managed path),
        `None` if it's unset (the caller should fall back to `superset_resource_key` instead).
        Raises clearly, rather than deferring to a confusing `pydantic` error, if `base_url` is
        set but authentication is missing or ambiguous -- that combination unambiguously means
        the user intended the managed path but didn't finish configuring it, not that they meant
        the external-resource path instead.
        """
        if self.base_url is None:
            return None
        has_api_key = self.api_key is not None
        has_password_auth = self.username is not None or self.password is not None
        missing = [name for name in ("username", "password") if getattr(self, name) is None]
        if has_api_key and has_password_auth:
            raise dg.DagsterInvalidDefinitionError(
                "base_url is set with both api_key and username/password configured -- set "
                "one authentication method, not both."
            )
        if not has_api_key and missing:
            raise dg.DagsterInvalidDefinitionError(
                "base_url is set, which means this component should build and manage its own "
                "SupersetResource directly, but no authentication is configured "
                f"({' and '.join(missing)} missing). Set base_url with either api_key, or both "
                "username and password, together -- or remove base_url entirely (and set "
                "superset_resource_key) to bind an existing SupersetResource externally instead."
            )
        return SupersetResource(
            base_url=self.base_url,
            username=self.username,
            password=self.password,
            api_key=self.api_key,
            verify_tls=self.verify_tls,
        )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        sibling = context.load_component(self.dbt_cube_component, CubeDbtProjectComponent)
        # `DefinitionsLoadContext`/`DefsStateStorage` aren't re-exported from dagster's public
        # `dagster`/`dagster.components` surface as of this writing -- imported from their real
        # (private-module-path) location instead, the same modules
        # `StateBackedComponent.build_defs` itself imports internally to implement this exact
        # state_path() contract. See SUPERSET_SYNC_PLAN.md for why this mechanism (not
        # reimplementing the sibling's state key by hand) is still the right call despite that.
        with DefinitionsLoadContext.get().state_path(
            sibling.defs_state_config, DefsStateStorage.get(), context.project_root
        ) as state_path:
            return self.build_defs_from_sibling_state(context, sibling, state_path)

    def build_defs_from_sibling_state(
        self,
        context: dg.ComponentLoadContext,
        sibling: CubeDbtProjectComponent,
        state_path: Path | None,
    ) -> dg.Definitions:
        """The pure core of `build_defs`, split out (mirroring
        `CubeDbtProjectComponent.write_state_to_path`/`build_defs_from_state`'s own split) so
        it can be exercised directly against a manually constructed sibling instance and state
        path, without needing a real on-disk defs tree / `context.load_component` resolution.
        """
        # `sibling` was only *loaded* (via context.load_component or a direct constructor in
        # tests) -- its own build_defs/build_defs_from_state has never run, so none of the
        # state `get_cube_asset_spec`/`get_view_asset_spec` depend on exists on it yet. This
        # populates it, entirely from the same cached state read below -- no live dbt project
        # needed, and no separate call to the sibling's own build_defs required.
        merged = sibling.prepare_state_aware_lookup(state_path)
        views = merged.get("views", [])
        resolved_cubes = resolve_extends(merged.get("cubes", []))

        self._sibling = sibling
        self._resolved_cubes = resolved_cubes
        specs = [self.get_dataset_asset_spec(view) for view in views]
        view_name_by_key = {spec.key: view["name"] for view, spec in zip(views, specs)}
        views_by_name = {view["name"]: view for view in views}

        managed_resource = self.build_managed_resource()
        required_resource_keys = set() if managed_resource is not None else {self.superset_resource_key}

        @dg.multi_asset(
            specs=specs,
            name="superset_datasets",
            can_subset=True,
            required_resource_keys=required_resource_keys,
            pool=self.sync_pool or _sync_pool_name(self.dbt_cube_component),
        )
        def _superset_dataset_assets(context: dg.AssetExecutionContext):
            superset = managed_resource or getattr(context.resources, self.superset_resource_key)
            for spec in specs:
                if spec.key not in context.selected_asset_keys:
                    continue
                name = view_name_by_key[spec.key]
                dimensions, measures = _resolve_view_members(views_by_name[name], resolved_cubes)
                superset.sync_dataset(
                    database_name=self.database_name,
                    schema=_CUBE_SQL_API_SCHEMA,
                    table_name=name,
                    dimensions=dimensions,
                    measures=measures,
                )
                yield dg.MaterializeResult(asset_key=spec.key)

        # Without this, these assets' `automation_condition=GENERATED_ASSET_AUTOMATION_CONDITION`
        # (set on their specs above) falls to the platform's single default
        # `default_automation_condition_sensor` instead -- which still evaluates the condition
        # correctly, but means these assets can't be started/stopped/observed independently of
        # every other automation-condition asset in the deployment. Scoped and always-on for the
        # same reason `CubeDbtProjectComponent.build_defs_from_state`'s own `cube_sensor` is (see
        # its comment): Dagster automatically excludes any asset targeted by an explicit
        # `AutomationConditionSensorDefinition` from the default one, so this doesn't cause
        # double evaluation, and `default_status=RUNNING` avoids automation silently going off
        # for these assets until someone starts a custom sensor by hand.
        superset_sensor = dg.AutomationConditionSensorDefinition(
            name=f"{_sync_pool_name(self.dbt_cube_component)}_superset_dataset_automation_condition_sensor",
            target=dg.AssetSelection.assets(*[spec.key for spec in specs]),
            default_status=dg.DefaultSensorStatus.RUNNING,
        )

        return dg.Definitions(assets=[_superset_dataset_assets], sensors=[superset_sensor])

    @public
    def asset_key_for_dataset(self, name: str) -> dg.AssetKey:
        return dg.AssetKey([SUPERSET_DATASET_KEY_PREFIX, name])

    @public
    def get_dataset_asset_spec(self, view: Mapping[str, Any]) -> dg.AssetSpec:
        """Builds the `AssetSpec` for one Superset dataset. Override this in a subclass to
        customize dataset assets, e.g. to set `group_name` or `owners`.

        The dataset's single dependency is the sibling's own view asset (`sibling.
        get_view_asset_spec(view).key`, not `sibling.asset_key_for_view(name)` directly) --
        resolved through the sibling's own overridable method so a subclass of
        `CubeDbtProjectComponent` that renames a view's key is still respected here, the same
        override-safe pattern `CubeDbtProjectComponent.get_cube_asset_spec` itself documents.
        `code_version` is read from that same sibling spec rather than recomputed, so this
        dataset only needs updating exactly when the view's own generated definition
        (including anything a `get_view_asset_spec` override changes about it) actually does.
        """
        name = view["name"]
        sibling_view_spec = self._sibling.get_view_asset_spec(view)
        return dg.AssetSpec(
            key=self.asset_key_for_dataset(name),
            deps=[sibling_view_spec.key],
            description=view.get("description"),
            metadata=_dataset_metadata(view, self._resolved_cubes),
            code_version=sibling_view_spec.code_version,
            automation_condition=GENERATED_ASSET_AUTOMATION_CONDITION,
            kinds={"superset"},
        )
