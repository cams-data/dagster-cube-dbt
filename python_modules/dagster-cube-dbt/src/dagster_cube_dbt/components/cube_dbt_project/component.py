"""`CubeDbtProjectComponent`: extends `dagster_dbt.DbtProjectComponent` to also generate a
Cube semantic-layer schema from the same dbt project's manifest, exposing each generated (and
merge-patched) cube or view as a virtual, pass-through Dagster asset.

See the package README for the full generation and merge-patch model this implements.
"""

import hashlib
import json
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
from dagster_dbt.dbt_manifest import validate_manifest

from dagster_cube_dbt.generation import generate_cubes
from dagster_cube_dbt.merge import discover_patch_files, merge_documents, resolve_extends
from dagster_cube_dbt.output import write_entities

DEFS_YAML_FILENAME = "defs.yaml"
CUBE_STATE_FILENAME = "cube_dbt_state.json"
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
# `code_version_changed()` needs no such wrapping: unlike a one-tick pulse, it stays true
# from the tick the version changes until the tick it's actually evaluated as part of a
# request, so it isn't lost the same way a raw edge-triggered condition would be.
GENERATED_ASSET_AUTOMATION_CONDITION = (
    (
        (
            dg.AutomationCondition.missing()
            & ~dg.AutomationCondition.any_deps_missing()
            & ~dg.AutomationCondition.any_deps_in_progress()
        )
        .newly_true()
        .since_last_handled()
        | dg.AutomationCondition.code_version_changed()
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

        # `state_path` itself is a sentinel file `DbtProjectManager.prepare` touches, not a
        # directory -- its real per-key working directory is `state_path.parent` (already
        # created by the `super()` call above), so our own cached data goes there instead,
        # under a filename that won't collide with dagster_dbt's own "project" subdirectory.
        (state_path.parent / CUBE_STATE_FILENAME).write_text(json.dumps(merged))

    def build_defs_from_state(
        self, context: dg.ComponentLoadContext, state_path: Path | None
    ) -> dg.Definitions:
        dbt_defs = super().build_defs_from_state(context, state_path)

        state_file = state_path.parent / CUBE_STATE_FILENAME if state_path else None
        if state_file is None or not state_file.exists():
            raise dg.DagsterInvalidDefinitionError(
                "No generated cube state found. Run `dg utils refresh-defs-state` to "
                "generate it before loading definitions."
            )

        merged = json.loads(state_file.read_text())
        cubes = merged.get("cubes", [])
        views = merged.get("views", [])
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
        specs = []
        for cube in cubes:
            name = cube["name"]
            resolved_cube = dict(resolved_cubes[name])
            parent_name = cube.get("extends")
            if parent_name is not None and parent_name in resolved_cubes:
                resolved_cube[_EXTENDS_PARENT_KEY] = parent_name
            elif name in cube_source_models:
                resolved_cube[_DBT_MODEL_NAME_KEY] = cube_source_models[name]
            specs.append(self.get_cube_asset_spec(resolved_cube))
        specs += [self.get_view_asset_spec(view) for view in views]

        @dg.multi_asset(
            specs=specs,
            name=f"{self.dbt_project.name}_cubes",
            can_subset=True,
            required_resource_keys={self.promoter_resource_key},
            pool=self.promotion_pool or f"{self.dbt_project.name}_cube_promotion",
        )
        def _cube_assets(context: dg.AssetExecutionContext):
            promoter = getattr(context.resources, self.promoter_resource_key)
            with tempfile.TemporaryDirectory() as tmp_dir:
                cubes_dir = Path(tmp_dir) / "cubes"
                views_dir = Path(tmp_dir) / "views"
                write_entities(cubes_dir, "cubes", cubes)
                write_entities(views_dir, "views", views)
                promoter.promote(context, cubes_dir, views_dir)
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
            name=f"{self.dbt_project.name}_cube_automation_condition_sensor",
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

    def _dbt_model_asset_key_or_none(self, name: str) -> dg.AssetKey | None:
        try:
            return self.asset_key_for_model(name)
        except KeyError:
            return None

    @public
    def get_cube_asset_spec(self, cube: Mapping[str, Any]) -> dg.AssetSpec:
        """Builds the `AssetSpec` for one generated (and merge-patched) cube. Override this in
        a subclass to customize cube assets, e.g. to set `group_name` or `owners`.

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
            dep_key = self.asset_key_for_cube(parent_cube_name)
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
        """
        name = view["name"]
        member_cube_names = {
            str(member["join_path"]).split(".")[0]
            for member in view.get("cubes", [])
            if "join_path" in member
        }
        return dg.AssetSpec(
            key=self.asset_key_for_view(name),
            deps=[self.asset_key_for_cube(member_name) for member_name in sorted(member_cube_names)],
            description=view.get("description"),
            metadata=_yaml_metadata(view),
            code_version=_code_version(view),
            automation_condition=GENERATED_ASSET_AUTOMATION_CONDITION,
            kinds={"cube"},
            is_virtual=True,
        )
