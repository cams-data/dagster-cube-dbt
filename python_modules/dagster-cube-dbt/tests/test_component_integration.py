"""End-to-end tests against the real fixture dbt project (already `dbt parse`d, see
tests/fixtures/dbt_project). Constructs `CubeDbtProjectComponent` directly with a
pre-instantiated `DbtProject` (the `NoopDbtProjectManager` path) rather than going through
YAML/`Resolver` resolution, so these focus on `write_state_to_path` / `build_defs_from_state`
logic rather than the YAML-loading plumbing.
"""

import json
from pathlib import Path

import dagster as dg
import pytest
from dagster_dbt.dbt_project import DbtProject

from dagster_cube_dbt.components.cube_dbt_project.component import (
    CUBE_STATE_FILENAME,
    CubeDbtProjectComponent,
    CubeSelect,
)
from dagster_cube_dbt.output import read_entities
from dagster_cube_dbt.resources import CubeFilePromoter, LocalFileCubeFilePromoter
from dbt_engine import DBT_TARGET

FIXTURE_DBT_PROJECT = Path(__file__).parent / "fixtures" / "dbt_project"

JOURNEY_SAMPLES_PATCH = """
cubes:
  - name: journey_samples
    $mergeStrategy: patch
    measures:
      - name: count
        type: count
    joins:
      - name: destination_locations
        relationship: many_to_one
        sql: "{CUBE}.destination_location_key = {destination_locations.geographic_location_key}"
      - name: origin_locations
        relationship: many_to_one
        sql: "{CUBE}.origin_location_key = {origin_locations.geographic_location_key}"
      - name: dates
        relationship: many_to_one
        sql: "{CUBE}.date_key = {dates.date_key}"
views:
  - name: journeys_overview
    cubes:
      - join_path: journey_samples
        includes: "*"
      - join_path: destination_locations
        includes: "*"
"""

EXCHANGE_RATES_PATCH = """
cubes:
  - name: exchange_rates
    sql: "SELECT * FROM some_external_source"
"""

ALL_GENERATED_CUBE_NAMES = {
    "journey_samples",
    "destination_locations",
    "origin_locations",
    "dates",
    "exchange_rates",
}


def _recording_promoter() -> tuple[CubeFilePromoter, list]:
    """Builds a `CubeFilePromoter` test double plus a plain list its `promote()` calls are
    recorded into. A list captured by closure, not an instance/pydantic attribute, because
    `ConfigurableResource` instances are copied when a run initializes them -- the object
    actually invoked inside the op is never the same Python object the test holds a
    reference to, so any recording that lived on `self` wouldn't be observable here; the
    closure is shared by every copy since they all run the same underlying function object.
    """
    calls: list = []

    class _RecordingCubeFilePromoter(CubeFilePromoter):
        def promote(self, context: dg.AssetExecutionContext, cubes_dir: Path, views_dir: Path) -> None:
            calls.append(
                {
                    "run_id": context.run.run_id,
                    "cubes_dir": cubes_dir,
                    "cube_names": {c["name"] for c in read_entities(cubes_dir, "cubes")},
                }
            )

    return _RecordingCubeFilePromoter(), calls


class FailingCubeFilePromoter(CubeFilePromoter):
    def promote(self, context: dg.AssetExecutionContext, cubes_dir: Path, views_dir: Path) -> None:
        raise RuntimeError("upload failed")


class NoopCubeFilePromoter(CubeFilePromoter):
    """A resource satisfying `cube_file_promoter` for tests that only care about specs/
    automation conditions, never actual materialization -- Dagster validates that every
    required resource key is bound as soon as a repository/asset graph is built (even without
    executing anything), so these need *something* bound, just not one that does real work.
    """

    def promote(self, context: dg.AssetExecutionContext, cubes_dir: Path, views_dir: Path) -> None:
        return


def _make_component(defs_dir: Path, **kwargs) -> CubeDbtProjectComponent:
    project = DbtProject(project_dir=FIXTURE_DBT_PROJECT, target=DBT_TARGET)
    component = CubeDbtProjectComponent(
        project=project,
        cube_select=CubeSelect(paths=["marts"]),
        **kwargs,
    )
    component._defs_dir = defs_dir  # normally set by CubeDbtProjectComponent.load()
    return component


def _with_promoter(
    defs: dg.Definitions, promoter: CubeFilePromoter, key: str = "cube_file_promoter"
) -> dg.Definitions:
    return dg.Definitions.merge(defs, dg.Definitions(resources={key: promoter}))


def _read_state(state_path: Path) -> dict:
    return json.loads((state_path.parent / CUBE_STATE_FILENAME).read_text())


@pytest.fixture
def defs_dir(tmp_path) -> Path:
    directory = tmp_path / "defs"
    directory.mkdir()
    (directory / "defs.yaml").write_text("type: dagster_cube_dbt.CubeDbtProjectComponent\n")
    (directory / "journey_samples_patch.yaml").write_text(JOURNEY_SAMPLES_PATCH)
    (directory / "exchange_rates_patch.yaml").write_text(EXCHANGE_RATES_PATCH)
    return directory


def test_write_state_to_path_caches_merged_cube_and_view_data(tmp_path, defs_dir):
    component = _make_component(defs_dir)
    state_path = tmp_path / "state"

    component.write_state_to_path(state_path)

    merged = _read_state(state_path)
    cubes = {c["name"]: c for c in merged["cubes"]}
    views = {v["name"]: v for v in merged["views"]}

    # dbt-derived cubes, restricted to models/marts by cube_select
    assert set(cubes) == ALL_GENERATED_CUBE_NAMES
    assert "int_raw_journey_events" not in cubes  # excluded by cube_select

    journey_samples = cubes["journey_samples"]
    assert journey_samples["description"] == "Individual journey samples."
    assert journey_samples["measures"] == [{"name": "count", "type": "count"}]
    assert len(journey_samples["joins"]) == 3
    dimension_names = [d["name"] for d in journey_samples["dimensions"]]
    assert "internal_row_hash" not in dimension_names  # meta.cube.dimension: false
    assert "journey_sample_key" in dimension_names
    [pk_dimension] = [d for d in journey_samples["dimensions"] if d["name"] == "journey_sample_key"]
    assert pk_dimension["primary_key"] is True
    assert pk_dimension["type"] == "number"

    exchange_rates = cubes["exchange_rates"]
    assert "dimensions" not in exchange_rates  # not dbt-derived, patch content only

    assert set(views) == {"journeys_overview"}
    assert views["journeys_overview"]["cubes"] == [
        {"join_path": "journey_samples", "includes": "*"},
        {"join_path": "destination_locations", "includes": "*"},
    ]


def test_write_state_to_path_writes_no_files_outside_the_state_cache(tmp_path, defs_dir):
    """Generation and merge-patching happen during state refresh, but delivering the result
    anywhere is a materialization-time concern (the bound `CubeFilePromoter` resource) --
    `write_state_to_path` must not touch the filesystem outside `state_path` at all, not even
    when a `LocalFileCubeFilePromoter` is what's eventually bound to promote it.
    """
    output_dir = tmp_path / "cubes"
    component = _make_component(defs_dir)
    state_path = tmp_path / "state"

    component.write_state_to_path(state_path)

    assert not output_dir.exists()


def test_build_defs_from_state_wires_asset_specs_correctly(tmp_path, defs_dir):
    component = _make_component(defs_dir)
    state_path = tmp_path / "state"
    component.write_state_to_path(state_path)

    context = dg.ComponentTree.for_test().load_context
    defs = _with_promoter(component.build_defs_from_state(context, state_path), NoopCubeFilePromoter())

    asset_graph = defs.resolve_asset_graph()
    journey_samples_key = component.asset_key_for_cube("journey_samples")
    exchange_rates_key = component.asset_key_for_cube("exchange_rates")
    view_key = component.asset_key_for_view("journeys_overview")

    assert journey_samples_key in asset_graph.get_all_asset_keys()
    assert exchange_rates_key in asset_graph.get_all_asset_keys()
    assert view_key in asset_graph.get_all_asset_keys()

    # a dbt-backed cube depends on its own dbt model asset only
    journey_samples_deps = asset_graph.get(journey_samples_key).parent_keys
    assert journey_samples_deps == {component.asset_key_for_model("journey_samples")}

    # a patch-only cube with no backing dbt model has no automatic deps
    exchange_rates_deps = asset_graph.get(exchange_rates_key).parent_keys
    assert exchange_rates_deps == set()

    # a view depends on the cube assets it's composed of
    view_deps = asset_graph.get(view_key).parent_keys
    assert view_deps == {
        component.asset_key_for_cube("journey_samples"),
        component.asset_key_for_cube("destination_locations"),
    }

    # cube and view assets both carry the "cube" kind tag for the UI
    assert asset_graph.get(journey_samples_key).kinds == {"cube"}
    assert asset_graph.get(view_key).kinds == {"cube"}


def test_cube_asset_spec_has_column_schema_metadata_from_dimensions_and_measures(tmp_path, defs_dir):
    """`dagster/column_schema` (not `dagster/column_lineage` -- that's for real column-level
    dependency edges onto upstream asset columns, out of scope here) should list every
    dimension and measure as a `TableColumn`, with Cube's own type and description where
    present -- and it's static `AssetSpec` metadata, so it's visible even if the asset is
    never materialized, same as everything else about these virtual assets.
    """
    component = _make_component(defs_dir)
    state_path = tmp_path / "state"
    component.write_state_to_path(state_path)

    context = dg.ComponentTree.for_test().load_context
    defs = _with_promoter(component.build_defs_from_state(context, state_path), NoopCubeFilePromoter())
    asset_graph = defs.resolve_asset_graph()

    journey_samples_key = component.asset_key_for_cube("journey_samples")
    schema = asset_graph.get(journey_samples_key).metadata["dagster/column_schema"]
    assert isinstance(schema, dg.TableSchema)

    columns_by_name = {column.name: column for column in schema.columns}
    # internal_row_hash is excluded via meta.cube.dimension: false -- must not appear
    assert set(columns_by_name) == {
        "journey_sample_key",
        "journey_type",
        "direction",
        "destination_location_key",
        "origin_location_key",
        "date_key",
        "count",  # the measure the patch adds
    }

    journey_type = columns_by_name["journey_type"]
    assert journey_type.type == "string"
    assert journey_type.description == "The type of journey."
    assert journey_type.tags == {"dagster_cube_dbt/member_type": "dimension"}
    assert journey_type.constraints.unique is False  # not part of the primary key
    assert journey_type.constraints.nullable is True

    count_measure = columns_by_name["count"]
    assert count_measure.type == "count"
    assert count_measure.tags == {"dagster_cube_dbt/member_type": "measure"}

    # journey_sample_key is a single-column primary key (detected from unique+not_null tests)
    pk_column = columns_by_name["journey_sample_key"]
    assert pk_column.constraints.unique is True
    assert pk_column.constraints.nullable is False
    assert pk_column.constraints.other == ["primary key"]
    # a single-column key needs no table-level constraint -- the column-level one says it all
    assert schema.constraints.other == []


def test_cube_asset_spec_column_schema_marks_a_composite_primary_key_correctly(tmp_path, defs_dir):
    """No single dimension in a composite key is unique on its own -- only the combination of
    all of them is -- so column-level `unique` must not be set for any of them individually;
    the table-level constraint is what actually states the composite key.
    """
    (defs_dir / "composite_key_patch.yaml").write_text(
        "cubes:\n"
        "  - name: composite_key_cube\n"
        "    sql: SELECT 1\n"
        "    dimensions:\n"
        "      - name: tenant_id\n"
        "        sql: tenant_id\n"
        "        type: string\n"
        "        primary_key: true\n"
        "      - name: order_date\n"
        "        sql: order_date\n"
        "        type: time\n"
        "        primary_key: true\n"
        "      - name: status\n"
        "        sql: status\n"
        "        type: string\n"
    )
    component = _make_component(defs_dir)
    state_path = tmp_path / "state"
    component.write_state_to_path(state_path)

    context = dg.ComponentTree.for_test().load_context
    defs = _with_promoter(component.build_defs_from_state(context, state_path), NoopCubeFilePromoter())
    asset_graph = defs.resolve_asset_graph()

    key = component.asset_key_for_cube("composite_key_cube")
    schema = asset_graph.get(key).metadata["dagster/column_schema"]
    columns_by_name = {column.name: column for column in schema.columns}

    for pk_name in ("tenant_id", "order_date"):
        constraints = columns_by_name[pk_name].constraints
        assert constraints.unique is False  # not individually unique -- only the pair is
        assert constraints.nullable is False
        assert constraints.other == ["primary key"]

    assert columns_by_name["status"].constraints.unique is False
    assert columns_by_name["status"].constraints.other == []

    assert schema.constraints.other == ["primary key: (order_date, tenant_id)"]


def test_cube_and_view_assets_are_virtual_and_freshness_looks_through_them(tmp_path, defs_dir):
    """Cube/view assets are `is_virtual=True`, so Dagster's own staleness engine treats them
    as transparent -- a downstream consumer's freshness should resolve straight through the
    virtual layer to the real dbt model, not stop at the cube/view. Verified through a real
    chain: view -> cube -> dbt model, exercising `get_non_virtual_ancestor_keys`, the actual
    mechanism the staleness resolver uses (see DECISIONS.md for the full explanation).
    """
    component = _make_component(defs_dir)
    state_path = tmp_path / "state"
    component.write_state_to_path(state_path)

    context = dg.ComponentTree.for_test().load_context
    defs = _with_promoter(component.build_defs_from_state(context, state_path), NoopCubeFilePromoter())
    asset_graph = defs.resolve_asset_graph()

    journey_samples_key = component.asset_key_for_cube("journey_samples")
    exchange_rates_key = component.asset_key_for_cube("exchange_rates")
    view_key = component.asset_key_for_view("journeys_overview")
    model_key = component.asset_key_for_model("journey_samples")

    assert asset_graph.get(journey_samples_key).is_virtual
    assert asset_graph.get(view_key).is_virtual
    assert asset_graph.get(exchange_rates_key).is_virtual  # every cube/view is virtual, ...
    assert not asset_graph.get(model_key).is_virtual  # ... but the dbt model behind it isn't

    # journey_samples cube -> dbt model: resolves straight through the virtual cube
    assert asset_graph.get_non_virtual_ancestor_keys(journey_samples_key) == {model_key}

    # view depends on both journey_samples and destination_locations cubes, so its
    # non-virtual ancestors are both of those cubes' dbt models, reached straight through
    # two hops of virtual assets (view -> cube -> dbt model)
    assert asset_graph.get_non_virtual_ancestor_keys(view_key) == {
        model_key,
        component.asset_key_for_model("destination_locations"),
    }

    assert asset_graph.get_non_virtual_ancestor_keys(exchange_rates_key) == set()


def test_get_cube_asset_spec_sees_extends_resolved_fields(tmp_path, defs_dir):
    """A cube introduced via a patch that `extends` another cube should have the parent's
    fields (here, `journey_samples`' dbt-sourced `description`) reflected in its own
    AssetSpec, even though its own patch fragment never declares one -- while the cached/
    generated document (what actually gets written to disk and promoted) keeps `extends:` as
    a literal field for Cube to resolve itself, unflattened.
    """
    (defs_dir / "extends_patch.yaml").write_text(
        "cubes:\n"
        "  - name: journey_samples_extended\n"
        "    extends: journey_samples\n"
        "    title: An Extended View\n"
    )
    component = _make_component(defs_dir)
    state_path = tmp_path / "state"
    component.write_state_to_path(state_path)

    # the cached state -- and therefore whatever gets promoted -- keeps `extends:` intact.
    merged = _read_state(state_path)
    extended_cube = next(c for c in merged["cubes"] if c["name"] == "journey_samples_extended")
    assert extended_cube["extends"] == "journey_samples"
    assert "description" not in extended_cube  # not flattened here -- Cube resolves this itself

    context = dg.ComponentTree.for_test().load_context
    defs = _with_promoter(component.build_defs_from_state(context, state_path), NoopCubeFilePromoter())
    asset_graph = defs.resolve_asset_graph()

    extended_key = component.asset_key_for_cube("journey_samples_extended")
    node = asset_graph.get(extended_key)
    # inherited from journey_samples via extends, not present on the cube's own patch fragment
    assert node.description == "Individual journey samples."
    # depends on the journey_samples *cube* asset (its extends target), not the dbt model
    assert node.parent_keys == {component.asset_key_for_cube("journey_samples")}


def test_get_cube_asset_spec_resolves_dbt_model_dependency_after_a_rename(tmp_path, defs_dir):
    """A cube renamed via `meta.cube.name`/`suffix` (unit-tested directly against
    `generate_cubes` in test_generation.py) no longer shares its dbt model's name --
    `build_defs_from_state` must still resolve the *real* dbt model dependency (needed for
    freshness/lineage propagation through the virtual cube asset) using `cube_source_models`,
    not the cube's own (now different) `name`. Simulated here by rewriting the cached state
    after a real `write_state_to_path` run, rather than editing the shared fixture's
    schema.yml (which many other tests also depend on the exact shape of).
    """
    component = _make_component(defs_dir)
    state_path = tmp_path / "state"
    component.write_state_to_path(state_path)

    state_file = state_path.parent / CUBE_STATE_FILENAME
    merged = json.loads(state_file.read_text())
    for cube in merged["cubes"]:
        if cube["name"] == "dates":
            cube["name"] = "dates_base"
            cube["public"] = False
    merged["cube_source_models"] = {"dates_base": "dates"}
    state_file.write_text(json.dumps(merged))

    context = dg.ComponentTree.for_test().load_context
    defs = _with_promoter(component.build_defs_from_state(context, state_path), NoopCubeFilePromoter())
    asset_graph = defs.resolve_asset_graph()

    renamed_key = component.asset_key_for_cube("dates_base")
    node = asset_graph.get(renamed_key)
    # depends on the real "dates" dbt model, not a (nonexistent) "dates_base" one
    assert node.parent_keys == {component.asset_key_for_model("dates")}

    # the internal bookkeeping key that made this resolution possible never leaks into the
    # asset's own displayed metadata
    assert "__dagster_cube_dbt_dbt_model_name" not in str(node.metadata["dagster_cube_dbt/yaml"])


def test_hand_authored_extends_children_of_a_renamed_cube_depend_on_the_parent_cube(
    tmp_path, defs_dir
):
    """The real pattern this feature exists for: a suffixed, `public: false` base cube plus
    one or more hand-authored `extends:` cubes (via merge patches) exposing it publicly under
    different names/join roles -- e.g. one dbt model reused as both "origin" and
    "destination". Each child's Dagster dependency is the *parent cube's own asset* (its
    `extends` target), not the dbt model directly -- since these are `is_virtual` assets,
    Dagster's staleness engine already looks straight through a *chain* of them, so freshness
    still propagates back to the real dbt model transitively, exercised here via
    `get_non_virtual_ancestor_keys` the same way `test_cube_and_view_assets_are_virtual_and_
    freshness_looks_through_them` verifies it for the view -> cube -> dbt model case.
    """
    component = _make_component(defs_dir)
    state_path = tmp_path / "state"
    component.write_state_to_path(state_path)

    state_file = state_path.parent / CUBE_STATE_FILENAME
    merged = json.loads(state_file.read_text())
    for cube in merged["cubes"]:
        if cube["name"] == "dates":
            cube["name"] = "dates_base"
            cube["public"] = False
    merged["cube_source_models"] = {"dates_base": "dates"}
    # simulates what a merge patch would add: two separate public cubes, neither sharing the
    # dbt model's own name, both extending the renamed base.
    merged["cubes"].append({"name": "dates_a", "extends": "dates_base", "public": True})
    merged["cubes"].append({"name": "dates_b", "extends": "dates_base", "public": True})
    state_file.write_text(json.dumps(merged))

    context = dg.ComponentTree.for_test().load_context
    defs = _with_promoter(component.build_defs_from_state(context, state_path), NoopCubeFilePromoter())
    asset_graph = defs.resolve_asset_graph()

    dates_base_key = component.asset_key_for_cube("dates_base")
    dates_model_key = component.asset_key_for_model("dates")
    for cube_name in ("dates_a", "dates_b"):
        cube_key = component.asset_key_for_cube(cube_name)
        node = asset_graph.get(cube_key)
        # depends on the parent *cube* asset, not the dbt model directly
        assert node.parent_keys == {dates_base_key}
        assert "__dagster_cube_dbt_extends_parent" not in str(node.metadata["dagster_cube_dbt/yaml"])
        # ... but freshness still resolves through the virtual chain to the real dbt model
        assert asset_graph.get_non_virtual_ancestor_keys(cube_key) == {dates_model_key}


def test_build_defs_from_state_raises_clear_error_without_prior_refresh(tmp_path, defs_dir):
    component = _make_component(defs_dir)
    context = dg.ComponentTree.for_test().load_context

    with pytest.raises(dg.DagsterInvalidDefinitionError, match="refresh-defs-state"):
        component.build_defs_from_state(context, tmp_path / "never_refreshed_state")


def test_materializing_without_a_bound_promoter_fails_clearly(tmp_path, defs_dir):
    """`CubeDbtProjectComponent` requires a `CubeFilePromoter` resource bound under
    `promoter_resource_key` (`cube_file_promoter` by default) -- there's no default, since no
    on-disk path is reachable by both a Dagster run and an independently-running Cube
    instance in a real deployment. Materializing with nothing bound to that key must fail
    clearly rather than silently doing nothing.
    """
    component = _make_component(defs_dir)
    state_path = tmp_path / "state"
    component.write_state_to_path(state_path)

    context = dg.ComponentTree.for_test().load_context
    defs = component.build_defs_from_state(context, state_path)

    with pytest.raises(dg.DagsterInvalidDefinitionError, match="cube_file_promoter"):
        defs.resolve_implicit_global_asset_job_def().execute_in_process(
            asset_selection=[component.asset_key_for_cube("journey_samples")]
        )


def test_local_file_promoter_writes_output_only_on_materialization(tmp_path, defs_dir):
    """`LocalFileCubeFilePromoter` is the resource that does deliver files to a fixed
    directory on disk -- and, per the write-at-materialization-time design, only once a
    cube/view asset is actually materialized, not merely because state was refreshed.
    """
    output_dir = tmp_path / "cubes"
    views_output_dir = tmp_path / "views"
    component = _make_component(defs_dir)
    state_path = tmp_path / "state"
    component.write_state_to_path(state_path)
    assert not output_dir.exists()  # nothing written yet -- state refresh only cached data

    context = dg.ComponentTree.for_test().load_context
    promoter = LocalFileCubeFilePromoter(
        output_dir=str(output_dir), views_output_dir=str(views_output_dir)
    )
    defs = _with_promoter(component.build_defs_from_state(context, state_path), promoter)

    result = defs.resolve_implicit_global_asset_job_def().execute_in_process(
        asset_selection=[component.asset_key_for_cube("journey_samples")]
    )

    assert result.success
    materializations = result.asset_materializations_for_node(f"{component.dbt_project.name}_cubes")
    assert len(materializations) == 1

    # the promoter writes the *full* generated set, not just the selected subset
    cubes = {c["name"] for c in read_entities(output_dir, "cubes")}
    assert cubes == ALL_GENERATED_CUBE_NAMES
    views = {v["name"] for v in read_entities(views_output_dir, "views")}
    assert views == {"journeys_overview"}


def test_bound_promoter_is_called_before_materialization(tmp_path, defs_dir):
    component = _make_component(defs_dir)
    state_path = tmp_path / "state"
    component.write_state_to_path(state_path)

    context = dg.ComponentTree.for_test().load_context
    promoter, calls = _recording_promoter()
    defs = _with_promoter(component.build_defs_from_state(context, state_path), promoter)

    result = defs.resolve_implicit_global_asset_job_def().execute_in_process(
        asset_selection=[component.asset_key_for_cube("journey_samples")]
    )

    assert result.success
    assert [call["run_id"] for call in calls] == [result.run_id]
    # the staged directory holds every generated cube, not just the one selected for this run
    assert calls[0]["cube_names"] == ALL_GENERATED_CUBE_NAMES
    # the staging directory is a temp dir, cleaned up once promote() returns
    assert not calls[0]["cubes_dir"].exists()


def test_materializing_all_cubes_and_views_yields_in_topological_order(tmp_path, defs_dir):
    """Regression test for a real bug caught while verifying this against the actual example
    project: `_cube_assets` used to iterate `context.selected_asset_keys` directly (an
    unordered set) to decide `MaterializeResult` yield order, which intermittently yielded
    `journeys_overview` before one of the cubes it depends on -- Dagster requires multi-assets
    to yield in topological order, so that run failed with `DagsterInvariantViolationError`.
    Materializing every cube and view together (not just one, to actually put the view's
    ordering relative to its dependency cubes at stake) must succeed every time.
    """
    component = _make_component(defs_dir)
    state_path = tmp_path / "state"
    component.write_state_to_path(state_path)

    context = dg.ComponentTree.for_test().load_context
    defs = _with_promoter(component.build_defs_from_state(context, state_path), NoopCubeFilePromoter())

    result = defs.resolve_implicit_global_asset_job_def().execute_in_process()

    assert result.success
    materialized_order = [
        event.asset_key for event in result.get_asset_materialization_events()
    ]
    view_key = component.asset_key_for_view("journeys_overview")
    view_deps = {
        component.asset_key_for_cube("journey_samples"),
        component.asset_key_for_cube("destination_locations"),
    }
    assert materialized_order.index(view_key) > max(
        materialized_order.index(dep) for dep in view_deps
    )


def test_promoter_failure_fails_the_run_with_no_materializations(tmp_path, defs_dir):
    component = _make_component(defs_dir)
    state_path = tmp_path / "state"
    component.write_state_to_path(state_path)

    context = dg.ComponentTree.for_test().load_context
    defs = _with_promoter(component.build_defs_from_state(context, state_path), FailingCubeFilePromoter())

    result = defs.resolve_implicit_global_asset_job_def().execute_in_process(
        asset_selection=[component.asset_key_for_cube("journey_samples")],
        raise_on_error=False,
    )

    assert not result.success
    assert result.asset_materializations_for_node(f"{component.dbt_project.name}_cubes") == []


def test_promoter_resource_key_is_configurable(tmp_path, defs_dir):
    """A project with more than one `CubeDbtProjectComponent` needs to bind more than one
    promoter -- `promoter_resource_key` lets each instance pick a distinct resource key
    instead of all sharing the `cube_file_promoter` default.
    """
    component = _make_component(defs_dir, promoter_resource_key="a_different_key")
    state_path = tmp_path / "state"
    component.write_state_to_path(state_path)

    context = dg.ComponentTree.for_test().load_context
    promoter, calls = _recording_promoter()
    defs = _with_promoter(
        component.build_defs_from_state(context, state_path), promoter, key="a_different_key"
    )

    result = defs.resolve_implicit_global_asset_job_def().execute_in_process(
        asset_selection=[component.asset_key_for_cube("journey_samples")]
    )

    assert result.success
    assert len(calls) == 1


def _cube_multi_asset_op(defs: dg.Definitions, dbt_project_name: str) -> dg.OpDefinition:
    [assets_def] = [a for a in defs.assets if a.op.name == f"{dbt_project_name}_cubes"]
    return assets_def.op


def test_cube_assets_get_a_default_promotion_pool(tmp_path, defs_dir):
    """Most `CubeFilePromoter` implementations mutate some shared external state (a git
    checkout, a fixed output directory, ...) that two concurrent runs touching at once would
    corrupt or race on -- assigning a `pool` to the promotion op by default means a max
    concurrency of 1 can be set for it in the Dagster UI without any code change, rather than
    requiring every user to remember to configure this themselves.
    """
    component = _make_component(defs_dir)
    state_path = tmp_path / "state"
    component.write_state_to_path(state_path)

    context = dg.ComponentTree.for_test().load_context
    defs = _with_promoter(component.build_defs_from_state(context, state_path), NoopCubeFilePromoter())

    op = _cube_multi_asset_op(defs, component.dbt_project.name)
    assert op.pool == f"{component.dbt_project.name}_cube_promotion"


def test_promotion_pool_is_configurable(tmp_path, defs_dir):
    """Lets multiple components that share the same underlying promoter/destination (and so
    genuinely need to be mutually exclusive with each other, not just internally) be assigned
    the same pool explicitly, instead of each getting an independent per-project default.
    """
    component = _make_component(defs_dir, promotion_pool="shared_cube_promotion_pool")
    state_path = tmp_path / "state"
    component.write_state_to_path(state_path)

    context = dg.ComponentTree.for_test().load_context
    defs = _with_promoter(component.build_defs_from_state(context, state_path), NoopCubeFilePromoter())

    op = _cube_multi_asset_op(defs, component.dbt_project.name)
    assert op.pool == "shared_cube_promotion_pool"


def test_generated_asset_automation_condition_only_fires_on_code_version_change(tmp_path, defs_dir):
    """Cube/view assets should auto-run once when their own generated definition changes
    (detected via `code_version`, a hash of their generated YAML), not on every upstream dbt
    model data update the way `AutomationCondition.eager()` would. Exercises the real
    condition via `dg.evaluate_automation_conditions`/`report_runless_asset_event` rather
    than just asserting the condition object was constructed.
    """
    component = _make_component(defs_dir)
    state_path = tmp_path / "state"
    component.write_state_to_path(state_path)

    context = dg.ComponentTree.for_test().load_context
    defs = _with_promoter(component.build_defs_from_state(context, state_path), NoopCubeFilePromoter())

    journey_samples_key = component.asset_key_for_cube("journey_samples")
    model_key = component.asset_key_for_model("journey_samples")
    selection = dg.AssetSelection.assets(journey_samples_key)

    instance = dg.DagsterInstance.ephemeral()

    # tick 1: nothing materialized yet. This is the *initial* evaluation, which
    # `since_last_handled()` deliberately suppresses regardless of the inner condition (to
    # avoid mass-materializing a whole pre-existing asset graph the moment an automation
    # condition using it is first turned on) -- so this is 0 even though the cube is missing.
    result = dg.evaluate_automation_conditions(
        defs=defs, instance=instance, asset_selection=selection
    )
    assert result.total_requested == 0

    # tick 2: still nothing materialized, nothing changed -> still 0 (the dbt model dep is
    # still missing, which blocks the request regardless of the trigger clause).
    result = dg.evaluate_automation_conditions(
        defs=defs, instance=instance, asset_selection=selection, cursor=result.cursor
    )
    assert result.total_requested == 0

    # simulate the dbt model materializing, without running an actual dbt build.
    instance.report_runless_asset_event(dg.AssetMaterialization(asset_key=model_key))

    # tick 3: the dep is no longer missing, and the cube has been "pending" (missing, and
    # blocked) since tick 1 -> requested exactly once now that it's actually able to run.
    result = dg.evaluate_automation_conditions(
        defs=defs, instance=instance, asset_selection=selection, cursor=result.cursor
    )
    assert result.total_requested == 1

    # simulate the cube itself having materialized as a result of that request.
    instance.report_runless_asset_event(dg.AssetMaterialization(asset_key=journey_samples_key))

    # tick 4: nothing has changed since -> not re-requested.
    result = dg.evaluate_automation_conditions(
        defs=defs, instance=instance, asset_selection=selection, cursor=result.cursor
    )
    assert result.total_requested == 0

    # tick 5: the dbt model re-materializes with new *data* (a normal dbt run), but the
    # cube's own generated definition -- and therefore its code_version -- hasn't changed.
    # This is the key difference from eager(): must NOT be re-requested.
    instance.report_runless_asset_event(dg.AssetMaterialization(asset_key=model_key))
    result = dg.evaluate_automation_conditions(
        defs=defs, instance=instance, asset_selection=selection, cursor=result.cursor
    )
    assert result.total_requested == 0

    # tick 6: the cube's generated definition actually changes (a merge patch is added and
    # state is refreshed again), changing its code_version -> requested once.
    (defs_dir / "title_patch.yaml").write_text(
        "cubes:\n  - name: journey_samples\n    title: Changed Title\n"
    )
    component.write_state_to_path(state_path)
    changed_defs = _with_promoter(
        component.build_defs_from_state(context, state_path), NoopCubeFilePromoter()
    )
    result = dg.evaluate_automation_conditions(
        defs=changed_defs, instance=instance, asset_selection=selection, cursor=result.cursor
    )
    assert result.total_requested == 1

    # tick 7: nothing further has changed -> not re-requested.
    result = dg.evaluate_automation_conditions(
        defs=changed_defs, instance=instance, asset_selection=selection, cursor=result.cursor
    )
    assert result.total_requested == 0


def test_generated_asset_automation_condition_does_not_duplicate_request_while_pending(
    tmp_path, defs_dir
):
    """Regression test for a real bug caught during development: wrapping bare `missing()`
    in `.since_last_handled()` (instead of `missing().newly_true()` wrapped in
    `.since_last_handled()`, applied to the *whole* missing-and-deps-ready state) looked
    correct for a few ticks but started re-requesting again a couple of ticks after the
    first request went out, while the asset was still pending (not yet actually
    materialized, not yet visible as in_progress) -- exactly the "continually requesting
    partitions" failure mode the real `eager()` pattern is designed to avoid. This asserts
    across several ticks with the asset deliberately left un-materialized (simulating a run
    that was requested but hasn't completed yet) that no second request is ever generated.

    (Note: like the real `eager()`, this condition only reacts to transitions from the
    baseline established at its first-ever evaluation forward -- a dep that's already
    satisfied *before* that first evaluation never triggers a request on its own; see
    DECISIONS.md. That's why the dbt model materializes after, not before, the first tick
    here.)
    """
    component = _make_component(defs_dir)
    state_path = tmp_path / "state"
    component.write_state_to_path(state_path)

    context = dg.ComponentTree.for_test().load_context
    defs = _with_promoter(component.build_defs_from_state(context, state_path), NoopCubeFilePromoter())

    journey_samples_key = component.asset_key_for_cube("journey_samples")
    model_key = component.asset_key_for_model("journey_samples")
    selection = dg.AssetSelection.assets(journey_samples_key)

    instance = dg.DagsterInstance.ephemeral()

    # the dbt model must become ready *after* the baseline (first) evaluation, not before --
    # like the real built-in eager(), this condition only reacts to transitions from that
    # baseline forward; an asset whose deps were already satisfied before the daemon's very
    # first look at it needs one manual initial materialization, same as eager() would.
    result = dg.evaluate_automation_conditions(
        defs=defs, instance=instance, asset_selection=selection
    )
    assert result.total_requested == 0  # initial evaluation

    instance.report_runless_asset_event(dg.AssetMaterialization(asset_key=model_key))
    result = dg.evaluate_automation_conditions(
        defs=defs, instance=instance, asset_selection=selection, cursor=result.cursor
    )
    assert result.total_requested == 1  # dep just became ready -> requested once

    # deliberately do NOT materialize journey_samples here, simulating a run that was
    # requested but hasn't completed (or even started) yet -- across several more ticks.
    for _ in range(4):
        result = dg.evaluate_automation_conditions(
            defs=defs, instance=instance, asset_selection=selection, cursor=result.cursor
        )
        assert result.total_requested == 0
