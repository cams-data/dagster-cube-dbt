"""Verifies that `CubeDbtProjectComponent` doesn't change how the underlying dbt-asset layer
is surfaced, compared to using `dagster_dbt.DbtProjectComponent` directly for the same
project. A user swapping our component in for the vanilla one should get identical dbt
assets and checks, plus the added cube/view layer -- never any drift in the dbt portion,
since that's not something this library should ever be able to influence.

Uses the same real fixture dbt project as `tests/fixtures/dbt_project` (already `dbt parse`d
by `conftest.py`), constructed via a pre-instantiated `DbtProject` (the `NoopDbtProjectManager`
path) for both components, matching the pattern in `test_component_integration.py`.
"""

from pathlib import Path

import dagster as dg
import pytest
from dagster_dbt import DbtProjectComponent
from dagster_dbt.dbt_project import DbtProject

from dagster_cube_dbt.components.cube_dbt_project.component import (
    CubeDbtProjectComponent,
    CubeSelect,
)
from dagster_cube_dbt.resources import CubeFilePromoter
from dbt_engine import DBT_TARGET


class _NoopCubeFilePromoter(CubeFilePromoter):
    """Satisfies CubeDbtProjectComponent's required `cube_file_promoter` resource key --
    Dagster validates every required resource is bound as soon as a repository/asset graph is
    built, even for these spec/execution-comparison tests that never touch the cube layer.
    """

    def promote(self, context, cubes_dir, views_dir) -> None:
        return

FIXTURE_DBT_PROJECT = Path(__file__).parent / "fixtures" / "dbt_project"

# Every dbt model in the fixture, deliberately not scoped by cube_select -- the dbt-asset
# layer should be identical regardless of which cubes get generated from it.
DBT_MODEL_ASSET_KEYS = {
    dg.AssetKey(["dates"]),
    dg.AssetKey(["destination_locations"]),
    dg.AssetKey(["journey_samples"]),
    dg.AssetKey(["origin_locations"]),
    dg.AssetKey(["int_raw_journey_events"]),
}

# Fields on an AssetNode that describe how a dbt model is *surfaced* -- these must be
# identical between the two components. `child_keys` is deliberately excluded: our cube
# assets add themselves as new children of their dbt model, which is an expected, additive
# difference, not drift in how the dbt asset itself is presented.
COMPARABLE_NODE_FIELDS = [
    "description",
    "group_name",
    "tags",
    "kinds",
    "code_version",
    "owners",
    "is_partitioned",
    "is_materializable",
    "is_observable",
    "is_external",
    "parent_keys",
    "check_keys",
    "partitions_def",
    "backfill_policy",
]


@pytest.fixture
def defs_dir(tmp_path) -> Path:
    directory = tmp_path / "defs"
    directory.mkdir()
    (directory / "defs.yaml").write_text("type: dagster_cube_dbt.CubeDbtProjectComponent\n")
    return directory


def _vanilla_defs() -> dg.Definitions:
    component = DbtProjectComponent(
        project=DbtProject(project_dir=FIXTURE_DBT_PROJECT, target=DBT_TARGET)
    )
    context = dg.ComponentTree.for_test().load_context
    return component.build_defs_from_state(context, None)


def _cube_defs(tmp_path: Path, defs_dir: Path) -> dg.Definitions:
    # cube_select is scoped to marts, matching how the fixture is meant to be used
    # (int_raw_journey_events deliberately has an untyped column, exempt only because it's
    # excluded from cube generation) -- deliberately *not* passing select/exclude/selector,
    # so all 5 dbt models still get built by both components identically; cube_select only
    # controls which of them additionally get a cube, never what dbt itself builds.
    component = CubeDbtProjectComponent(
        project=DbtProject(project_dir=FIXTURE_DBT_PROJECT, target=DBT_TARGET),
        cube_select=CubeSelect(paths=["marts"]),
    )
    component._defs_dir = defs_dir  # normally set by CubeDbtProjectComponent.load()
    state_path = tmp_path / "state"
    component.write_state_to_path(state_path)
    context = dg.ComponentTree.for_test().load_context
    defs = component.build_defs_from_state(context, state_path)
    return dg.Definitions.merge(
        defs, dg.Definitions(resources={"cube_file_promoter": _NoopCubeFilePromoter()})
    )


def test_dbt_asset_specs_are_identical_to_vanilla_dbt_project_component(tmp_path, defs_dir):
    vanilla_graph = _vanilla_defs().resolve_asset_graph()
    cube_graph = _cube_defs(tmp_path, defs_dir).resolve_asset_graph()

    assert DBT_MODEL_ASSET_KEYS <= vanilla_graph.get_all_asset_keys()
    assert DBT_MODEL_ASSET_KEYS <= cube_graph.get_all_asset_keys()

    mismatches = []
    for key in sorted(DBT_MODEL_ASSET_KEYS):
        vanilla_node = vanilla_graph.get(key)
        cube_node = cube_graph.get(key)
        for field in COMPARABLE_NODE_FIELDS:
            vanilla_value = getattr(vanilla_node, field)
            cube_value = getattr(cube_node, field)
            if vanilla_value != cube_value:
                mismatches.append(f"{key.to_user_string()}.{field}: {vanilla_value!r} != {cube_value!r}")
    assert not mismatches, "\n".join(mismatches)

    # dbt's generic tests (unique/not_null) surface as asset checks -- must match exactly,
    # not just be a subset, since these are entirely dagster_dbt's own behavior.
    assert vanilla_graph.asset_check_keys == cube_graph.asset_check_keys


def test_dbt_metadata_keys_are_identical_to_vanilla_dbt_project_component(tmp_path, defs_dir):
    """Metadata values can legitimately differ in incidental ways (e.g. code references
    embedding a run-specific timestamp), so this checks the *keys* dagster_dbt attaches --
    catching drift in what information is surfaced, without being brittle about exact values.
    """
    vanilla_graph = _vanilla_defs().resolve_asset_graph()
    cube_graph = _cube_defs(tmp_path, defs_dir).resolve_asset_graph()

    for key in sorted(DBT_MODEL_ASSET_KEYS):
        vanilla_keys = set(vanilla_graph.get(key).metadata.keys())
        cube_keys = set(cube_graph.get(key).metadata.keys())
        assert cube_keys == vanilla_keys, f"{key.to_user_string()}: {cube_keys} != {vanilla_keys}"


def test_dbt_build_materializes_identically_through_both_components(tmp_path, defs_dir):
    """Execution-level parity, not just spec-level: actually run `dbt build` through both
    components' generated multi-assets and confirm the same set of dbt models/checks succeed.
    This is the strongest form of "no drift" verification -- it exercises the real dbt CLI
    invocation path, not just the declared AssetSpecs.
    """
    vanilla_defs = _vanilla_defs()
    cube_defs = _cube_defs(tmp_path, defs_dir)

    vanilla_result = vanilla_defs.resolve_implicit_global_asset_job_def().execute_in_process(
        asset_selection=list(DBT_MODEL_ASSET_KEYS)
    )
    cube_result = cube_defs.resolve_implicit_global_asset_job_def().execute_in_process(
        asset_selection=list(DBT_MODEL_ASSET_KEYS)
    )

    assert vanilla_result.success
    assert cube_result.success

    vanilla_materialized = {
        event.asset_key
        for event in vanilla_result.get_asset_materialization_events()
    }
    cube_materialized = {
        event.asset_key
        for event in cube_result.get_asset_materialization_events()
    }
    assert vanilla_materialized == DBT_MODEL_ASSET_KEYS
    assert cube_materialized == DBT_MODEL_ASSET_KEYS

    vanilla_checks_passed = {
        (event.asset_key, event.check_name)
        for event in vanilla_result.get_asset_check_evaluations()
        if event.passed
    }
    cube_checks_passed = {
        (event.asset_key, event.check_name)
        for event in cube_result.get_asset_check_evaluations()
        if event.passed
    }
    assert vanilla_checks_passed == cube_checks_passed
    assert len(vanilla_checks_passed) == 8  # 4 typed PK columns x (unique + not_null)
