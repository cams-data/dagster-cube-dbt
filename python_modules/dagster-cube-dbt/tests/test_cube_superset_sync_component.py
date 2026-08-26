"""Tests for `CubeSupersetSyncComponent`. Constructs the sibling `CubeDbtProjectComponent`
directly (same `_make_sibling` / `defs_dir` pattern `test_component_integration.py` uses) and
exercises `build_defs_from_sibling_state` -- the pure core of `build_defs`, split out
specifically so these tests don't need a real on-disk defs tree / `context.load_component`
resolution (see that method's docstring for why).
"""

import json
import shutil
from pathlib import Path
from unittest.mock import patch

import dagster as dg
import pytest
from dagster_dbt.components.dbt_project.component import DbtProjectArgs
from dagster_dbt.dbt_project import DbtProject
from dagster_dbt.dbt_project_manager import DbtProjectArgsManager
from dbt_engine import DBT_TARGET

from dagster_cube_dbt.components.cube_dbt_project.component import CubeDbtProjectComponent, CubeSelect
from dagster_cube_dbt.components.cube_superset_sync.component import (
    CubeSupersetSyncComponent,
    _resolve_view_members,
)
from dagster_cube_dbt.cube_state import CUBE_STATE_FILENAME
from dagster_cube_dbt.superset_resource import SupersetResource

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


@pytest.fixture
def defs_dir(tmp_path) -> Path:
    directory = tmp_path / "defs"
    directory.mkdir()
    (directory / "defs.yaml").write_text("type: dagster_cube_dbt.CubeDbtProjectComponent\n")
    (directory / "journey_samples_patch.yaml").write_text(JOURNEY_SAMPLES_PATCH)
    return directory


def _make_sibling(defs_dir: Path, **kwargs) -> CubeDbtProjectComponent:
    project = DbtProject(project_dir=FIXTURE_DBT_PROJECT, target=DBT_TARGET)
    component = CubeDbtProjectComponent(project=project, cube_select=CubeSelect(paths=["marts"]), **kwargs)
    component._defs_dir = defs_dir  # noqa: SLF001 -- normally set by CubeDbtProjectComponent.load()
    return component


def _make_sync_component(**kwargs) -> CubeSupersetSyncComponent:
    return CubeSupersetSyncComponent(dbt_cube_component="../dbt_ingest", **kwargs)


class _NoopSuperset:
    """Satisfies the `superset` resource requirement for tests that only care about specs/
    dependencies, never actual syncing -- Dagster validates every required resource key is
    bound as soon as an asset graph is built, even without executing anything.
    """

    def sync_dataset(self, database_name, schema, table_name, dimensions, measures):
        return 0


def _with_superset(defs: dg.Definitions) -> dg.Definitions:
    return dg.Definitions.merge(defs, dg.Definitions(resources={"superset": _NoopSuperset()}))


def test_one_dataset_asset_spec_per_view(tmp_path, defs_dir):
    sibling = _make_sibling(defs_dir)
    state_path = tmp_path / "state"
    sibling.write_state_to_path(state_path)

    context = dg.ComponentTree.for_test().load_context
    defs = _with_superset(_make_sync_component().build_defs_from_sibling_state(context, sibling, state_path))
    asset_graph = defs.resolve_asset_graph()

    dataset_key = dg.AssetKey(["superset_dataset", "journeys_overview"])
    dataset_keys = {key for key in asset_graph.get_all_asset_keys() if key.path[0] == "superset_dataset"}
    assert dataset_keys == {dataset_key}


def test_dataset_depends_on_the_sibling_view_asset_and_shares_its_code_version(tmp_path, defs_dir):
    sibling = _make_sibling(defs_dir)
    state_path = tmp_path / "state"
    sibling.write_state_to_path(state_path)

    context = dg.ComponentTree.for_test().load_context
    defs = _with_superset(_make_sync_component().build_defs_from_sibling_state(context, sibling, state_path))
    asset_graph = defs.resolve_asset_graph()

    dataset_key = dg.AssetKey(["superset_dataset", "journeys_overview"])
    node = asset_graph.get(dataset_key)
    assert node.parent_keys == {sibling.asset_key_for_view("journeys_overview")}

    sibling_view_spec = sibling.get_view_asset_spec(
        next(v for v in _read_state(state_path)["views"] if v["name"] == "journeys_overview")
    )
    assets_def = next(a for a in defs.assets if isinstance(a, dg.AssetsDefinition) and dataset_key in a.keys)
    assert assets_def.get_asset_spec(dataset_key).code_version == sibling_view_spec.code_version


def test_dataset_column_schema_includes_the_resolved_measure(tmp_path, defs_dir):
    sibling = _make_sibling(defs_dir)
    state_path = tmp_path / "state"
    sibling.write_state_to_path(state_path)

    context = dg.ComponentTree.for_test().load_context
    defs = _with_superset(_make_sync_component().build_defs_from_sibling_state(context, sibling, state_path))
    asset_graph = defs.resolve_asset_graph()

    dataset_key = dg.AssetKey(["superset_dataset", "journeys_overview"])
    schema = asset_graph.get(dataset_key).metadata["dagster/column_schema"]
    column_names = {column.name for column in schema.columns}
    assert "count" in column_names  # journey_samples' own measure, included via includes: "*"


def _read_state(state_path: Path) -> dict:
    return json.loads((state_path.parent / CUBE_STATE_FILENAME).read_text())


def test_a_subclass_renaming_the_view_key_is_reflected_in_the_dataset_deps(tmp_path, defs_dir):
    """Regression-shaped test for the same class of bug DECISIONS.md Phase 37/40 already fixed
    twice elsewhere in this codebase: a dependency must be resolved through the sibling's own
    overridable `get_view_asset_spec`, never a guessed/reconstructed key.
    """

    class RenamingComponent(CubeDbtProjectComponent):
        def get_view_asset_spec(self, view):
            base_spec = super().get_view_asset_spec(view)
            return base_spec.replace_attributes(key=dg.AssetKey(["renamed", *base_spec.key.path]))

    project = DbtProject(project_dir=FIXTURE_DBT_PROJECT, target=DBT_TARGET)
    sibling = RenamingComponent(project=project, cube_select=CubeSelect(paths=["marts"]))
    sibling._defs_dir = defs_dir  # noqa: SLF001 -- normally set by CubeDbtProjectComponent.load()
    state_path = tmp_path / "state"
    sibling.write_state_to_path(state_path)

    context = dg.ComponentTree.for_test().load_context
    defs = _with_superset(_make_sync_component().build_defs_from_sibling_state(context, sibling, state_path))
    asset_graph = defs.resolve_asset_graph()

    dataset_key = dg.AssetKey(["superset_dataset", "journeys_overview"])
    renamed_view_key = dg.AssetKey(["renamed", *sibling.asset_key_for_view("journeys_overview").path])
    assert asset_graph.get(dataset_key).parent_keys == {renamed_view_key}


def test_build_defs_from_sibling_state_does_not_need_the_live_dbt_project_directory(tmp_path, defs_dir):
    """Mirrors `test_build_defs_from_state_does_not_need_the_live_dbt_project_directory` in
    test_component_integration.py: `build_defs_from_sibling_state` must work off the cached
    state alone. Uses a real `DbtProjectArgsManager` (not the `NoopDbtProjectManager` a
    pre-built `DbtProject` instance routes through) pointed at a throwaway copy of the fixture
    project, deleted before the sync component's defs are ever built, to prove nothing here
    falls back to a live lookup.
    """
    live_project_dir = tmp_path / "live_dbt_project"
    shutil.copytree(FIXTURE_DBT_PROJECT, live_project_dir)

    def _build_sibling() -> CubeDbtProjectComponent:
        manager = DbtProjectArgsManager(DbtProjectArgs(project_dir=str(live_project_dir), target=DBT_TARGET))
        component = CubeDbtProjectComponent(project=manager, cube_select=CubeSelect(paths=["marts"]))
        component._defs_dir = defs_dir  # noqa: SLF001
        return component

    state_path = tmp_path / "state"
    _build_sibling().write_state_to_path(state_path)  # thrown away once state is cached

    shutil.rmtree(live_project_dir)

    sibling = _build_sibling()  # a fresh instance, pointed at the now-deleted directory
    context = dg.ComponentTree.for_test().load_context
    defs = _with_superset(_make_sync_component().build_defs_from_sibling_state(context, sibling, state_path))
    asset_graph = defs.resolve_asset_graph()

    dataset_key = dg.AssetKey(["superset_dataset", "journeys_overview"])
    assert dataset_key in asset_graph.get_all_asset_keys()


def test_multi_asset_op_syncs_each_selected_dataset(tmp_path, defs_dir):
    sibling = _make_sibling(defs_dir)
    state_path = tmp_path / "state"
    sibling.write_state_to_path(state_path)

    context = dg.ComponentTree.for_test().load_context
    defs = _make_sync_component().build_defs_from_sibling_state(context, sibling, state_path)

    calls: list = []

    class _RecordingSuperset:
        def sync_dataset(self, database_name, schema, table_name, dimensions, measures):
            calls.append(
                {
                    "database_name": database_name,
                    "schema": schema,
                    "table_name": table_name,
                    "dimension_names": {d["name"] for d in dimensions},
                    "measure_names": {m["name"] for m in measures},
                }
            )
            return 1

    result = dg.materialize(
        defs.assets, resources={"superset": _RecordingSuperset()}, instance=dg.DagsterInstance.ephemeral()
    )
    assert result.success
    assert len(calls) == 1
    call = calls[0]
    assert call["table_name"] == "journeys_overview"
    assert call["schema"] == "public"
    assert call["database_name"] == "Cube"
    assert "count" in call["measure_names"]


def test_build_managed_resource_returns_none_without_base_url():
    assert _make_sync_component().build_managed_resource() is None


def test_build_managed_resource_builds_a_superset_resource():
    resource = _make_sync_component(base_url="https://s.example.com", username="u", password="p").build_managed_resource()
    assert isinstance(resource, SupersetResource)
    assert resource.base_url == "https://s.example.com"
    assert resource.verify_tls is True


def test_build_managed_resource_passes_verify_tls_through():
    resource = _make_sync_component(
        base_url="https://s.example.com", username="u", password="p", verify_tls=False
    ).build_managed_resource()
    assert resource.verify_tls is False


def test_build_managed_resource_raises_when_username_and_password_missing():
    with pytest.raises(dg.DagsterInvalidDefinitionError, match="username and password"):
        _make_sync_component(base_url="https://s.example.com").build_managed_resource()


def test_build_managed_resource_builds_a_superset_resource_from_an_api_key():
    resource = _make_sync_component(
        base_url="https://s.example.com", api_key="k"
    ).build_managed_resource()
    assert isinstance(resource, SupersetResource)
    assert resource.api_key == "k"
    assert resource.username is None
    assert resource.password is None


def test_build_managed_resource_raises_when_api_key_and_username_password_both_set():
    with pytest.raises(dg.DagsterInvalidDefinitionError, match="not both"):
        _make_sync_component(
            base_url="https://s.example.com", api_key="k", username="u", password="p"
        ).build_managed_resource()


def test_multi_asset_op_uses_the_managed_resource_without_needing_one_externally_bound(tmp_path, defs_dir):
    """Setting `base_url`/`username`/`password` means the component builds its own
    `SupersetResource` -- the multi-asset op's `required_resource_keys` must not demand
    anything bound under `superset_resource_key` in that case, or every project using the
    managed path would also have to bind a pointless resource under a key nothing reads.
    """
    sibling = _make_sibling(defs_dir)
    state_path = tmp_path / "state"
    sibling.write_state_to_path(state_path)

    context = dg.ComponentTree.for_test().load_context
    sync_component = _make_sync_component(base_url="https://s.example.com", username="u", password="p")
    defs = sync_component.build_defs_from_sibling_state(context, sibling, state_path)

    # No "superset" resource bound at all -- resolving the asset graph must not complain about
    # a missing resource requirement, proving the op doesn't require one in managed mode.
    asset_graph = defs.resolve_asset_graph()
    dataset_key = dg.AssetKey(["superset_dataset", "journeys_overview"])
    assert dataset_key in asset_graph.get_all_asset_keys()

    calls: list = []
    with patch.object(SupersetResource, "sync_dataset", lambda self, **kwargs: calls.append(kwargs) or 1):
        result = dg.materialize(defs.assets, instance=dg.DagsterInstance.ephemeral())

    assert result.success
    assert len(calls) == 1
    assert calls[0]["table_name"] == "journeys_overview"


def test_dataset_column_schema_metadata_matches_resolve_view_members_output():
    """Sanity check tying `_dataset_metadata` (used to build AssetSpec.metadata) to the same
    `_resolve_view_members` the op itself calls -- both must agree on what a view exposes.
    """
    resolved_cubes = {
        "a": {
            "dimensions": [{"name": "x", "type": "string"}, {"name": "y", "type": "number"}],
            "measures": [{"name": "total", "type": "sum"}],
        }
    }
    view = {"name": "v", "cubes": [{"join_path": "a", "includes": ["x"]}]}

    dimensions, measures = _resolve_view_members(view, resolved_cubes)

    assert [d["name"] for d in dimensions] == ["x"]
    assert measures == []  # "total" wasn't in includes


def test_resolve_view_members_include_star_pulls_in_everything():
    resolved_cubes = {
        "a": {
            "dimensions": [{"name": "x"}, {"name": "y"}],
            "measures": [{"name": "total"}],
        }
    }
    view = {"cubes": [{"join_path": "a", "includes": "*"}]}

    dimensions, measures = _resolve_view_members(view, resolved_cubes)

    assert {d["name"] for d in dimensions} == {"x", "y"}
    assert {m["name"] for m in measures} == {"total"}


def test_resolve_view_members_excludes_win_over_star_include():
    resolved_cubes = {"a": {"dimensions": [{"name": "x"}, {"name": "y"}], "measures": []}}
    view = {"cubes": [{"join_path": "a", "includes": "*", "excludes": ["y"]}]}

    dimensions, _measures = _resolve_view_members(view, resolved_cubes)

    assert {d["name"] for d in dimensions} == {"x"}


def test_resolve_view_members_skips_members_with_no_join_path_or_unknown_cube():
    resolved_cubes = {"a": {"dimensions": [{"name": "x"}], "measures": []}}
    view = {"cubes": [{"includes": "*"}, {"join_path": "unknown_cube", "includes": "*"}]}

    dimensions, measures = _resolve_view_members(view, resolved_cubes)

    assert dimensions == []
    assert measures == []


def test_resolve_view_members_uses_the_last_segment_of_a_dotted_join_path():
    """Regression test for a real production bug: a multi-hop `join_path` (e.g.
    `"fact.dates"`) names the cube reached by joining *through* the first segment(s) -- the
    entry's `includes`/`excludes` apply to the *last* segment, not the first. An earlier
    version of this took the first segment, which -- for a view with several members chained
    off one fact cube (`"fact.dates"`, `"fact.times"`, `"fact.routes"`) -- silently collapsed
    all of them onto just the fact cube, dropping every dimension table's members entirely.
    """
    resolved_cubes = {
        "fact": {"dimensions": [{"name": "amount"}], "measures": []},
        "dates": {"dimensions": [{"name": "date_key"}], "measures": []},
    }
    view = {
        "cubes": [
            {"join_path": "fact", "includes": "*"},
            {"join_path": "fact.dates", "includes": "*"},
        ]
    }

    dimensions, _measures = _resolve_view_members(view, resolved_cubes)

    assert {d["name"] for d in dimensions} == {"amount", "date_key"}
