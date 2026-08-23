import copy

import pytest

from dagster_cube_dbt.merge import (
    CircularExtendsError,
    UnmatchedPatchTargetError,
    discover_patch_files,
    merge_documents,
    resolve_extends,
)

JOURNEY_SAMPLES_BASE = {
    "cubes": [
        {
            "name": "journey_samples",
            "sql_table": "ch_transport_silver.journey_samples",
            "title": "Journey Samples",
            "dimensions": [
                {
                    "name": "journey_sample_key",
                    "sql": "journey_sample_key",
                    "type": "string",
                    "primary_key": True,
                },
                {"name": "journey_type", "sql": "journey_type", "type": "string"},
                {"name": "direction", "sql": "direction", "type": "string"},
            ],
        }
    ]
}


def test_remove_dimension_matches_conversation_example():
    base = copy.deepcopy(JOURNEY_SAMPLES_BASE)
    patch = {
        "cubes": [
            {
                "name": "journey_samples",
                "dimensions": [
                    {"name": "journey_type", "$mergeStrategy": "remove"},
                ],
            }
        ]
    }

    result = merge_documents(base, [patch])

    [cube] = result["cubes"]
    assert [d["name"] for d in cube["dimensions"]] == ["journey_sample_key", "direction"]
    # everything else on the cube is untouched
    assert cube["sql_table"] == "ch_transport_silver.journey_samples"
    assert cube["title"] == "Journey Samples"


def test_default_strategy_deep_merges_matched_item():
    base = copy.deepcopy(JOURNEY_SAMPLES_BASE)
    patch = {
        "cubes": [
            {
                "name": "journey_samples",
                "measures": [{"name": "count", "type": "count"}],
            }
        ]
    }

    result = merge_documents(base, [patch])

    [cube] = result["cubes"]
    assert cube["measures"] == [{"name": "count", "type": "count"}]
    # dimensions from the base survive untouched
    assert len(cube["dimensions"]) == 3


def test_replace_strategy_substitutes_whole_item():
    base = copy.deepcopy(JOURNEY_SAMPLES_BASE)
    patch = {
        "cubes": [
            {
                "name": "journey_samples",
                "$mergeStrategy": "replace",
                "sql_table": "other_schema.other_table",
            }
        ]
    }

    result = merge_documents(base, [patch])

    [cube] = result["cubes"]
    assert cube == {"name": "journey_samples", "sql_table": "other_schema.other_table"}


def test_no_patches_passes_base_through_unchanged():
    base = copy.deepcopy(JOURNEY_SAMPLES_BASE)

    result = merge_documents(base, [])

    assert result == JOURNEY_SAMPLES_BASE


def test_appended_new_item_does_not_leak_merge_strategy_key():
    """Regression test: an item with no existing match used to be appended as-is, including
    a literal `$mergeStrategy` key, since stripping only happened on the matched-item branch.
    Caught via a real dg-driven run where a cube_select misconfiguration meant every patch
    landed on this "no match" branch and the stray key showed up in generated output."""
    base = copy.deepcopy(JOURNEY_SAMPLES_BASE)
    patch = {
        "cubes": [
            {
                "name": "journey_samples",
                "dimensions": [
                    {"name": "brand_new_dimension", "$mergeStrategy": "merge", "type": "string"},
                ],
            }
        ]
    }

    result = merge_documents(base, [patch])

    [cube] = result["cubes"]
    new_dimension = next(d for d in cube["dimensions"] if d["name"] == "brand_new_dimension")
    assert "$mergeStrategy" not in new_dimension


def test_remove_on_unmatched_item_is_a_noop_but_warns():
    base = copy.deepcopy(JOURNEY_SAMPLES_BASE)
    patch = {
        "cubes": [
            {
                "name": "journey_samples",
                "dimensions": [
                    {"name": "does_not_exist", "$mergeStrategy": "remove"},
                ],
            }
        ]
    }

    with pytest.warns(UserWarning, match="does_not_exist"):
        result = merge_documents(base, [patch])

    [cube] = result["cubes"]
    dimension_names = [d["name"] for d in cube["dimensions"]]
    assert "does_not_exist" not in dimension_names
    assert len(dimension_names) == 3  # unchanged from the base


def test_patch_strategy_merges_when_matched_just_like_the_default():
    base = copy.deepcopy(JOURNEY_SAMPLES_BASE)
    patch = {
        "cubes": [
            {
                "name": "journey_samples",
                "$mergeStrategy": "patch",
                "measures": [{"name": "count", "type": "count"}],
            }
        ]
    }

    result = merge_documents(base, [patch])

    [cube] = result["cubes"]
    assert cube["measures"] == [{"name": "count", "type": "count"}]
    assert len(cube["dimensions"]) == 3  # untouched, same as the default strategy


def test_patch_strategy_raises_when_unmatched():
    """The exact bug this strategy exists to catch: a patch meant to modify an existing cube
    (here, removing a dimension) whose target has since disappeared -- e.g. the dbt model was
    dropped or renamed. Without `$mergeStrategy: patch`, this would previously have silently
    become a new, broken cube missing `sql_table` and still carrying the nested
    `$mergeStrategy: remove` key unprocessed (see the appended-item stray-key regression test
    above -- the same underlying mechanism)."""
    base = {"cubes": []}  # journey_samples no longer exists in the generated base at all
    patch = {
        "cubes": [
            {
                "name": "journey_samples",
                "$mergeStrategy": "patch",
                "dimensions": [{"name": "journey_type", "$mergeStrategy": "remove"}],
            }
        ]
    }

    with pytest.raises(UnmatchedPatchTargetError) as excinfo:
        merge_documents(base, [patch])

    assert excinfo.value.targets == ["journey_samples"]
    assert "journey_samples" in str(excinfo.value)


def test_patch_strategy_collects_all_unmatched_targets_across_patches():
    base = {"cubes": []}
    patch_a = {"cubes": [{"name": "journey_samples", "$mergeStrategy": "patch", "title": "x"}]}
    patch_b = {"cubes": [{"name": "exchange_rates", "$mergeStrategy": "patch", "title": "y"}]}

    with pytest.raises(UnmatchedPatchTargetError) as excinfo:
        merge_documents(base, [patch_a, patch_b])

    assert excinfo.value.targets == ["exchange_rates", "journey_samples"]


def test_patch_strategy_can_coexist_with_new_resources_in_the_same_file():
    """A single file can patch an existing cube and introduce new ones below it -- the
    scenario that ruled out a per-file marker in favor of a per-item one."""
    base = copy.deepcopy(JOURNEY_SAMPLES_BASE)
    patch = {
        "cubes": [
            {
                "name": "journey_samples",
                "$mergeStrategy": "patch",
                "measures": [{"name": "count", "type": "count"}],
            },
            {
                "name": "journey_samples_summary",
                "sql_table": "ch_transport_silver.journey_samples_summary",
            },
        ]
    }

    result = merge_documents(base, [patch])

    names = {cube["name"] for cube in result["cubes"]}
    assert names == {"journey_samples", "journey_samples_summary"}
    patched = next(c for c in result["cubes"] if c["name"] == "journey_samples")
    assert patched["measures"] == [{"name": "count", "type": "count"}]


def test_patch_can_append_a_wholly_new_cube():
    base = copy.deepcopy(JOURNEY_SAMPLES_BASE)
    patch = {
        "cubes": [
            {
                "name": "exchange_rates",
                "sql": "SELECT * FROM some_external_source",
            }
        ]
    }

    result = merge_documents(base, [patch])

    names = {cube["name"] for cube in result["cubes"]}
    assert names == {"journey_samples", "exchange_rates"}


def test_patch_can_introduce_a_views_section_from_nothing():
    base = copy.deepcopy(JOURNEY_SAMPLES_BASE)
    patch = {
        "views": [
            {
                "name": "journeys_overview",
                "cubes": [{"join_path": "journey_samples", "includes": "*"}],
            }
        ]
    }

    result = merge_documents(base, [patch])

    assert result["views"] == patch["views"]
    # base cubes list is untouched
    assert result["cubes"] == JOURNEY_SAMPLES_BASE["cubes"]


def test_patches_apply_in_sorted_path_order():
    base = {"cubes": [{"name": "journey_samples", "title": "original"}]}
    patch_a = {"cubes": [{"name": "journey_samples", "title": "from_a"}]}
    patch_b = {"cubes": [{"name": "journey_samples", "title": "from_b"}]}

    result = merge_documents(copy.deepcopy(base), [patch_a, patch_b])

    assert result["cubes"][0]["title"] == "from_b"


def test_resolve_extends_matches_cube_docs_example():
    """Verified against Cube's own documented `extends` example: the child reuses the
    parent's `sql_table` and `count` measure, and adds its own `double_count` measure."""
    cubes = [
        {
            "name": "order_facts",
            "sql_table": "orders",
            "measures": [{"name": "count", "type": "count", "sql": "id"}],
        },
        {
            "name": "extended_order_facts",
            "extends": "order_facts",
            "measures": [{"name": "double_count", "type": "number", "sql": "{count} * 2"}],
        },
    ]

    resolved = resolve_extends(cubes)

    assert resolved["extended_order_facts"] == {
        "name": "extended_order_facts",
        "sql_table": "orders",
        "measures": [
            {"name": "count", "type": "count", "sql": "id"},
            {"name": "double_count", "type": "number", "sql": "{count} * 2"},
        ],
    }
    # unrelated to any extends chain -- returned as-is
    assert resolved["order_facts"] == cubes[0]


def test_resolve_extends_child_overrides_win_over_inherited_fields():
    cubes = [
        {"name": "base", "description": "base description", "title": "Base"},
        {"name": "child", "extends": "base", "description": "child's own description"},
    ]

    resolved = resolve_extends(cubes)

    assert resolved["child"]["description"] == "child's own description"
    assert resolved["child"]["title"] == "Base"  # inherited, not overridden


def test_resolve_extends_follows_multi_level_chains():
    cubes = [
        {"name": "a", "description": "from a", "meta": {"owner": "team-a"}},
        {"name": "b", "extends": "a", "title": "from b"},
        {"name": "c", "extends": "b", "description": "from c"},
    ]

    resolved = resolve_extends(cubes)

    assert resolved["c"]["description"] == "from c"  # c's own override
    assert resolved["c"]["title"] == "from b"  # inherited from b
    assert resolved["c"]["meta"] == {"owner": "team-a"}  # inherited from a, through b


def test_resolve_extends_unknown_parent_is_left_unresolved():
    """`extends` can target a hand-authored cube defined entirely outside this pipeline --
    there's no visibility into it, so the child is left with just its own fields."""
    cubes = [{"name": "child", "extends": "some_external_cube", "description": "own desc"}]

    resolved = resolve_extends(cubes)

    assert resolved["child"] == {"name": "child", "description": "own desc"}


def test_resolve_extends_raises_on_a_cycle():
    cubes = [
        {"name": "x", "extends": "y"},
        {"name": "y", "extends": "x"},
    ]

    with pytest.raises(CircularExtendsError) as excinfo:
        resolve_extends(cubes)

    assert excinfo.value.cycle == ["x", "y", "x"]


def test_resolve_extends_never_mutates_its_input():
    cubes = [
        {"name": "base", "measures": [{"name": "count", "type": "count"}]},
        {"name": "child", "extends": "base", "measures": [{"name": "sum", "type": "sum"}]},
    ]
    original = copy.deepcopy(cubes)

    resolve_extends(cubes)

    assert cubes == original


def test_discover_patch_files_sorted_and_excludes(tmp_path):
    (tmp_path / "b_patch.yaml").write_text("cubes: []")
    (tmp_path / "a_patch.yml").write_text("cubes: []")
    (tmp_path / "defs.yaml").write_text("type: dagster_cube_dbt.CubeDbtProjectComponent")
    nested = tmp_path / "nested"
    nested.mkdir()
    (nested / "c_patch.yaml").write_text("cubes: []")
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    (output_dir / "generated_cube.yaml").write_text("name: should_not_be_a_patch")

    found = list(
        discover_patch_files(
            tmp_path,
            exclude=[tmp_path / "defs.yaml", output_dir],
        )
    )

    assert found == [
        tmp_path / "a_patch.yml",
        tmp_path / "b_patch.yaml",
        nested / "c_patch.yaml",
    ]
