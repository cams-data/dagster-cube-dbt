from pathlib import Path

from dagster_cube_dbt.output import read_entities, write_entities


def test_write_then_read_round_trips(tmp_path):
    cubes = [
        {"name": "journey_samples", "sql_table": "a.b"},
        {"name": "destination_locations", "sql_table": "a.c"},
    ]

    write_entities(tmp_path, "cubes", cubes)
    result = read_entities(tmp_path, "cubes")

    # read_entities orders deterministically by filename, not insertion order
    assert sorted(result, key=lambda c: c["name"]) == sorted(cubes, key=lambda c: c["name"])


def test_write_removes_stale_entities_of_the_same_key(tmp_path):
    write_entities(tmp_path, "cubes", [{"name": "old_cube"}])
    assert (tmp_path / "old_cube.yaml").exists()

    write_entities(tmp_path, "cubes", [{"name": "new_cube"}])

    assert not (tmp_path / "old_cube.yaml").exists()
    assert read_entities(tmp_path, "cubes") == [{"name": "new_cube"}]


def test_write_leaves_other_keys_files_untouched_in_shared_directory(tmp_path):
    write_entities(tmp_path, "cubes", [{"name": "journey_samples"}])
    write_entities(tmp_path, "views", [{"name": "journeys_overview"}])

    # rewriting cubes should not remove the co-located view file
    write_entities(tmp_path, "cubes", [{"name": "journey_samples"}])

    assert read_entities(tmp_path, "cubes") == [{"name": "journey_samples"}]
    assert read_entities(tmp_path, "views") == [{"name": "journeys_overview"}]


def test_read_entities_on_missing_directory_returns_empty():
    assert read_entities(Path("does/not/exist"), "cubes") == []
