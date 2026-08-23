"""Reading and writing the generated, merged per-cube / per-view YAML files that make up a
`CubeDbtProjectComponent`'s `output_dir` / `views_output_dir`.

Each entity (cube or view) is written to its own file as `{<key>: [<entity>]}` (e.g.
`{"cubes": [{"name": "journey_samples", ...}]}`), so cube and view files can be told apart by
their top-level key even when `output_dir` and `views_output_dir` are the same directory.
"""

from pathlib import Path
from typing import Any

import yaml


def _entity_files(directory: Path) -> set[Path]:
    return {*directory.glob("*.yml"), *directory.glob("*.yaml")}


def write_entities(directory: Path, key: str, entities: list[dict[str, Any]]) -> None:
    """Write one file per entity, removing stale files for entities (under this `key`) that
    no longer exist. Files belonging to a different `key` (e.g. views, when `output_dir` and
    `views_output_dir` are the same directory) are left untouched.
    """
    directory.mkdir(parents=True, exist_ok=True)
    expected_names = {entity["name"] for entity in entities}

    for existing in _entity_files(directory):
        try:
            doc = yaml.safe_load(existing.read_text()) or {}
        except yaml.YAMLError:
            continue
        if key in doc and existing.stem not in expected_names:
            existing.unlink()

    for entity in entities:
        path = directory / f"{entity['name']}.yaml"
        path.write_text(yaml.safe_dump({key: [entity]}, sort_keys=False))


def read_entities(directory: Path, key: str) -> list[dict[str, Any]]:
    if not directory.exists():
        return []

    entities: list[dict[str, Any]] = []
    for path in sorted(_entity_files(directory), key=lambda p: p.name):
        doc = yaml.safe_load(path.read_text()) or {}
        entities.extend(doc.get(key, []))
    return entities
