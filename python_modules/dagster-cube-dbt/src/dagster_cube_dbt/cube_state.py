"""Read/write for the generated-cube state cache (`cube_dbt_state.json`) that
`CubeDbtProjectComponent.write_state_to_path` produces and `build_defs_from_state` reads back.
Pulled out into its own module (rather than staying inline in `cube_dbt_project/component.py`)
so a second, sibling component (`CubeSupersetSyncComponent`) can read the same cache file
without importing cube-generation internals it doesn't need.
"""

import json
from pathlib import Path
from typing import Any

import dagster as dg

CUBE_STATE_FILENAME = "cube_dbt_state.json"


def write_cube_state(state_path: Path, merged: dict[str, Any]) -> None:
    # `state_path` itself is a sentinel file `DbtProjectManager.prepare` touches, not a
    # directory -- its real per-key working directory is `state_path.parent`, so the cache
    # goes there instead, under a filename that won't collide with dagster_dbt's own
    # "project" subdirectory.
    (state_path.parent / CUBE_STATE_FILENAME).write_text(json.dumps(merged))


def read_cube_state(state_path: Path | None) -> dict[str, Any]:
    state_file = state_path.parent / CUBE_STATE_FILENAME if state_path else None
    if state_file is None or not state_file.exists():
        raise dg.DagsterInvalidDefinitionError(
            "No generated cube state found. Run `dg utils refresh-defs-state` to "
            "generate it before loading definitions."
        )
    return json.loads(state_file.read_text())
