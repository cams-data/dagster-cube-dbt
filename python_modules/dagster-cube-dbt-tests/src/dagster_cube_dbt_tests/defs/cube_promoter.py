"""Binds the `cube_file_promoter` resource `dbt_cubes/defs.yaml`'s `CubeDbtProjectComponent`
delegates delivery of generated cube/view YAML to. `LocalFileCubeFilePromoter` writes straight
to `cube_project/model/{cubes,views}`, which the local `cube dev` process reads directly --
appropriate here since this is a local example project, not a production deployment.
"""

from pathlib import Path

import dagster as dg
from dagster_cube_dbt import LocalFileCubeFilePromoter

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "cube_file_promoter": LocalFileCubeFilePromoter(
                output_dir=str(PROJECT_ROOT / "cube_project" / "model" / "cubes"),
                views_output_dir=str(PROJECT_ROOT / "cube_project" / "model" / "views"),
            )
        }
    )
