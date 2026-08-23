"""`CubeFilePromoter`: the resource `CubeDbtProjectComponent` delegates delivery of generated
cube/view YAML to. Promotion needs credentials and destination config (an S3 bucket, a git
remote, ...) -- exactly what Dagster resources are for -- so it lives here rather than as an
overridable method on the component itself, which is reserved for asset-shape customization
(translation, asset attributes) rather than runtime dependencies.
"""

from abc import ABC, abstractmethod
from pathlib import Path

import dagster as dg

from dagster_cube_dbt.output import read_entities, write_entities


class CubeFilePromoter(dg.ConfigurableResource, ABC):
    """Base resource for delivering generated cube/view YAML wherever a running Cube instance
    reads its schema from. `CubeDbtProjectComponent` has no default implementation to bind --
    there's no deployment topology where a fixed on-disk path is reachable by both a Dagster
    run and an independently-running Cube instance (Dagster Cloud and most container-per-run
    setups give each run its own throwaway filesystem) -- so a concrete subclass must be
    implemented and bound as a resource under the component's `promoter_resource_key`
    (`cube_file_promoter` by default).

    Example -- pushing to S3:

        .. code-block:: python

            import boto3
            from dagster_cube_dbt import CubeFilePromoter

            class S3CubeFilePromoter(CubeFilePromoter):
                bucket: str

                def promote(self, context, cubes_dir, views_dir):
                    s3 = boto3.client("s3")
                    for path in [*cubes_dir.glob("*.yaml"), *views_dir.glob("*.yaml")]:
                        s3.upload_file(str(path), self.bucket, f"cubes/{path.name}")

    See `CubeDbtProjectComponent`'s docstring for how to bind a concrete instance to the
    component's `promoter_resource_key`.
    """

    @abstractmethod
    def promote(self, context: dg.AssetExecutionContext, cubes_dir: Path, views_dir: Path) -> None:
        """Called once per materialization of the cube/view multi-asset, before any
        `MaterializeResult` is yielded (so raising here fails the run rather than reporting a
        false materialization). `cubes_dir`/`views_dir` hold *every* currently generated (and
        merge-patched) cube/view YAML file -- not just whatever subset of assets was actually
        selected for this run -- staged in a temp directory that's deleted as soon as this
        call returns, so an implementation must actually move/copy/upload what it needs before
        returning, not defer it.
        """
        ...


class LocalFileCubeFilePromoter(CubeFilePromoter):
    """Writes generated cube/view YAML straight to a fixed directory on disk, on the
    assumption that whatever reads it (typically a `cube dev` process running on the same
    machine) shares that filesystem. Only makes sense for local development -- true on a
    laptop, essentially never true in a real deployment. For anything beyond local dev,
    implement your own `CubeFilePromoter` instead.
    """

    # str, not Path -- ConfigurableResource fields must resolve to a Dagster config type, and
    # Path isn't one.
    output_dir: str
    views_output_dir: str | None = None

    def promote(self, context: dg.AssetExecutionContext, cubes_dir: Path, views_dir: Path) -> None:
        output_dir = Path(self.output_dir)
        views_output_dir = Path(self.views_output_dir) if self.views_output_dir else output_dir
        write_entities(output_dir, "cubes", read_entities(cubes_dir, "cubes"))
        write_entities(views_output_dir, "views", read_entities(views_dir, "views"))
