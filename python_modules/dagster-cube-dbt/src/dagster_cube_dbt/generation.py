"""Pure, framework-agnostic generation of a base Cube YAML document from a dbt manifest.

No merge-patch handling and no Dagster/Component dependency lives here. See `merge.py` for
folding user-authored merge-patch files on top of what this module produces.
"""

from collections.abc import Mapping, Sequence
from typing import Any

from dagster_cube_dbt.manifest import (
    ColumnNode,
    ModelNode,
    build_test_index,
    column_sql,
    filter_models,
    infer_dimension_type,
    model_columns,
    model_contract_enforced,
    model_primary_key_names,
    model_sql_table,
)


class MissingDimensionTypeError(ValueError):
    """Raised when a column that would become a dimension has no `data_type` declared.

    dbt does not back-fill column types from the warehouse into manifest.json during
    parse/compile, so an undeclared `data_type` is a real gap, not a value that can be
    silently defaulted to `string`. Collects every offending column across a run so a user
    can fix them all at once.
    """

    def __init__(self, missing: list[str]) -> None:
        self.missing = missing
        offenders = "\n".join(f"  - {name}" for name in missing)
        super().__init__(
            "The following columns have no explicit `data_type` declared in dbt and cannot "
            "be reliably typed as a Cube dimension:\n"
            f"{offenders}\n"
            "Declare `data_type` on these columns in schema.yml, exclude them with "
            "`meta.cube.dimension: false`, or narrow `cube_select` to exclude their model."
        )


class UnrecognizedColumnTypeError(ValueError):
    """Raised when a column's dbt `data_type` can't be mapped to a Cube dimension type --
    typically a warehouse-specific type name with no entry in `manifest.TYPE_MAPPINGS` (e.g.
    ClickHouse's `Date32`, `UInt64`, or `Nullable(...)` -- the last of which can't be handled
    by a bigger mapping table in general, since parenthesized inner types are stripped before
    a type name is ever inspected, so `Nullable(String)` and `Nullable(Int32)` are
    indistinguishable). Collects every offending column across a run so a user can fix them
    all at once.
    """

    def __init__(self, missing: list[str]) -> None:
        self.missing = missing
        offenders = "\n".join(f"  - {name}" for name in missing)
        super().__init__(
            "The following columns' dbt data_type can't be mapped to a Cube dimension type:\n"
            f"{offenders}\n"
            "Set `meta.cube.type` on each column to the Cube dimension type it should be "
            "(one of: string, number, time, boolean), exclude it with "
            "`meta.cube.dimension: false`, or narrow `cube_select` to exclude their model."
        )


class UnsupportedGeoDimensionError(ValueError):
    """Raised when a column would resolve to Cube's `geo` dimension type -- either inferred
    from a data_type like BigQuery's `GEOGRAPHY` (see `manifest.TYPE_MAPPINGS`) or via an
    explicit `meta.cube.type: geo` override -- since generation has no way to actually build
    one. Per Cube's own docs, a `geo` dimension takes `latitude`/`longitude` SQL
    sub-expressions *instead of* a single `sql` field -- a structurally different shape from
    every other dimension this library generates (`_build_dimension` always emits a single
    `sql`) -- and there's no generic way to derive two separate SQL expressions from one dbt
    column's declared type (a ClickHouse `Point` is a `Tuple(Float64, Float64)`; other
    geometry types like `Polygon`/`MultiPolygon` have no single lat/long point at all).
    Collects every offending column across a run so a user can fix them all at once.
    """

    def __init__(self, missing: list[str]) -> None:
        self.missing = missing
        offenders = "\n".join(f"  - {name}" for name in missing)
        super().__init__(
            "The following columns would resolve to Cube's `geo` dimension type, which this "
            "library can't generate: a `geo` dimension requires separate `latitude`/"
            "`longitude` SQL sub-expressions instead of a single `sql` field, and there's no "
            "generic way to derive those from one dbt column's declared type:\n"
            f"{offenders}\n"
            "Exclude the column with `meta.cube.dimension: false`, or hand-author a `geo` "
            "dimension for it directly in a merge patch with explicit `latitude`/`longitude` "
            "SQL (e.g. a warehouse-specific coordinate-extraction expression)."
        )


class UnenforcedContractError(ValueError):
    """Raised when a model selected for cube generation doesn't have dbt contract
    enforcement turned on (`config: {contract: {enforced: true}}`).

    Cube generation only ever sees whatever columns happen to be declared in schema.yml --
    a model with no `columns:` block at all (or an incomplete one) produces a technically
    successful but silently useless cube (empty or partial `dimensions:`), since there's
    nothing for `MissingDimensionTypeError` above to catch when there's nothing declared to
    check in the first place. Enabling a dbt contract closes that gap: dbt itself refuses to
    even parse a contracted model unless every declared column has a `data_type`, and
    further validates at `dbt build` time that the model's real output can't drift to have
    columns beyond what's declared. Collects every offending model across a run so a user
    can fix them all at once.
    """

    def __init__(self, missing: list[str]) -> None:
        self.missing = missing
        offenders = "\n".join(f"  - {name}" for name in missing)
        super().__init__(
            "The following models selected for cube generation don't have dbt contract "
            "enforcement enabled, so their column declarations can't be trusted to be "
            "complete:\n"
            f"{offenders}\n"
            "Add `config: {contract: {enforced: true}}` to each model in schema.yml (this "
            "also requires declaring a data_type on every column, including ones excluded "
            "via `meta.cube.dimension: false`, since dbt's contract check has no notion of "
            "that flag), or narrow `cube_select` to exclude them."
        )


class ConflictingCubeNameError(ValueError):
    """Raised when a model sets both `meta.cube.name` and `meta.cube.suffix` -- mutually
    exclusive, since it's ambiguous whether `suffix` should still apply on top of an explicit
    `name` override or be ignored. Collects every offending model across a run so a user can
    fix them all at once.
    """

    def __init__(self, missing: list[str]) -> None:
        self.missing = missing
        offenders = "\n".join(f"  - {name}" for name in missing)
        super().__init__(
            "The following models set both `meta.cube.name` and `meta.cube.suffix`, which "
            "can't be combined:\n"
            f"{offenders}\n"
            "Use `meta.cube.name` to set the cube's full name outright, or "
            "`meta.cube.suffix` to append to the dbt model's own name instead -- not both."
        )


# meta.cube.* is a reserved namespace on dbt columns *and* models alike: on a column,
# `dimension` is a control flag consumed by generation and never emitted, and `type`
# overrides the inferred dimension type (handled separately, before _build_dimension, since
# consulting it can skip a type-inference failure entirely); on a model, `name` and `suffix`
# are control flags consumed by _resolve_cube_name (handled separately, before _build_cube,
# since the chosen name is a required positional argument to it) rather than becoming
# attributes named literally `name`/`suffix` on the cube. The keys below are promoted to real
# top-level attributes on the generated dimension/cube (rather than staying nested under its
# own `meta:`).
PROMOTED_CUBE_DIMENSION_META_KEYS = ("order", "mask", "public")
PROMOTED_CUBE_MODEL_META_KEYS = ("public", "title")


def _cube_meta(node: Mapping[str, Any]) -> dict[str, Any]:
    """The `cube` sub-dict of a dbt column's or model's `meta`, if any -- `ColumnNode` and
    `ModelNode` are both just `Mapping[str, Any]` aliases, and both carry `meta` in the same
    shape, so this one helper covers both.
    """
    meta = node.get("meta")
    cube_meta = meta.get("cube") if isinstance(meta, dict) else None
    return dict(cube_meta) if isinstance(cube_meta, dict) else {}


def _is_dimension_excluded(column: ColumnNode) -> bool:
    return _cube_meta(column).get("dimension") is False


def _resolve_cube_name(model: ModelNode) -> str | None:
    """The cube's name: `meta.cube.name` verbatim if set (a full override), `model['name']`
    with `meta.cube.suffix` appended if a suffix is set instead (plain string concatenation,
    no separator inserted -- include it in the suffix itself, e.g. `suffix: "_base"`), or
    just the dbt model's own name unchanged if neither is set. `None` if *both* `name` and
    `suffix` are set on the same model -- mutually exclusive, so callers should treat `None`
    as a conflict to collect as a violation rather than a name to use.

    A common Cube pattern this enables: generate a suffixed, `public: false` "base" cube (a
    ClickHouse-style `_base` suffix, say) that a hand-authored `extends:` cube (via a merge
    patch) then exposes publicly under the dbt model's own plain name.
    """
    cube_meta = _cube_meta(model)
    name_override = cube_meta.get("name")
    suffix = cube_meta.get("suffix")
    if name_override is not None and suffix is not None:
        return None
    if name_override is not None:
        return str(name_override)
    if suffix is not None:
        return f"{model['name']}{suffix}"
    return model["name"]


def _resolve_dimension_type(column: ColumnNode) -> str | None:
    """The Cube dimension type for `column`: an explicit `meta.cube.type` override if set,
    otherwise the type inferred from its dbt `data_type`. `None` if neither is available,
    so callers can collect it as a violation rather than build a wrongly/un-typed dimension.
    """
    override = _cube_meta(column).get("type")
    if override is not None:
        return override
    return infer_dimension_type(column.get("data_type"))


def _build_dimension(
    column: ColumnNode, dimension_type: str, primary_key_names: set[str]
) -> dict[str, Any]:
    dimension: dict[str, Any] = {"name": column["name"]}
    if column.get("description"):
        dimension["description"] = column["description"]
    dimension["sql"] = column_sql(column)
    dimension["type"] = dimension_type
    if column["name"] in primary_key_names:
        dimension["primary_key"] = True

    # Promote known meta.cube.* keys to real dimension attributes; consume the `dimension`
    # control flag and the `type` override (already applied via _resolve_dimension_type);
    # leave anything else (other cube.* keys, or meta outside the cube namespace) in the
    # dimension's own `meta:` so nothing is silently dropped.
    remaining_meta = dict(column.get("meta") or {})
    cube_meta = dict(remaining_meta.pop("cube", None) or {})
    cube_meta.pop("dimension", None)
    cube_meta.pop("type", None)
    for key in PROMOTED_CUBE_DIMENSION_META_KEYS:
        if key in cube_meta:
            dimension[key] = cube_meta.pop(key)
    if cube_meta:
        remaining_meta["cube"] = cube_meta
    if remaining_meta:
        dimension["meta"] = remaining_meta

    return dimension


def _build_cube(model: ModelNode, dimensions: list[dict[str, Any]], cube_name: str) -> dict[str, Any]:
    cube: dict[str, Any] = {"name": cube_name}
    if model.get("description"):
        cube["description"] = model["description"]
    cube["sql_table"] = model_sql_table(model)

    # Promote known meta.cube.* keys (e.g. `public`, `title`) to real cube attributes, the
    # same convention as _build_dimension above; consume the `name`/`suffix` control flags
    # (already applied via _resolve_cube_name -- `cube_name` above); leave anything else
    # (other cube.* keys, or meta outside the cube namespace) in the cube's own `meta:` so
    # nothing is silently dropped.
    remaining_meta = dict(model.get("meta") or {})
    cube_meta = dict(remaining_meta.pop("cube", None) or {})
    cube_meta.pop("name", None)
    cube_meta.pop("suffix", None)
    for key in PROMOTED_CUBE_MODEL_META_KEYS:
        if key in cube_meta:
            cube[key] = cube_meta.pop(key)
    if cube_meta:
        remaining_meta["cube"] = cube_meta
    if remaining_meta:
        cube["meta"] = remaining_meta

    cube["dimensions"] = dimensions
    return cube


def generate_cubes(
    manifest: Mapping[str, Any],
    paths: Sequence[str] = (),
    tags: Sequence[str] = (),
    names: Sequence[str] = (),
) -> dict[str, Any]:
    """Build the base `{cubes: [...], cube_source_models: {...}}` document for the models
    selected by `paths`/`tags`/`names`. `cube_source_models` maps each generated cube's final
    name (after any `meta.cube.name`/`suffix` rename) back to the dbt model name it came from
    -- not part of Cube's own schema, kept as a sibling key so callers can still resolve the
    real dbt model dependency behind a renamed cube without it leaking into the cube's own
    generated fields.

    No `measures` or `joins` are produced -- both are left entirely to merge patches. Raises
    `UnenforcedContractError` if any selected model doesn't have dbt contract enforcement
    turned on, `ConflictingCubeNameError` if a model sets both `meta.cube.name` and
    `meta.cube.suffix`, `MissingDimensionTypeError` if any surfaced column (one not excluded
    via `meta.cube.dimension: false`) has no `data_type` declared, `UnrecognizedColumnTypeError`
    if a surfaced column's `data_type` can't be mapped to a Cube type and no `meta.cube.type`
    override was given, or `UnsupportedGeoDimensionError` if a surfaced column resolves to
    Cube's `geo` type (inferred or via override) -- recognized, but not something this
    library can actually build a working dimension for.
    """
    selected_models = filter_models(manifest, paths=paths, tags=tags, names=names)
    test_index = build_test_index(manifest)

    uncontracted_models: list[str] = []
    conflicting_cube_names: list[str] = []
    missing_data_types: list[str] = []
    unrecognized_types: list[str] = []
    unsupported_geo_dimensions: list[str] = []
    cubes: list[dict[str, Any]] = []
    cube_source_models: dict[str, str] = {}

    for model in selected_models:
        if not model_contract_enforced(model):
            uncontracted_models.append(model["name"])
            continue

        cube_name = _resolve_cube_name(model)
        if cube_name is None:
            conflicting_cube_names.append(model["name"])
            continue

        primary_key_names = model_primary_key_names(model, test_index)

        dimensions: list[dict[str, Any]] = []
        for column in model_columns(model):
            if _is_dimension_excluded(column):
                continue
            if column.get("data_type") is None:
                missing_data_types.append(f"{model['name']}.{column['name']}")
                continue
            dimension_type = _resolve_dimension_type(column)
            if dimension_type is None:
                unrecognized_types.append(f"{model['name']}.{column['name']} ({column['data_type']})")
                continue
            if dimension_type == "geo":
                unsupported_geo_dimensions.append(f"{model['name']}.{column['name']}")
                continue
            dimensions.append(_build_dimension(column, dimension_type, primary_key_names))
        cubes.append(_build_cube(model, dimensions, cube_name))
        cube_source_models[cube_name] = model["name"]

    if uncontracted_models:
        raise UnenforcedContractError(uncontracted_models)
    if conflicting_cube_names:
        raise ConflictingCubeNameError(conflicting_cube_names)
    if missing_data_types:
        raise MissingDimensionTypeError(missing_data_types)
    if unrecognized_types:
        raise UnrecognizedColumnTypeError(unrecognized_types)
    if unsupported_geo_dimensions:
        raise UnsupportedGeoDimensionError(unsupported_geo_dimensions)

    return {"cubes": cubes, "cube_source_models": cube_source_models}
