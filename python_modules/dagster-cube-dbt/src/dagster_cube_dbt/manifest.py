"""Minimal, self-contained reading of a dbt `manifest.json` for cube generation.

Vendored (not depended on) in place of the `cube_dbt` PyPI package -- see DECISIONS.md for
why: `cube_dbt` is small (~540 lines) but its public API doesn't cover what this library
actually needs (contract status, a type-override escape hatch), forcing reaches into its
private attributes for the rest; the actual reusable logic here is a small, stable subset
(model/column filtering, primary-key detection, dbt-type-to-Cube-type mapping) worth owning
directly rather than depending on an external package maintained on a roughly 5-14-month
release cadence, especially with Cube's own newer dbt integration effort apparently going
into a separate, Cube-Cloud product feature rather than this package.

Operates on plain dicts throughout (dbt manifest node shape), matching the rest of this
library's style -- no wrapper classes, since the previous `cube_dbt.Model`/`Column` wrappers
added no real behavior beyond what's here.
"""

import re
from collections.abc import Mapping, Sequence
from typing import Any

ModelNode = Mapping[str, Any]
ColumnNode = Mapping[str, Any]

# model_name -> column_name -> [test names] (e.g. ["unique", "not_null"])
TestIndex = dict[str, dict[str, list[str]]]


def build_test_index(manifest: Mapping[str, Any]) -> TestIndex:
    """Indexes dbt generic tests (unique/not_null/etc.) by the model and column they target,
    for primary-key detection. Mirrors dbt's own `depends_on.nodes` convention: a test node's
    unique_id lists the node(s) it depends on, and a column-level test's `test_metadata.kwargs`
    names the column.
    """
    index: TestIndex = {}
    for node in manifest.get("nodes", {}).values():
        if node.get("resource_type") != "test":
            continue
        test_metadata = node.get("test_metadata")
        if not test_metadata:
            continue
        test_name = test_metadata.get("name")
        column_name = test_metadata.get("kwargs", {}).get("column_name")
        if not test_name or not column_name:
            continue
        for dep in node.get("depends_on", {}).get("nodes", []):
            if not dep.startswith("model."):
                continue
            model_name = dep.split(".")[-1]
            index.setdefault(model_name, {}).setdefault(column_name, []).append(test_name)
    return index


def filter_models(
    manifest: Mapping[str, Any],
    paths: Sequence[str] = (),
    tags: Sequence[str] = (),
    names: Sequence[str] = (),
) -> list[ModelNode]:
    """Every non-ephemeral model node matching all of `paths`/`tags`/`names` (each an
    empty/no-op filter when not given). `paths` matches by prefix against the model's
    manifest `path`.
    """
    return [
        node
        for node in manifest.get("nodes", {}).values()
        if node.get("resource_type") == "model"
        and node.get("config", {}).get("materialized") != "ephemeral"
        and (any(node["path"].startswith(path) for path in paths) if paths else True)
        and all(tag in node.get("config", {}).get("tags", []) for tag in tags)
        and (node["name"] in names if names else True)
    ]


def model_columns(model: ModelNode) -> list[ColumnNode]:
    return list(model.get("columns", {}).values())


def model_sql_table(model: ModelNode) -> str:
    if "relation_name" in model:
        return model["relation_name"]
    database = model["database"]
    schema = model["schema"]
    name = model.get("alias", model["name"])
    return f"`{database}`.`{schema}`.`{name}`"


def _as_name_set(value: str | Sequence[str] | None, column_names: set[str]) -> set[str]:
    """Normalizes a `config.primary_key`/`config.order_by`-shaped value (string, list, or
    absent) to a set of names, intersected against the model's actual columns -- `order_by`
    in particular can hold arbitrary SQL expressions (e.g. `toStartOfMonth(event_time)`), not
    just plain column names, and those should be silently dropped rather than treated as a
    (bogus) primary-key column name.
    """
    if not value:
        return set()
    raw = {value} if isinstance(value, str) else set(value)
    return raw & column_names


def model_primary_key_names(model: ModelNode, test_index: TestIndex) -> set[str]:
    """Primary key detection, in strict priority order (each tier used only if every tier
    above it found nothing):

    1. Model-level dbt 1.5+ constraint (`constraints: [{type: primary_key, columns: [...]}]`)
       -- the required form for composite keys. Confirmed against real `dbt parse` output
       (dbt-core + DuckDB) that this can never coexist with tier 2 on the same model: dbt's
       own parser hard-errors ("Primary key constraints defined at the model level and the
       columns level... not both") if both are declared, so a valid manifest only ever
       populates one of the two -- this is priority order for defensiveness, not because
       real manifests exercise it.
    2. Column-level dbt 1.5+ constraint (`columns: - name: x constraints: [{type:
       primary_key}]`).
    3. Adapter-specific `config.primary_key` model config, as a plain string or a list --
       ClickHouse doesn't support SQL primary-key constraints at all (its tables have no
       relational key concept), so `constraints`-based declaration is never available there;
       `config(primary_key=...)` on the model is the *only* way to declare one, and it's kept
       in a wholly separate manifest field from `constraints`, confirmed against real
       `dbt parse` output (dbt-clickhouse).
    4. Adapter-specific `config.order_by` model config, same string-or-list shape -- for a
       ClickHouse MergeTree table, `ORDER BY` *is* the primary key whenever `PRIMARY KEY`
       isn't explicitly set (confirmed against ClickHouse's own docs: "If no primary key is
       defined... ClickHouse uses the sorting key as primary key"), so this is consulted only
       when tier 3 found nothing.
    5. A column tagged `primary_key`, or covered by both a `unique` and a `not_null` test.
    """
    model_level = {
        column_name
        for constraint in model.get("constraints", [])
        if constraint.get("type") == "primary_key"
        for column_name in constraint.get("columns", [])
    }
    if model_level:
        return model_level

    column_level = {
        column["name"]
        for column in model_columns(model)
        if any(c.get("type") == "primary_key" for c in column.get("constraints", []))
    }
    if column_level:
        return column_level

    column_names = {column["name"] for column in model_columns(model)}
    config = model.get("config", {})

    config_primary_key = _as_name_set(config.get("primary_key"), column_names)
    if config_primary_key:
        return config_primary_key

    config_order_by = _as_name_set(config.get("order_by"), column_names)
    if config_order_by:
        return config_order_by

    column_tests = test_index.get(model["name"], {})
    return {
        column["name"]
        for column in model_columns(model)
        if _column_is_primary_key(column, column_tests.get(column["name"], []))
    }


def _column_is_primary_key(column: ColumnNode, tests: list[str]) -> bool:
    if "primary_key" in column.get("tags", []):
        return True
    return "unique" in tests and "not_null" in tests


def column_sql(column: ColumnNode) -> str:
    return column["name"]


def model_contract_enforced(model: ModelNode) -> bool:
    contract = model.get("contract")
    return isinstance(contract, dict) and bool(contract.get("enforced"))


# dbt data_type -> Cube dimension type. Combines the BigQuery/Redshift/Snowflake type
# vocabularies (the union covers every mainstream warehouse dbt commonly targets); anything
# outside this falls through to `infer_dimension_type` returning None, the signal for
# "cube_dbt doesn't recognize this" that generation.py's UnrecognizedColumnTypeError and
# meta.cube.type override are built around.
TYPE_MAPPINGS: dict[str, str] = {
    # BigQuery (https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types)
    "array": "string",
    "bool": "boolean",
    "bytes": "string",
    "date": "time",
    "datetime": "time",
    # Mapped for completeness -- "geo" is a real Cube dimension type -- but generation.py
    # always rejects it with a dedicated, explanatory error (`UnsupportedGeoDimensionError`)
    # rather than building a dimension: a `geo` dimension needs `latitude`/`longitude` SQL
    # sub-expressions instead of a single `sql` field, which this generic dbt-column mapping
    # has no way to derive.
    "geography": "geo",
    "interval": "string",
    "json": "string",
    "int64": "number",
    "int": "number",
    "smallint": "number",
    "integer": "number",
    "bigint": "number",
    "tinyint": "number",
    "byteint": "number",
    "numeric": "number",
    "decimal": "number",
    "bignumeric": "number",
    "bigdecimal": "number",
    "float64": "number",
    "range": "string",
    "struct": "string",
    "timestamp": "time",
    # Redshift (https://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html)
    "int2": "number",
    "int4": "number",
    "int8": "number",
    "real": "number",
    "float4": "number",
    "double precision": "number",
    "float8": "number",
    "float": "number",
    "char": "string",
    "character": "string",
    "nchar": "string",
    "bpchar": "string",
    "varchar": "string",
    "character varying": "string",
    "text": "string",
    "time": "time",
    "time without time zone": "time",
    "timetz": "time",
    "time with time zone": "time",
    "timestamp without time zone": "time",
    "timestamptz": "time",
    "timestamp with time zone": "time",
    "interval year to month": "string",
    "interval day to second": "string",
    "hllsketch": "string",
    "super": "string",
    "varbyte": "string",
    "varbinary": "string",
    "binary varying": "string",
    "geometry": "string",
    # Snowflake (https://docs.snowflake.com/en/sql-reference-data-types)
    "dec": "number",
    "double": "number",
    "nvarchar": "string",
    "nvarchar2": "string",
    "char varying": "string",
    "nchar varying": "string",
    "binary": "string",
    "timestamp_ltz": "time",
    "timestamp_ntz": "time",
    "timestamp_tz": "time",
    "variant": "string",
    "object": "string",
    "vector": "string",
    # ClickHouse (https://clickhouse.com/docs/en/sql-reference/data-types) -- added directly
    # (rather than via meta.cube.type on every affected column) because ClickHouse leans on
    # explicitly-sized int/uint types and Date32 pervasively, even for ordinary dimension
    # tables: a single date-dimension model can easily have 20+ columns needing this.
    "int16": "number",
    "int32": "number",
    "int128": "number",
    "int256": "number",
    "uint8": "number",
    "uint16": "number",
    "uint32": "number",
    "uint64": "number",
    "uint128": "number",
    "uint256": "number",
    "float32": "number",
    "date32": "time",
    "datetime64": "time",
    "uuid": "string",
    "fixedstring": "string",
    "enum8": "string",
    "enum16": "string",
    "ipv4": "string",
    "ipv6": "string",
}
VALID_DIMENSION_TYPES = ("boolean", "geo", "number", "string", "time")

# ClickHouse wrapper types that parameterize *another* type rather than just carrying
# precision/scale info (contrast Decimal(10,2) or DateTime64(3), where the parenthesized
# content can be safely discarded) -- naively stripping the parens here would throw away the
# only information that actually determines the Cube type, collapsing e.g. both
# Nullable(String) and Nullable(Int32) to the same bare "nullable". Handled by recursing into
# the wrapped type instead (see infer_dimension_type), so nested combinations like
# LowCardinality(Nullable(String)) resolve correctly too.
_WRAPPER_TYPE_RE = re.compile(r"^(nullable|lowcardinality)\((.+)\)$", re.IGNORECASE)


def infer_dimension_type(data_type: str | None) -> str | None:
    """Maps a dbt `data_type` to a Cube dimension type. Returns `None` -- rather than
    raising -- when `data_type` doesn't normalize to a known or already-valid Cube type, so
    callers can collect it as a violation (or let a `meta.cube.type` override take over)
    instead of crashing.
    """
    if not data_type:
        return "string"
    wrapped = _WRAPPER_TYPE_RE.match(data_type.strip())
    if wrapped:
        return infer_dimension_type(wrapped.group(2))
    normalized = re.sub(r"<.*>", "", re.sub(r"\([^)]*\)", "", data_type.lower()))
    cube_type = TYPE_MAPPINGS.get(normalized, normalized)
    return cube_type if cube_type in VALID_DIMENSION_TYPES else None
