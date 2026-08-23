import pytest

from dagster_cube_dbt.generation import (
    ConflictingCubeNameError,
    MissingDimensionTypeError,
    UnenforcedContractError,
    UnrecognizedColumnTypeError,
    UnsupportedGeoDimensionError,
    generate_cubes,
)


def _column(name, *, data_type="present", description="", meta=None, tags=None, constraints=None):
    column = {
        "name": name,
        "description": description,
        "meta": meta if meta is not None else {},
        "tags": tags if tags is not None else [],
    }
    if data_type is not None:
        column["data_type"] = data_type
    if constraints is not None:
        column["constraints"] = constraints
    return column


def _model(
    name,
    *,
    path="marts/model.sql",
    columns=None,
    description="",
    tags=None,
    materialized="table",
    constraints=None,
    database="db",
    schema="sch",
    alias=None,
    contract_enforced=True,
    config_primary_key=None,
    config_order_by=None,
    meta=None,
):
    node = {
        "name": name,
        "resource_type": "model",
        "path": path,
        "description": description,
        "database": database,
        "schema": schema,
        "config": {"materialized": materialized, "tags": tags if tags is not None else []},
        "columns": {c["name"]: c for c in (columns or [])},
        "contract": {"enforced": contract_enforced},
        "meta": meta if meta is not None else {},
    }
    if alias is not None:
        node["alias"] = alias
    if constraints is not None:
        node["constraints"] = constraints
    if config_primary_key is not None:
        node["config"]["primary_key"] = config_primary_key
    if config_order_by is not None:
        node["config"]["order_by"] = config_order_by
    return node


def _unique_not_null_tests(project, model_name, column_name):
    return {
        f"test.{project}.unique_{model_name}_{column_name}.abc": {
            "resource_type": "test",
            "test_metadata": {"name": "unique", "kwargs": {"column_name": column_name}},
            "depends_on": {"nodes": [f"model.{project}.{model_name}"]},
        },
        f"test.{project}.not_null_{model_name}_{column_name}.def": {
            "resource_type": "test",
            "test_metadata": {"name": "not_null", "kwargs": {"column_name": column_name}},
            "depends_on": {"nodes": [f"model.{project}.{model_name}"]},
        },
    }


def _manifest(models, extra_nodes=None):
    nodes = {f"model.testproj.{m['name']}": m for m in models}
    nodes.update(extra_nodes or {})
    return {"nodes": nodes}


def test_generates_dimension_with_type_and_description():
    manifest = _manifest(
        [
            _model(
                "journey_samples",
                description="Individual journey samples.",
                columns=[
                    _column("journey_type", data_type="varchar", description="The type of journey."),
                ],
            )
        ]
    )

    result = generate_cubes(manifest)

    [cube] = result["cubes"]
    assert cube["name"] == "journey_samples"
    assert cube["description"] == "Individual journey samples."
    [dimension] = cube["dimensions"]
    assert dimension["name"] == "journey_type"
    assert dimension["type"] == "string"
    assert dimension["description"] == "The type of journey."


def test_description_omitted_when_absent():
    manifest = _manifest(
        [_model("journey_samples", columns=[_column("journey_type", data_type="varchar")])]
    )

    result = generate_cubes(manifest)

    [cube] = result["cubes"]
    assert "description" not in cube
    assert "description" not in cube["dimensions"][0]


def test_no_measures_or_joins_generated():
    manifest = _manifest(
        [_model("journey_samples", columns=[_column("journey_type", data_type="varchar")])]
    )

    result = generate_cubes(manifest)

    [cube] = result["cubes"]
    assert "measures" not in cube
    assert "joins" not in cube


def test_column_with_no_data_type_raises_with_all_offenders_listed():
    manifest = _manifest(
        [
            _model(
                "journey_samples",
                columns=[
                    _column("journey_type", data_type=None),
                    _column("direction", data_type=None),
                    _column("journey_sample_key", data_type="varchar"),
                ],
            )
        ]
    )

    with pytest.raises(MissingDimensionTypeError) as excinfo:
        generate_cubes(manifest)

    assert excinfo.value.missing == [
        "journey_samples.journey_type",
        "journey_samples.direction",
    ]
    assert "journey_samples.journey_type" in str(excinfo.value)
    assert "journey_samples.direction" in str(excinfo.value)


def test_unrecognized_data_type_raises_with_all_offenders_listed():
    """`manifest.TYPE_MAPPINGS` has no built-in mapping for arbitrary/exotic data_types --
    generation surfaces a clear, actionable error for these, collecting every offending
    column across the run the same way MissingDimensionTypeError does.
    """
    manifest = _manifest(
        [
            _model(
                "dates",
                columns=[
                    _column("weird_column", data_type="SomeExoticWarehouseType"),
                    _column("another_weird_one", data_type="Tuple(String, Int32)"),
                    _column("calendar_date", data_type="date"),
                ],
            )
        ]
    )

    with pytest.raises(UnrecognizedColumnTypeError) as excinfo:
        generate_cubes(manifest)

    assert excinfo.value.missing == [
        "dates.weird_column (SomeExoticWarehouseType)",
        "dates.another_weird_one (Tuple(String, Int32))",
    ]
    assert "dates.weird_column" in str(excinfo.value)
    assert "dates.another_weird_one" in str(excinfo.value)
    assert "geo" not in str(excinfo.value)


def test_geography_data_type_raises_unsupported_geo_dimension_error():
    """BigQuery's `GEOGRAPHY` maps to Cube's `geo` type in TYPE_MAPPINGS (real Cube type, so
    it's not "unrecognized"), but generation has no way to actually build a `geo` dimension --
    it needs `latitude`/`longitude` SQL sub-expressions instead of a single `sql` field, per
    Cube's own docs. This used to silently succeed and produce a broken dimension; it must
    raise instead.
    """
    manifest = _manifest(
        [
            _model(
                "geographic_locations",
                columns=[
                    _column("geom", data_type="GEOGRAPHY"),
                    _column("name", data_type="varchar"),
                ],
            )
        ]
    )

    with pytest.raises(UnsupportedGeoDimensionError) as excinfo:
        generate_cubes(manifest)

    assert excinfo.value.missing == ["geographic_locations.geom"]
    assert "geographic_locations.geom" in str(excinfo.value)


def test_meta_cube_type_geo_override_raises_unsupported_geo_dimension_error():
    """An explicit `meta.cube.type: geo` override hits the same wall as an inferred one --
    the override mechanism has no way to supply the required latitude/longitude either.
    """
    manifest = _manifest(
        [
            _model(
                "geographic_locations",
                columns=[_column("geom", data_type="Point", meta={"cube": {"type": "geo"}})],
            )
        ]
    )

    with pytest.raises(UnsupportedGeoDimensionError) as excinfo:
        generate_cubes(manifest)

    assert excinfo.value.missing == ["geographic_locations.geom"]


def test_clickhouse_types_are_natively_recognized():
    """Real bug report: a ClickHouse date/time-dimension model can easily have 20+ columns
    using explicitly-sized int/uint types and Date32 -- these are mapped directly rather than
    requiring a `meta.cube.type` override on every single one.
    """
    manifest = _manifest(
        [
            _model(
                "dates",
                columns=[
                    _column("date_day", data_type="Date32"),
                    _column("created_at", data_type="DateTime64(3)"),
                    _column("day_dow_num", data_type="UInt8"),
                    _column("year_num", data_type="UInt16"),
                    _column("some_id", data_type="UInt32"),
                    _column("big_id", data_type="UInt64"),
                    # Signed variants -- real bug report: these were missing entirely (only
                    # the UInt* family and Int8/Int64, which happened to collide with
                    # Redshift/Snowflake vocabulary, had been added), causing e.g.
                    # `statistical_area_1_id Int32` to fail as unrecognized.
                    _column("small_signed_id", data_type="Int16"),
                    _column("signed_id", data_type="Int32"),
                    _column("is_weekend", data_type="Bool"),
                    _column("external_id", data_type="UUID"),
                ],
            )
        ]
    )

    result = generate_cubes(manifest)

    types_by_name = {d["name"]: d["type"] for d in result["cubes"][0]["dimensions"]}
    assert types_by_name == {
        "date_day": "time",
        "created_at": "time",
        "day_dow_num": "number",
        "year_num": "number",
        "some_id": "number",
        "big_id": "number",
        "small_signed_id": "number",
        "signed_id": "number",
        "is_weekend": "boolean",
        "external_id": "string",
    }


def test_nullable_and_low_cardinality_wrappers_resolve_to_their_inner_type():
    """Nullable(T)/LowCardinality(T) can wrap *any* type -- naively stripping parenthesized
    content (as done for e.g. Decimal(10,2)) would collapse both Nullable(String) and
    Nullable(Int32) to the same bare "nullable", losing the real type entirely. These are
    unwrapped and recursed into instead, including nested combinations.
    """
    manifest = _manifest(
        [
            _model(
                "events",
                columns=[
                    _column("maybe_count", data_type="Nullable(UInt32)"),
                    _column("maybe_name", data_type="Nullable(String)"),
                    _column("category", data_type="LowCardinality(String)"),
                    _column("maybe_category", data_type="LowCardinality(Nullable(String))"),
                ],
            )
        ]
    )

    result = generate_cubes(manifest)

    types_by_name = {d["name"]: d["type"] for d in result["cubes"][0]["dimensions"]}
    assert types_by_name == {
        "maybe_count": "number",
        "maybe_name": "string",
        "category": "string",
        "maybe_category": "string",
    }


def test_meta_cube_type_override_bypasses_unrecognized_data_type():
    manifest = _manifest(
        [
            _model(
                "dates",
                columns=[
                    _column("date_day", data_type="Date32", meta={"cube": {"type": "time"}}),
                ],
            )
        ]
    )

    result = generate_cubes(manifest)

    [dimension] = result["cubes"][0]["dimensions"]
    assert dimension["type"] == "time"
    assert "meta" not in dimension  # the override is fully consumed, not passed through


def test_meta_cube_type_override_takes_priority_over_a_recognized_type_too():
    manifest = _manifest(
        [
            _model(
                "journey_samples",
                columns=[
                    _column(
                        "journey_type", data_type="varchar", meta={"cube": {"type": "number"}}
                    ),
                ],
            )
        ]
    )

    result = generate_cubes(manifest)

    [dimension] = result["cubes"][0]["dimensions"]
    assert dimension["type"] == "number"  # override wins even though "varchar" -> "string"


def test_model_without_enforced_contract_raises_with_all_offenders_listed():
    manifest = _manifest(
        [
            _model(
                "journey_samples",
                columns=[_column("journey_type", data_type="varchar")],
                contract_enforced=False,
            ),
            _model(
                "destination_locations",
                columns=[_column("location_name", data_type="varchar")],
                contract_enforced=False,
            ),
            _model(
                "dates",
                columns=[_column("calendar_date", data_type="date")],
                contract_enforced=True,
            ),
        ]
    )

    with pytest.raises(UnenforcedContractError) as excinfo:
        generate_cubes(manifest)

    assert excinfo.value.missing == ["journey_samples", "destination_locations"]
    assert "journey_samples" in str(excinfo.value)
    assert "destination_locations" in str(excinfo.value)
    assert "dates" not in str(excinfo.value)


def test_model_with_no_columns_declared_raises_via_contract_check():
    """The original bug report this check exists for: a model selected for cube generation
    with no `columns:` block in schema.yml at all previously produced a silently empty
    `dimensions: []` with no error, since MissingDimensionTypeError only checks columns that
    are already present in the manifest -- there was nothing to iterate. An unenforced
    contract now catches this before that silent case is ever reached.
    """
    manifest = _manifest([_model("journey_samples", columns=[], contract_enforced=False)])

    with pytest.raises(UnenforcedContractError) as excinfo:
        generate_cubes(manifest)

    assert excinfo.value.missing == ["journey_samples"]


def test_contract_check_takes_priority_over_missing_data_type_check():
    """A model can't actually be both unenforced *and* missing a data_type on a declared
    column in a real dbt project -- dbt itself refuses to parse a contracted model unless
    every declared column has a data_type, so the two errors are mutually exclusive in
    practice. This just pins that when a model fails the contract check, its columns are
    never even inspected for the data_type check (`continue` skips straight past them).
    """
    manifest = _manifest(
        [
            _model(
                "journey_samples",
                columns=[_column("journey_type", data_type=None)],
                contract_enforced=False,
            )
        ]
    )

    with pytest.raises(UnenforcedContractError):
        generate_cubes(manifest)


def test_meta_cube_dimension_false_excludes_column_and_exempts_type_check():
    manifest = _manifest(
        [
            _model(
                "journey_samples",
                columns=[
                    _column(
                        "internal_row_hash",
                        data_type=None,
                        meta={"cube": {"dimension": False}},
                    ),
                    _column("journey_type", data_type="varchar"),
                ],
            )
        ]
    )

    result = generate_cubes(manifest)

    [cube] = result["cubes"]
    dimension_names = [d["name"] for d in cube["dimensions"]]
    assert dimension_names == ["journey_type"]


def test_promoted_meta_keys_become_top_level_dimension_attributes():
    manifest = _manifest(
        [
            _model(
                "journey_samples",
                columns=[
                    _column(
                        "journey_type",
                        data_type="varchar",
                        meta={"cube": {"order": 3, "mask": True, "public": False}},
                    ),
                ],
            )
        ]
    )

    result = generate_cubes(manifest)

    [dimension] = result["cubes"][0]["dimensions"]
    assert dimension["order"] == 3
    assert dimension["mask"] is True
    assert dimension["public"] is False
    assert "meta" not in dimension  # fully consumed, nothing left to pass through


def test_unrecognized_cube_meta_and_non_cube_meta_still_pass_through():
    manifest = _manifest(
        [
            _model(
                "journey_samples",
                columns=[
                    _column(
                        "journey_type",
                        data_type="varchar",
                        meta={"owner": "team-x", "cube": {"order": 1, "unknown_key": "kept"}},
                    ),
                ],
            )
        ]
    )

    result = generate_cubes(manifest)

    [dimension] = result["cubes"][0]["dimensions"]
    assert dimension["order"] == 1
    assert dimension["meta"] == {"owner": "team-x", "cube": {"unknown_key": "kept"}}


def test_dimension_control_flag_never_leaks_into_output_meta():
    manifest = _manifest(
        [
            _model(
                "journey_samples",
                columns=[
                    _column(
                        "journey_type",
                        data_type="varchar",
                        meta={"cube": {"dimension": True, "order": 2}},
                    ),
                ],
            )
        ]
    )

    result = generate_cubes(manifest)

    [dimension] = result["cubes"][0]["dimensions"]
    assert dimension["order"] == 2
    assert "meta" not in dimension  # `dimension: True` was consumed, not passed through


def test_promoted_meta_keys_become_top_level_cube_attributes():
    manifest = _manifest(
        [
            _model(
                "journey_samples",
                columns=[_column("journey_type", data_type="varchar")],
                meta={"cube": {"public": False, "title": "Journey Samples"}},
            )
        ]
    )

    result = generate_cubes(manifest)

    [cube] = result["cubes"]
    assert cube["public"] is False
    assert cube["title"] == "Journey Samples"
    assert "meta" not in cube  # fully consumed, nothing left to pass through


def test_unrecognized_cube_meta_and_non_cube_meta_still_pass_through_on_the_cube():
    manifest = _manifest(
        [
            _model(
                "journey_samples",
                columns=[_column("journey_type", data_type="varchar")],
                meta={"owner": "team-x", "cube": {"title": "Journey Samples", "unknown_key": "kept"}},
            )
        ]
    )

    result = generate_cubes(manifest)

    [cube] = result["cubes"]
    assert cube["title"] == "Journey Samples"
    assert cube["meta"] == {"owner": "team-x", "cube": {"unknown_key": "kept"}}


def test_meta_cube_name_overrides_the_cube_name_outright():
    manifest = _manifest(
        [
            _model(
                "journey_samples",
                columns=[_column("journey_type", data_type="varchar")],
                meta={"cube": {"name": "journeys"}},
            )
        ]
    )

    result = generate_cubes(manifest)

    [cube] = result["cubes"]
    assert cube["name"] == "journeys"
    assert "name" not in cube.get("meta", {}).get("cube", {})  # consumed, not left over
    assert result["cube_source_models"] == {"journeys": "journey_samples"}


def test_meta_cube_suffix_appends_to_the_model_name():
    """The common Cube pattern this enables: a `public: false`, `_base`-suffixed cube that a
    hand-authored `extends:` cube (via a merge patch) exposes publicly under a plain name.
    """
    manifest = _manifest(
        [
            _model(
                "journey_samples",
                columns=[_column("journey_type", data_type="varchar")],
                meta={"cube": {"suffix": "_base", "public": False}},
            )
        ]
    )

    result = generate_cubes(manifest)

    [cube] = result["cubes"]
    assert cube["name"] == "journey_samples_base"
    assert cube["public"] is False
    assert result["cube_source_models"] == {"journey_samples_base": "journey_samples"}


def test_meta_cube_name_and_suffix_together_raises_with_all_offenders_listed():
    manifest = _manifest(
        [
            _model(
                "journey_samples",
                columns=[_column("journey_type", data_type="varchar")],
                meta={"cube": {"name": "journeys", "suffix": "_base"}},
            ),
            _model(
                "exchange_rates",
                columns=[_column("currency", data_type="varchar")],
                meta={"cube": {"name": "rates", "suffix": "_base"}},
            ),
        ]
    )

    with pytest.raises(ConflictingCubeNameError) as excinfo:
        generate_cubes(manifest)

    assert excinfo.value.missing == ["journey_samples", "exchange_rates"]
    assert "journey_samples" in str(excinfo.value)
    assert "exchange_rates" in str(excinfo.value)


def test_cube_source_models_maps_unrenamed_cubes_too():
    manifest = _manifest(
        [_model("journey_samples", columns=[_column("journey_type", data_type="varchar")])]
    )

    result = generate_cubes(manifest)

    assert result["cube_source_models"] == {"journey_samples": "journey_samples"}


def test_cube_select_paths_filters_models():
    manifest = _manifest(
        [
            _model("mart_model", path="marts/mart_model.sql", columns=[_column("id", data_type="varchar")]),
            _model(
                "intermediate_model",
                path="intermediate/intermediate_model.sql",
                columns=[_column("id", data_type="varchar")],
            ),
        ]
    )

    result = generate_cubes(manifest, paths=["marts"])

    assert [c["name"] for c in result["cubes"]] == ["mart_model"]


def test_cube_select_names_filters_models():
    manifest = _manifest(
        [
            _model("a", columns=[_column("id", data_type="varchar")]),
            _model("b", columns=[_column("id", data_type="varchar")]),
        ]
    )

    result = generate_cubes(manifest, names=["a"])

    assert [c["name"] for c in result["cubes"]] == ["a"]


def test_cube_select_tags_filters_models():
    manifest = _manifest(
        [
            _model("a", tags=["cube"], columns=[_column("id", data_type="varchar")]),
            _model("b", tags=[], columns=[_column("id", data_type="varchar")]),
        ]
    )

    result = generate_cubes(manifest, tags=["cube"])

    assert [c["name"] for c in result["cubes"]] == ["a"]


def test_primary_key_detected_from_unique_not_null_tests():
    manifest = _manifest(
        [
            _model(
                "journey_samples",
                columns=[_column("journey_sample_key", data_type="varchar")],
            )
        ],
        extra_nodes=_unique_not_null_tests("testproj", "journey_samples", "journey_sample_key"),
    )

    result = generate_cubes(manifest)

    [cube] = result["cubes"]
    assert cube["dimensions"][0]["primary_key"] is True


def test_primary_key_detected_from_constraint():
    manifest = _manifest(
        [
            _model(
                "journey_samples",
                columns=[_column("journey_sample_key", data_type="varchar")],
                constraints=[{"type": "primary_key", "columns": ["journey_sample_key"]}],
            )
        ]
    )

    result = generate_cubes(manifest)

    [cube] = result["cubes"]
    assert cube["dimensions"][0]["primary_key"] is True


def test_primary_key_detected_from_column_level_constraint():
    manifest = _manifest(
        [
            _model(
                "dates",
                columns=[
                    _column(
                        "date_key",
                        data_type="string",
                        constraints=[{"type": "primary_key"}],
                    ),
                    _column("date_day", data_type="Date32"),
                ],
            )
        ]
    )

    result = generate_cubes(manifest)

    [cube] = result["cubes"]
    dimensions = {d["name"]: d for d in cube["dimensions"]}
    assert dimensions["date_key"]["primary_key"] is True
    assert "primary_key" not in dimensions["date_day"]


def test_model_level_primary_key_constraint_takes_priority_over_column_level():
    manifest = _manifest(
        [
            _model(
                "dates",
                columns=[
                    _column(
                        "date_key",
                        data_type="string",
                        constraints=[{"type": "primary_key"}],
                    ),
                    _column("other_key", data_type="string"),
                ],
                constraints=[{"type": "primary_key", "columns": ["other_key"]}],
            )
        ]
    )

    result = generate_cubes(manifest)

    [cube] = result["cubes"]
    dimensions = {d["name"]: d for d in cube["dimensions"]}
    assert dimensions["other_key"]["primary_key"] is True
    assert "primary_key" not in dimensions["date_key"]


def test_primary_key_detected_from_config_primary_key_string():
    """ClickHouse has no SQL primary-key constraint at all -- `config(primary_key=...)` on
    the model is the only way to declare one there, and it lives in a completely separate
    manifest field (`config.primary_key`) from the generic `constraints` mechanism. Confirmed
    against real `dbt parse` output (dbt-clickhouse).
    """
    manifest = _manifest(
        [
            _model(
                "dates",
                columns=[
                    _column("date_key", data_type="string"),
                    _column("date_day", data_type="Date32"),
                ],
                config_primary_key="date_key",
            )
        ]
    )

    result = generate_cubes(manifest)

    [cube] = result["cubes"]
    dimensions = {d["name"]: d for d in cube["dimensions"]}
    assert dimensions["date_key"]["primary_key"] is True
    assert "primary_key" not in dimensions["date_day"]


def test_primary_key_detected_from_config_primary_key_list():
    """A composite `config(primary_key=[...])`, confirmed against real `dbt parse` output
    (dbt-clickhouse) to be represented as a plain list in the manifest.
    """
    manifest = _manifest(
        [
            _model(
                "dates",
                columns=[
                    _column("date_key", data_type="string"),
                    _column("date_day", data_type="Date32"),
                ],
                config_primary_key=["date_key", "date_day"],
            )
        ]
    )

    result = generate_cubes(manifest)

    [cube] = result["cubes"]
    dimensions = {d["name"]: d for d in cube["dimensions"]}
    assert dimensions["date_key"]["primary_key"] is True
    assert dimensions["date_day"]["primary_key"] is True


def test_config_primary_key_only_consulted_when_constraints_are_absent():
    manifest = _manifest(
        [
            _model(
                "dates",
                columns=[
                    _column(
                        "date_key",
                        data_type="string",
                        constraints=[{"type": "primary_key"}],
                    ),
                    _column("date_day", data_type="Date32"),
                ],
                config_primary_key="date_day",
            )
        ]
    )

    result = generate_cubes(manifest)

    [cube] = result["cubes"]
    dimensions = {d["name"]: d for d in cube["dimensions"]}
    assert dimensions["date_key"]["primary_key"] is True
    assert "primary_key" not in dimensions["date_day"]


def test_primary_key_falls_back_to_config_order_by_when_no_primary_key_is_set():
    """ClickHouse MergeTree semantics, confirmed against ClickHouse's own docs: "If no
    primary key is defined... ClickHouse uses the sorting key as primary key." So a model
    with `order_by` but no `primary_key` config still has a real, well-defined primary key --
    the entire `order_by` expression.
    """
    manifest = _manifest(
        [
            _model(
                "dates",
                columns=[
                    _column("date_key", data_type="string"),
                    _column("date_day", data_type="Date32"),
                    _column("description", data_type="string"),
                ],
                config_order_by=["date_key", "date_day"],
            )
        ]
    )

    result = generate_cubes(manifest)

    [cube] = result["cubes"]
    dimensions = {d["name"]: d for d in cube["dimensions"]}
    assert dimensions["date_key"]["primary_key"] is True
    assert dimensions["date_day"]["primary_key"] is True
    assert "primary_key" not in dimensions["description"]


def test_config_primary_key_takes_priority_over_config_order_by():
    manifest = _manifest(
        [
            _model(
                "dates",
                columns=[
                    _column("date_key", data_type="string"),
                    _column("date_day", data_type="Date32"),
                ],
                config_primary_key="date_key",
                config_order_by=["date_key", "date_day"],
            )
        ]
    )

    result = generate_cubes(manifest)

    [cube] = result["cubes"]
    dimensions = {d["name"]: d for d in cube["dimensions"]}
    assert dimensions["date_key"]["primary_key"] is True
    assert "primary_key" not in dimensions["date_day"]


def test_config_order_by_expression_that_matches_no_column_is_ignored():
    """`order_by` can hold an arbitrary SQL expression, not just plain column names (e.g.
    `toStartOfMonth(event_time)`) -- that should be silently dropped rather than treated as
    a bogus primary-key column name, falling through to the tags/tests heuristic instead.
    """
    manifest = _manifest(
        [
            _model(
                "dates",
                columns=[_column("date_key", data_type="string")],
                config_order_by="toStartOfMonth(date_key)",
            )
        ],
        extra_nodes=_unique_not_null_tests("testproj", "dates", "date_key"),
    )

    result = generate_cubes(manifest)

    [cube] = result["cubes"]
    assert cube["dimensions"][0]["primary_key"] is True


