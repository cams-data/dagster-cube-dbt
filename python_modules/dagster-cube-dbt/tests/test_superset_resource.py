"""Unit tests for `SupersetResource` against a scripted fake `requests.Session` -- no real
Superset instance in CI, same spirit as `test_landing_check.py`'s mocked `requests.get` and
`test_component_integration.py`'s `NoopCubeFilePromoter`.
"""

from unittest.mock import MagicMock

import dagster as dg
import pytest
import requests

from dagster_cube_dbt.superset_resource import SupersetResource, _raise_for_status, _verbose_name


class _FakeResponse:
    def __init__(self, json_data, status_code: int = 200):
        self._json = json_data
        self.status_code = status_code

    def json(self):
        return self._json

    def raise_for_status(self):
        pass


def _login_response():
    return _FakeResponse({"access_token": "tok"})


def _csrf_response():
    return _FakeResponse({"result": "csrf-tok"})


def _resource_with_session(session: MagicMock) -> SupersetResource:
    resource = SupersetResource(base_url="https://superset.example.com", username="u", password="p")
    resource._session = session  # noqa: SLF001 -- swapping in the fake for this test
    return resource


def test_raise_for_status_includes_the_response_body_in_the_error():
    """Regression test: `response.raise_for_status()` alone only reports the status line
    ("400 Client Error: BAD REQUEST for url: ..."), discarding Superset's own JSON error
    payload explaining *why* -- a real production report where that made a genuine 400 (a
    dataset-creation validation failure) undiagnosable without a packet capture.
    """
    response = requests.Response()
    response.status_code = 400
    response.url = "https://superset.example.com/api/v1/dataset/"
    response._content = b'{"message": {"table_name": ["Dataset already exists"]}}'

    with pytest.raises(requests.HTTPError, match="Dataset already exists"):
        _raise_for_status(response)


def test_raise_for_status_does_not_raise_for_a_successful_response():
    response = requests.Response()
    response.status_code = 200

    _raise_for_status(response)  # must not raise


def test_sync_dataset_creates_a_new_dataset_when_none_exists():
    session = MagicMock()
    session.post.side_effect = [
        _login_response(),
        _FakeResponse({"id": 42}, status_code=201),  # create dataset
    ]
    session.get.side_effect = [
        _csrf_response(),
        _FakeResponse({"result": [{"id": 7}]}),  # find database
        _FakeResponse({"result": []}),  # find dataset -- none found
        _FakeResponse(  # dataset detail after refresh
            {
                "result": {
                    "columns": [{"column_name": "amount", "created_on": "2020-01-01"}],
                    "metrics": [],
                }
            }
        ),
    ]
    session.put.side_effect = [
        _FakeResponse({}),  # refresh
        _FakeResponse({"result": {}}),  # update
    ]
    resource = _resource_with_session(session)

    dataset_id = resource.sync_dataset(
        database_name="Cube",
        schema="public",
        table_name="orders_overview",
        dimensions=[{"name": "amount", "title": "Amount", "type": "number"}],
        measures=[{"name": "total_amount", "description": "Sum of amount"}],
    )

    assert dataset_id == 42
    create_call = session.post.call_args_list[1]
    assert create_call.args[0] == "https://superset.example.com/api/v1/dataset/"
    assert create_call.kwargs["json"] == {
        "database": 7,
        "schema": "public",
        "table_name": "orders_overview",
        "normalize_columns": False,
        "always_filter_main_dttm": False,
    }

    update_call = session.put.call_args_list[1]
    updated_columns = update_call.kwargs["json"]["columns"]
    assert updated_columns == [
        {
            "column_name": "amount",
            "verbose_name": "Amount",
            "description": "",
            "is_dttm": False,
            "groupby": True,
            "filterable": True,
            "is_active": True,
        }
    ]
    updated_metrics = update_call.kwargs["json"]["metrics"]
    assert updated_metrics == [
        {
            "metric_name": "total_amount",
            "verbose_name": "Total Amount",
            "expression": "MEASURE(orders_overview.total_amount)",
            "description": "Sum of amount",
            "metric_type": "simple",
        }
    ]


def test_sync_dataset_reuses_an_existing_dataset_instead_of_creating_one():
    session = MagicMock()
    session.post.side_effect = [_login_response()]
    session.get.side_effect = [
        _csrf_response(),
        _FakeResponse({"result": [{"id": 7}]}),
        _FakeResponse({"result": [{"id": 99}]}),  # find dataset -- already exists
        _FakeResponse({"result": {"columns": [{"column_name": "amount"}], "metrics": []}}),
    ]
    session.put.side_effect = [_FakeResponse({}), _FakeResponse({"result": {}})]
    resource = _resource_with_session(session)

    dataset_id = resource.sync_dataset("Cube", "public", "orders_overview", [], [])

    assert dataset_id == 99
    session.post.assert_called_once()  # login only -- no dataset creation


def test_sync_dataset_verify_tls_defaults_to_true_on_the_session():
    session = MagicMock()
    session.post.side_effect = [_login_response()]
    session.get.side_effect = [
        _csrf_response(),
        _FakeResponse({"result": [{"id": 7}]}),
        _FakeResponse({"result": [{"id": 99}]}),
        _FakeResponse({"result": {"columns": [{"column_name": "amount"}], "metrics": []}}),
    ]
    session.put.side_effect = [_FakeResponse({}), _FakeResponse({"result": {}})]
    resource = _resource_with_session(session)

    resource.sync_dataset("Cube", "public", "orders_overview", [], [])

    assert session.verify is True


def test_sync_dataset_verify_tls_false_disables_certificate_verification_on_the_session():
    session = MagicMock()
    session.post.side_effect = [_login_response()]
    session.get.side_effect = [
        _csrf_response(),
        _FakeResponse({"result": [{"id": 7}]}),
        _FakeResponse({"result": [{"id": 99}]}),
        _FakeResponse({"result": {"columns": [{"column_name": "amount"}], "metrics": []}}),
    ]
    session.put.side_effect = [_FakeResponse({}), _FakeResponse({"result": {}})]
    resource = SupersetResource(
        base_url="https://superset.example.com", username="u", password="p", verify_tls=False
    )
    resource._session = session  # noqa: SLF001

    resource.sync_dataset("Cube", "public", "orders_overview", [], [])

    assert session.verify is False


def test_sync_dataset_with_api_key_skips_the_login_call():
    session = MagicMock()
    session.headers = {}
    session.get.side_effect = [
        _csrf_response(),
        _FakeResponse({"result": [{"id": 7}]}),
        _FakeResponse({"result": [{"id": 99}]}),
        _FakeResponse({"result": {"columns": [{"column_name": "amount"}], "metrics": []}}),
    ]
    session.put.side_effect = [_FakeResponse({}), _FakeResponse({"result": {}})]
    resource = SupersetResource(base_url="https://superset.example.com", api_key="my-key")
    resource._session = session  # noqa: SLF001

    resource.sync_dataset("Cube", "public", "orders_overview", [], [])

    session.post.assert_not_called()  # no /api/v1/security/login call at all
    assert session.headers["Authorization"] == "Bearer my-key"


def test_constructor_raises_when_no_authentication_is_configured():
    with pytest.raises(ValueError, match="api_key"):
        SupersetResource(base_url="https://superset.example.com")


def test_constructor_raises_when_api_key_and_password_auth_both_set():
    with pytest.raises(ValueError, match="not both"):
        SupersetResource(base_url="https://superset.example.com", api_key="k", username="u", password="p")


def test_sync_dataset_only_authenticates_once_across_multiple_calls():
    session = MagicMock()
    session.post.side_effect = [_login_response()]
    session.get.side_effect = [
        _csrf_response(),
        _FakeResponse({"result": [{"id": 7}]}),
        _FakeResponse({"result": [{"id": 1}]}),
        _FakeResponse({"result": {"columns": [{"column_name": "amount"}], "metrics": []}}),
        _FakeResponse({"result": [{"id": 7}]}),
        _FakeResponse({"result": [{"id": 2}]}),
        _FakeResponse({"result": {"columns": [{"column_name": "amount"}], "metrics": []}}),
    ]
    session.put.side_effect = [
        _FakeResponse({}),
        _FakeResponse({"result": {}}),
        _FakeResponse({}),
        _FakeResponse({"result": {}}),
    ]
    resource = _resource_with_session(session)

    resource.sync_dataset("Cube", "public", "view_a", [], [])
    resource.sync_dataset("Cube", "public", "view_b", [], [])

    session.post.assert_called_once()  # login happened exactly once, not per-view


def test_sync_dataset_raises_dagster_failure_when_database_is_not_found():
    session = MagicMock()
    session.post.side_effect = [_login_response()]
    session.get.side_effect = [_csrf_response(), _FakeResponse({"result": []})]
    resource = _resource_with_session(session)

    with pytest.raises(dg.Failure, match="Cube"):
        resource.sync_dataset("Cube", "public", "orders_overview", [], [])


def test_readonly_fields_are_stripped_from_columns_and_metrics_before_put():
    session = MagicMock()
    session.post.side_effect = [_login_response()]
    session.get.side_effect = [
        _csrf_response(),
        _FakeResponse({"result": [{"id": 7}]}),
        _FakeResponse({"result": [{"id": 1}]}),
        _FakeResponse(
            {
                "result": {
                    "columns": [
                        {
                            "column_name": "amount",
                            "created_on": "x",
                            "changed_on": "x",
                            "type_generic": 0,
                            "uuid": "abc",
                            "advanced_data_type": None,
                        }
                    ],
                    "metrics": [
                        {"metric_name": "total_amount", "created_on": "x", "changed_on": "x", "uuid": "abc"}
                    ],
                }
            }
        ),
    ]
    session.put.side_effect = [_FakeResponse({}), _FakeResponse({"result": {}})]
    resource = _resource_with_session(session)

    resource.sync_dataset(
        "Cube", "public", "orders_overview", [{"name": "amount"}], [{"name": "total_amount"}]
    )

    update_call = session.put.call_args_list[1]
    columns = update_call.kwargs["json"]["columns"]
    metrics = update_call.kwargs["json"]["metrics"]
    for readonly_field in ("created_on", "changed_on", "type_generic", "uuid", "advanced_data_type"):
        assert readonly_field not in columns[0]
    for readonly_field in ("created_on", "changed_on", "uuid"):
        assert readonly_field not in metrics[0]


def test_column_matching_is_case_insensitive():
    session = MagicMock()
    session.post.side_effect = [_login_response()]
    session.get.side_effect = [
        _csrf_response(),
        _FakeResponse({"result": [{"id": 7}]}),
        _FakeResponse({"result": [{"id": 1}]}),
        _FakeResponse({"result": {"columns": [{"column_name": "Amount"}], "metrics": []}}),
    ]
    session.put.side_effect = [_FakeResponse({}), _FakeResponse({"result": {}})]
    resource = _resource_with_session(session)

    resource.sync_dataset("Cube", "public", "orders_overview", [{"name": "amount"}], [])

    update_call = session.put.call_args_list[1]
    assert update_call.kwargs["json"]["columns"][0]["groupby"] is True


def test_refresh_polls_until_columns_are_populated(monkeypatch):
    monkeypatch.setattr("dagster_cube_dbt.superset_resource.time.sleep", lambda _: None)
    session = MagicMock()
    session.post.side_effect = [_login_response()]
    session.get.side_effect = [
        _csrf_response(),
        _FakeResponse({"result": [{"id": 7}]}),
        _FakeResponse({"result": [{"id": 1}]}),
        _FakeResponse({"result": {"columns": [], "metrics": []}}),  # not populated yet
        _FakeResponse({"result": {"columns": [{"column_name": "amount"}], "metrics": []}}),
    ]
    session.put.side_effect = [_FakeResponse({}), _FakeResponse({"result": {}})]
    resource = _resource_with_session(session)

    resource.sync_dataset("Cube", "public", "orders_overview", [], [])

    # two GETs for the dataset detail (one empty, one populated) beyond csrf/database/find
    assert session.get.call_count == 5


def test_verbose_name_prefers_title_over_generated_name():
    assert _verbose_name({"name": "total_amount", "title": "Total $"}) == "Total $"


def test_verbose_name_falls_back_to_title_cased_name():
    assert _verbose_name({"name": "total_amount"}) == "Total Amount"
