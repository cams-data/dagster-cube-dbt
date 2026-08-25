"""Unit tests for `dagster_cube_dbt.landing_check` in isolation from the component/asset-graph
machinery -- `test_component_integration.py` covers the end-to-end op wiring (via a fake
`CubeApiClient`); these focus on the pure helpers and the real HTTP client's request/response
shape (mocked, since there's no real Cube instance in CI).
"""

from unittest.mock import MagicMock, patch

import dagster as dg
import pytest

from dagster_cube_dbt.landing_check import (
    LANDING_CHECK_META_KEY,
    CubeApiClient,
    CubeRestApiClient,
    wait_for_landing,
    with_landing_check_meta,
)


def test_with_landing_check_meta_merges_without_overwriting_existing_meta():
    """Injecting the code_version marker must not clobber `meta` a user already set via
    `meta.cube.meta` (promoted straight through from dbt), and must not clobber an unrelated
    key already nested under our own `dagster_cube_dbt` namespace.
    """
    cube = {
        "name": "orders",
        "meta": {
            "someKey": "someValue",
            LANDING_CHECK_META_KEY: {"unrelated_field": "keep_me"},
        },
    }

    stamped = with_landing_check_meta(cube, "abc123")

    assert stamped["meta"]["someKey"] == "someValue"
    assert stamped["meta"][LANDING_CHECK_META_KEY]["unrelated_field"] == "keep_me"
    assert stamped["meta"][LANDING_CHECK_META_KEY]["code_version"] == "abc123"
    # the original dict is untouched -- callers (the promotion op) build one promoted list from
    # the same underlying cube dicts used to build AssetSpecs, and must not mutate those.
    assert "code_version" not in cube.get("meta", {}).get(LANDING_CHECK_META_KEY, {})


def test_with_landing_check_meta_on_entity_with_no_existing_meta():
    stamped = with_landing_check_meta({"name": "orders"}, "abc123")
    assert stamped["meta"] == {LANDING_CHECK_META_KEY: {"code_version": "abc123"}}


def test_cube_rest_api_client_sends_bare_token_not_bearer_scheme():
    """Cube's REST API takes the token verbatim in `Authorization` -- no `Bearer ` prefix --
    confirmed against Cube's own auth docs. Getting this wrong means every request 401s.
    """
    client = CubeRestApiClient(api_url="https://example.cubecloudapp.dev/cubejs-api/v1", api_token="a.jwt.token")

    mock_response = MagicMock()
    mock_response.json.return_value = {"cubes": []}
    with patch("dagster_cube_dbt.landing_check.requests.get", return_value=mock_response) as mock_get:
        result = client.fetch_meta()

    assert result == {"cubes": []}
    mock_get.assert_called_once_with(
        "https://example.cubecloudapp.dev/cubejs-api/v1/meta",
        headers={"Authorization": "a.jwt.token"},
        timeout=30,
        verify=True,
    )
    mock_response.raise_for_status.assert_called_once()


def test_cube_rest_api_client_strips_trailing_slash_from_api_url():
    client = CubeRestApiClient(api_url="https://example.cubecloudapp.dev/cubejs-api/v1/", api_token="tok")

    mock_response = MagicMock()
    mock_response.json.return_value = {"cubes": []}
    with patch("dagster_cube_dbt.landing_check.requests.get", return_value=mock_response) as mock_get:
        client.fetch_meta()

    assert mock_get.call_args.args[0] == "https://example.cubecloudapp.dev/cubejs-api/v1/meta"


def test_cube_rest_api_client_verify_tls_defaults_to_true():
    client = CubeRestApiClient(api_url="https://example.cubecloudapp.dev/cubejs-api/v1", api_token="tok")

    mock_response = MagicMock()
    mock_response.json.return_value = {"cubes": []}
    with patch("dagster_cube_dbt.landing_check.requests.get", return_value=mock_response) as mock_get:
        client.fetch_meta()

    assert mock_get.call_args.kwargs["verify"] is True


def test_cube_rest_api_client_verify_tls_false_disables_certificate_verification():
    client = CubeRestApiClient(
        api_url="https://example.cubecloudapp.dev/cubejs-api/v1", api_token="tok", verify_tls=False
    )

    mock_response = MagicMock()
    mock_response.json.return_value = {"cubes": []}
    with patch("dagster_cube_dbt.landing_check.requests.get", return_value=mock_response) as mock_get:
        client.fetch_meta()

    assert mock_get.call_args.kwargs["verify"] is False


def _client_with_responses(responses: list[dict]) -> CubeApiClient:
    calls = {"count": 0}

    class _Client(CubeApiClient):
        def fetch_meta(self) -> dict:
            index = min(calls["count"], len(responses) - 1)
            calls["count"] += 1
            return responses[index]

    return _Client()


def test_wait_for_landing_returns_as_soon_as_every_expected_entity_matches():
    client = _client_with_responses(
        [
            {
                "cubes": [
                    {"name": "orders", "meta": {LANDING_CHECK_META_KEY: {"code_version": "v1"}}},
                    {"name": "customers", "meta": {LANDING_CHECK_META_KEY: {"code_version": "old"}}},
                ]
            },
            {
                "cubes": [
                    {"name": "orders", "meta": {LANDING_CHECK_META_KEY: {"code_version": "v1"}}},
                    {"name": "customers", "meta": {LANDING_CHECK_META_KEY: {"code_version": "v2"}}},
                ]
            },
        ]
    )

    # Should not raise -- both entities eventually match, well within the timeout.
    wait_for_landing(
        client,
        {"orders": "v1", "customers": "v2"},
        timeout_seconds=5.0,
        poll_interval_seconds=0.01,
    )


def test_wait_for_landing_times_out_and_names_only_the_still_pending_entities():
    client = _client_with_responses(
        [{"cubes": [{"name": "orders", "meta": {LANDING_CHECK_META_KEY: {"code_version": "v1"}}}]}]
    )

    with pytest.raises(dg.Failure) as exc_info:
        wait_for_landing(
            client,
            {"orders": "v1", "customers": "v2"},
            timeout_seconds=0.05,
            poll_interval_seconds=0.01,
        )

    message = str(exc_info.value)
    assert "customers" in message
    assert "orders" not in message  # already landed -- shouldn't be reported as pending
