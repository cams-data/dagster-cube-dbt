"""An optional post-promotion step: after a `CubeFilePromoter` delivers freshly generated
cube/view YAML, poll Cube's own REST API (`GET {api_url}/meta`) until the promoted content is
actually visible there, before the corresponding Dagster asset is considered materialized.

Without this, "materialized" only means "handed to the promoter" -- whether (and when) a
running Cube instance actually picks up a freshly written/uploaded file (hot-reloads it,
propagates it through Cube Cloud, etc.) is invisible to Dagster, and downstream consumers (a
dashboard querying the view immediately after a run finishes, a future Superset dataset sync)
have no way to tell "materialized" from "materialized, but not actually queryable yet."

The `code_version` Dagster already computes for a cube/view (`_code_version` in
`components/cube_dbt_project/component.py`, and what shows up as that asset's own
`AssetSpec.code_version`) is injected into the promoted entity's own `meta` block before
promotion (`with_landing_check_meta`), namespaced under `meta.dagster_cube_dbt.code_version` so
it's merged alongside -- never overwriting -- anything a user already set via `meta.cube.meta`.
Cube's REST API echoes a cube/view's `meta` verbatim in its `/v1/meta` response (confirmed
against Cube's own REST API reference docs), so polling that response for the same value we
just wrote is a direct way to confirm this exact generated content, not just *some* content,
has landed.
"""

import time
from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import Any

import dagster as dg
import requests

LANDING_CHECK_META_KEY = "dagster_cube_dbt"


def with_landing_check_meta(entity: Mapping[str, Any], code_version: str) -> dict[str, Any]:
    """Returns a copy of `entity` (a generated cube or view dict) with its expected
    `code_version` stamped into `meta.dagster_cube_dbt.code_version`, merged into whatever
    `meta` the entity already carries (e.g. from `meta.cube.meta` on the dbt model/column that
    generated it) rather than replacing it.
    """
    entity = dict(entity)
    meta = dict(entity.get("meta") or {})
    meta[LANDING_CHECK_META_KEY] = {
        **meta.get(LANDING_CHECK_META_KEY, {}),
        "code_version": code_version,
    }
    entity["meta"] = meta
    return entity


class CubeApiClient(dg.ConfigurableResource, ABC):
    """Base resource for reading back Cube's own live schema. `CubeRestApiClient` is the
    concrete HTTP implementation talking to a real Cube deployment; a test double can subclass
    this directly (see `fetch_meta`'s docstring for the expected response shape) to exercise
    the landing-check polling logic without a real Cube instance.
    """

    @abstractmethod
    def fetch_meta(self) -> dict[str, Any]:
        """Returns Cube's full `/v1/meta` response, deserialized from JSON: a dict with a
        `"cubes"` key (Cube's own REST API groups cubes *and* views under this one key,
        distinguished by a `"type"` field) holding a list of `{"name": ..., "meta": {...},
        ...}` entries.
        """
        ...


class CubeRestApiClient(CubeApiClient):
    """Talks to a real Cube deployment's REST API.

    `api_token` is sent verbatim in the `Authorization` header -- Cube's REST API takes a bare
    token there, not a `Bearer <token>` scheme (confirmed against Cube's own auth docs),
    typically a JWT signed with your deployment's `CUBEJS_API_SECRET`. Generating/rotating that
    token, and deciding what security-context claims it needs for your deployment's access
    rules, is left entirely to the caller -- bind this resource with a token from wherever your
    project already manages secrets (e.g. `EnvVar("CUBE_API_TOKEN")`), the same pattern as any
    other credentialed Dagster resource.
    """

    api_url: str
    api_token: str

    def fetch_meta(self) -> dict[str, Any]:
        response = requests.get(
            f"{self.api_url.rstrip('/')}/meta",
            headers={"Authorization": self.api_token},
            timeout=30,
        )
        response.raise_for_status()
        return response.json()


def wait_for_landing(
    client: CubeApiClient,
    expected_code_versions: Mapping[str, str],
    timeout_seconds: float,
    poll_interval_seconds: float,
) -> None:
    """Polls `client.fetch_meta()` until every name in `expected_code_versions` shows the
    matching `meta.dagster_cube_dbt.code_version` in Cube's response, or `timeout_seconds`
    elapses. Raises `dg.Failure` on timeout, naming whichever entities never landed -- this is
    called from inside the promotion op, before any `MaterializeResult` is yielded, so a
    timeout fails the run outright rather than reporting a false materialization (the same
    contract `CubeFilePromoter.promote` documents). Since the underlying asset's `code_version`
    is left unchanged by a failed run, the next automation evaluation just retries the whole
    promote-then-poll cycle.
    """
    pending = dict(expected_code_versions)
    deadline = time.monotonic() + timeout_seconds

    while True:
        response = client.fetch_meta()
        landed = {
            entry["name"]: (entry.get("meta") or {}).get(LANDING_CHECK_META_KEY, {}).get("code_version")
            for entry in response.get("cubes", [])
        }
        pending = {
            name: code_version
            for name, code_version in pending.items()
            if landed.get(name) != code_version
        }
        if not pending:
            return
        if time.monotonic() >= deadline:
            raise dg.Failure(
                "Timed out waiting for the following cube/view(s) to land in Cube (promoted "
                f"content not yet visible via the Cube REST API): {sorted(pending)}"
            )
        time.sleep(poll_interval_seconds)
