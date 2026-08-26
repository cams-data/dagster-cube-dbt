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


class CubeRestApiClient(dg.ConfigurableResource):
    """Talks to a real Cube deployment's REST API -- the one way this library reads back Cube's
    own live schema. Not an abstract base with pluggable implementations (unlike
    `CubeFilePromoter`, which genuinely has several -- local file, git, ...) -- there's no
    second real way to ask a Cube deployment what it currently has loaded, and Python's own
    duck typing means a test double for `fetch_meta` never needed a formal base class to
    substitute for this at all (see `test_landing_check.py`).

    `api_token` is sent verbatim in the `Authorization` header -- Cube's REST API takes a bare
    token there, not a `Bearer <token>` scheme (confirmed against Cube's own auth docs),
    typically a JWT signed with your deployment's `CUBEJS_API_SECRET`. Generating/rotating that
    token, and deciding what security-context claims it needs for your deployment's access
    rules, is left entirely to the caller -- bind this resource with a token from wherever your
    project already manages secrets (e.g. `EnvVar("CUBE_API_TOKEN")`), the same pattern as any
    other credentialed Dagster resource.

    `verify_tls` defaults to `True` (verify the server's certificate, `requests`' own default).
    Set it to `False` only for a deployment you can't otherwise reach with a valid certificate
    (a self-hosted instance behind a self-signed or internal-CA cert, most often) -- this
    disables certificate verification entirely for every request this resource makes, the same
    way `requests.get(..., verify=False)` does, so treat it the same as you would that call:
    fine for a trusted internal network, not something to reach for to route around an
    otherwise-fixable certificate problem.
    """

    api_url: str
    api_token: str
    verify_tls: bool = True

    def fetch_meta(self) -> dict[str, Any]:
        """Returns Cube's full `/v1/meta` response, deserialized from JSON: a dict with a
        `"cubes"` key (Cube's own REST API groups cubes *and* views under this one key,
        distinguished by a `"type"` field) holding a list of `{"name": ..., "meta": {...},
        ...}` entries. Raises `requests.HTTPError` on a non-2xx response -- `wait_for_landing`
        is what decides whether that's worth retrying.
        """
        response = requests.get(
            f"{self.api_url.rstrip('/')}/meta",
            headers={"Authorization": self.api_token},
            timeout=30,
            verify=self.verify_tls,
        )
        response.raise_for_status()
        return response.json()


def wait_for_landing(
    client: CubeRestApiClient,
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

    A `/meta` request failing outright (not just returning stale content) is itself expected
    during a promotion: a `git-sync`-style sidecar loading a bad file makes Cube start serving
    500s for its *own* schema until a fix propagates, and the window right after that fix lands
    is exactly when this poll is running. A 5xx response, or the request failing to complete at
    all (connection refused, timeout -- Cube mid-restart), is treated the same as "not landed
    yet" and retried; a 4xx response (bad `api_url`, an invalid/expired `api_token`) is a
    permanent misconfiguration that more polling won't fix, so it's raised immediately rather
    than only surfacing once `timeout_seconds` runs out.
    """
    pending = dict(expected_code_versions)
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None

    while True:
        try:
            response = client.fetch_meta()
        except requests.HTTPError as error:
            status_code = error.response.status_code if error.response is not None else None
            if status_code is None or not (500 <= status_code < 600):
                raise
            last_error = error
        except requests.RequestException as error:
            # Connection errors, timeouts, etc. -- the request never completed at all, as
            # plausibly transient as a 5xx (Cube mid-restart, a brief network blip), not a
            # permanent misconfiguration either.
            last_error = error
        else:
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
            last_error = None  # a successful-but-not-yet-matching poll supersedes any earlier error

        if time.monotonic() >= deadline:
            message = (
                "Timed out waiting for the following cube/view(s) to land in Cube (promoted "
                f"content not yet visible via the Cube REST API): {sorted(pending)}"
            )
            if last_error is not None:
                message += f" -- last error while polling: {last_error!r}"
            raise dg.Failure(message)
        time.sleep(poll_interval_seconds)
