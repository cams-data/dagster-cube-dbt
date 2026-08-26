"""`SupersetResource`: syncs one Cube view's dimensions/measures into an Apache Superset
dataset via Superset's own REST API (login -> CSRF -> find/create dataset -> refresh -> update
columns/metrics).

Request/response shapes below (payload fields, filter operators, which fields must be stripped
before round-tripping a column/metric back to Superset) were verified against a real, working
reference implementation -- ponderedw/dbt-to-cube's `SupersetConnector`
(dbt-cube-sync/dbt_cube_sync/connectors/superset.py) -- not guessed from Superset's REST API
reference docs alone; see SUPERSET_SYNC_PLAN.md for the investigation. One deliberate deviation
from that reference: it sleeps a fixed 2 seconds after `PUT .../refresh` before assuming
columns are populated; this polls (bounded by `refresh_timeout_seconds`) instead, the same
"poll, don't guess a sleep" shape `dagster_cube_dbt.landing_check.wait_for_landing` already
uses for the Cube-side propagation problem.
"""

import json
import time
from collections.abc import Mapping, Sequence
from typing import Any

import dagster as dg
import requests
from pydantic import PrivateAttr

# Fields Superset computes/manages itself and returns on a GET, but that a PUT must not echo
# back verbatim (rejected or silently reset) -- confirmed against the reference implementation.
_READONLY_COLUMN_FIELDS = {"created_on", "changed_on", "type_generic", "uuid", "advanced_data_type"}
_READONLY_METRIC_FIELDS = {"created_on", "changed_on", "uuid"}


def _verbose_name(member: Mapping[str, Any]) -> str:
    title = member.get("title")
    if title:
        return str(title)
    return str(member["name"]).replace("_", " ").title()


class SupersetResource(dg.ConfigurableResource):
    """Owns the login/CSRF/session lifecycle for Superset's REST API and the find-or-create
    dataset flow. Authenticates once per resource instance (cached), not once per dataset --
    `sync_dataset` is called once per synced view, and re-logging-in for every one of
    potentially dozens of views would be wasteful.

    `password` is a plain `str` config field, the same pattern `CubeRestApiClient.api_token`
    uses -- bind it from wherever your project already manages secrets (e.g.
    `EnvVar("SUPERSET_PASSWORD")`), not a literal value in checked-in `defs.yaml`.

    Doesn't create the underlying Superset database *connection* (the one pointed at Cube's SQL
    API) -- `database_name` (passed to `sync_dataset`) must already exist in Superset, set up
    once by hand, the same way `CubeDbtProjectComponent` assumes a running Cube instance already
    exists rather than provisioning one.

    `verify_tls` defaults to `True` (verify the server's certificate, `requests`' own default and
    the same default `CubeRestApiClient.verify_tls` uses). Set it to `False` only for a
    deployment you can't otherwise reach with a valid certificate -- a self-hosted instance
    behind a self-signed or internal-CA cert, most often -- and treat it with the same caution
    you would `requests`' own `verify=False`: it disables certificate verification entirely for
    every request this resource makes, for every call in `sync_dataset`'s login/CSRF/find/
    create/refresh/update flow, not just one of them.
    """

    base_url: str
    username: str
    password: str
    verify_tls: bool = True
    refresh_timeout_seconds: float = 30.0
    refresh_poll_interval_seconds: float = 1.0

    _session: requests.Session = PrivateAttr(default_factory=requests.Session)
    _authenticated: bool = PrivateAttr(default=False)

    def _url(self, path: str) -> str:
        return f"{self.base_url.rstrip('/')}{path}"

    def _ensure_authenticated(self) -> None:
        # Set on the session (applies to every request made through it) rather than passed
        # per-call -- this resource's login/CSRF/find/create/refresh/update flow is several
        # requests deep, and a per-call verify= would need repeating (and staying in sync)
        # everywhere `self._session` is used, not just here.
        self._session.verify = self.verify_tls
        if self._authenticated:
            return
        login_response = self._session.post(
            self._url("/api/v1/security/login"),
            json={
                "username": self.username,
                "password": self.password,
                "provider": "db",
                "refresh": True,
            },
        )
        login_response.raise_for_status()
        access_token = login_response.json()["access_token"]
        self._session.headers["Authorization"] = f"Bearer {access_token}"

        csrf_response = self._session.get(self._url("/api/v1/security/csrf_token/"))
        csrf_response.raise_for_status()
        self._session.headers["X-CSRFToken"] = csrf_response.json()["result"]
        self._authenticated = True

    def _find_database_id(self, database_name: str) -> int:
        response = self._session.get(
            self._url("/api/v1/database/"),
            params={
                "q": json.dumps({"filters": [{"col": "database_name", "opr": "eq", "value": database_name}]})
            },
        )
        response.raise_for_status()
        results = response.json()["result"]
        if not results:
            raise dg.Failure(
                f"No Superset database connection named {database_name!r} was found. Create "
                "it in Superset first (pointed at Cube's SQL API) -- this resource doesn't "
                "provision one."
            )
        return results[0]["id"]

    def _find_dataset_id(self, database_id: int, schema: str, table_name: str) -> int | None:
        response = self._session.get(
            self._url("/api/v1/dataset/"),
            params={
                "q": json.dumps(
                    {
                        "filters": [
                            {"col": "table_name", "opr": "eq", "value": table_name},
                            {"col": "schema", "opr": "eq", "value": schema},
                            {"col": "database", "opr": "rel_o_m", "value": database_id},
                        ]
                    }
                )
            },
        )
        response.raise_for_status()
        results = response.json()["result"]
        return results[0]["id"] if results else None

    def _create_dataset(self, database_id: int, schema: str, table_name: str) -> int:
        response = self._session.post(
            self._url("/api/v1/dataset/"),
            json={
                "database": database_id,
                "schema": schema,
                "table_name": table_name,
                "normalize_columns": False,
                "always_filter_main_dttm": False,
            },
        )
        response.raise_for_status()
        return response.json()["id"]

    def _refresh_and_fetch_columns(self, dataset_id: int) -> dict[str, Any]:
        refresh_response = self._session.put(self._url(f"/api/v1/dataset/{dataset_id}/refresh"))
        refresh_response.raise_for_status()

        deadline = time.monotonic() + self.refresh_timeout_seconds
        while True:
            get_response = self._session.get(self._url(f"/api/v1/dataset/{dataset_id}"))
            get_response.raise_for_status()
            dataset = get_response.json()["result"]
            if dataset.get("columns") or time.monotonic() >= deadline:
                return dataset
            time.sleep(self.refresh_poll_interval_seconds)

    def sync_dataset(
        self,
        database_name: str,
        schema: str,
        table_name: str,
        dimensions: Sequence[Mapping[str, Any]],
        measures: Sequence[Mapping[str, Any]],
    ) -> int:
        """Finds (or creates) the Superset dataset for `(database_name, schema, table_name)`,
        refreshes its column introspection, then updates its columns' `verbose_name`/
        `description`/`groupby`/`filterable` from `dimensions` (matched to Superset's
        introspected columns by name, case-insensitively) and adds/updates one metric per
        measure in `measures`, expressed as Cube SQL API's own aggregation-pushdown syntax
        (`MEASURE(<table_name>.<measure_name>)`) so Cube -- not Superset -- performs the
        aggregation. `dimensions`/`measures` are plain dicts in this project's own generated
        cube/view shape (`name`, optional `title`/`description`, `type` for dimensions).

        Returns the Superset dataset id.
        """
        self._ensure_authenticated()
        database_id = self._find_database_id(database_name)
        dataset_id = self._find_dataset_id(database_id, schema, table_name)
        if dataset_id is None:
            dataset_id = self._create_dataset(database_id, schema, table_name)

        dataset = self._refresh_and_fetch_columns(dataset_id)

        dimensions_by_name = {str(dimension["name"]).lower(): dimension for dimension in dimensions}
        updated_columns = []
        for column in dataset.get("columns", []):
            clean_column = {k: v for k, v in column.items() if k not in _READONLY_COLUMN_FIELDS}
            dimension = dimensions_by_name.get(str(column["column_name"]).lower())
            if dimension is not None:
                clean_column.update(
                    verbose_name=_verbose_name(dimension),
                    description=dimension.get("description") or "",
                    is_dttm=dimension.get("type") == "time",
                    groupby=True,
                    filterable=True,
                    is_active=True,
                )
            updated_columns.append(clean_column)

        metrics_by_name = {
            metric["metric_name"]: {k: v for k, v in metric.items() if k not in _READONLY_METRIC_FIELDS}
            for metric in dataset.get("metrics", [])
        }
        for measure in measures:
            name = measure["name"]
            metrics_by_name[name] = {
                **metrics_by_name.get(name, {}),
                "metric_name": name,
                "verbose_name": _verbose_name(measure),
                "expression": f"MEASURE({table_name}.{name})",
                "description": measure.get("description") or "",
                "metric_type": "simple",
            }

        update_response = self._session.put(
            self._url(f"/api/v1/dataset/{dataset_id}"),
            json={"columns": updated_columns, "metrics": list(metrics_by_name.values())},
        )
        update_response.raise_for_status()
        return dataset_id
