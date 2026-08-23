"""Which dbt engine (dbt-core vs. dbt Fusion) the test suite is currently running against.

dbt Fusion can't be installed alongside dbt-core/dbt-duckdb in the same environment -- it's
published as a distribution literally named `dbt` (a pre-release), which collides with
dbt-core's own console script. So there is no in-process way to "switch engines"; instead,
whichever engine is actually installed in the active venv is selected via the
`DAGSTER_CUBE_DBT_TEST_DBT_TARGET` environment variable, set per Hatch matrix environment
(see `pyproject.toml`), naming the `profiles.yml` target to use ("dev" for dbt-core's
`:memory:` DuckDB target, "fusion" for Fusion's on-disk one -- see profiles.yml for why).
"""

import os

DBT_TARGET = os.environ.get("DAGSTER_CUBE_DBT_TEST_DBT_TARGET", "dev")
