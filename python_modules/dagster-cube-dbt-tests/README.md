# dagster_cube_dbt_tests

A small, real dbt + Dagster project used to exercise [`dagster-cube-dbt`](../dagster-cube-dbt)
end to end via the actual `dg` CLI, rather than just the library's own test suite.

- `dbt_project/` — a minimal DuckDB dbt project: `journey_samples` and three related
  dimension models under `models/marts/`, plus an `models/intermediate/` model that's
  deliberately excluded from cube generation.
- `src/dagster_cube_dbt_tests/defs/dbt_cubes/defs.yaml` — the `CubeDbtProjectComponent`
  definition, scoped to the `models/marts` directory via `cube_select: {paths: ["marts"]}`
  (manifest paths are relative to dbt's `model-paths` root, so no `models/` prefix — this
  tripped us up once, see `DECISIONS.md` at the repo root).
- `src/dagster_cube_dbt_tests/defs/dbt_cubes/patches/` — merge-patch YAML files
  demonstrating dimension removal, adding measures/joins, a wholly new hand-written cube
  with no backing dbt model, and a `views:` composition.
- `cube_project/model/{cubes,views}/` — where the merged, generated output lands (created by
  `dg utils refresh-defs-state`; gitignored, it's build output).

> **Note on Python interpreters (Windows)**: `.python-version` here is pinned to a
> system-installed patch version (`3.12.7`) rather than letting `uv` download its own. On at
> least one Windows machine, `uv`'s managed python-build-standalone interpreters (any
> version) crash native code that touches OpenSSL — including dbt-core's own CLI (`dbt
> --version` fails with `OPENSSL_Uplink: no OPENSSL_Applink`, no Python traceback) — while an
> ordinary python.org-installed interpreter works fine. `.python-version` alone is normally
> enough to steer `uv` at a working interpreter; add `[tool.uv] python-preference = "system"`
> to `pyproject.toml` if a `uv sync` ever silently reaches for a managed Python again despite
> the pin (e.g. after the pin's exact patch version stops being installed). If
> `dg utils refresh-defs-state` fails with a `dbt --version` error, check which interpreter
> `uv sync` actually used (`.venv/Scripts/python.exe -c "import sys; print(sys.executable)"`).
> See `DECISIONS.md` at the repo root for the full diagnosis.

## Getting started

### Installing dependencies

**Option 1: uv**

Ensure [`uv`](https://docs.astral.sh/uv/) is installed following their [official documentation](https://docs.astral.sh/uv/getting-started/installation/).

Create a virtual environment, and install the required dependencies using _sync_:

```bash
uv sync
```

Then, activate the virtual environment:

| OS | Command |
| --- | --- |
| MacOS | ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |

**Option 2: pip**

Install the python dependencies with [pip](https://pypi.org/project/pip/):

```bash
python3 -m venv .venv
```

Then activate the virtual environment:

| OS | Command |
| --- | --- |
| MacOS | ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |

Install the required dependencies:

```bash
pip install -e ".[dev]"
```

### Running Dagster

Generate the Cube YAML files (required before the cube assets can load; see the library
README's "Generating cube files" section):

```bash
dg utils refresh-defs-state
```

Then start the Dagster UI web server:

```bash
dg dev
```

Open http://localhost:3000 in your browser to see the project.

## Learn more

To learn more about this template and Dagster in general:

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster University](https://courses.dagster.io/)
- [Dagster Slack Community](https://dagster.io/slack)
