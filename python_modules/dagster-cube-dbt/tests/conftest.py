"""Prepares the fixture dbt project's manifest.json once per test session.

`target/` is gitignored (it's build output), so a fresh clone/CI run has no manifest until
this runs `dbt parse` against the fixture project.
"""

import shutil
import subprocess
import sys
from pathlib import Path

import pytest

FIXTURE_DBT_PROJECT = Path(__file__).parent / "fixtures" / "dbt_project"


@pytest.fixture(scope="session", autouse=True)
def _parsed_fixture_dbt_manifest():
    dbt_executable = shutil.which("dbt") or str(Path(sys.executable).parent / "dbt.exe")
    manifest_path = FIXTURE_DBT_PROJECT / "target" / "manifest.json"

    result = subprocess.run(
        [dbt_executable, "parse", "--profiles-dir", str(FIXTURE_DBT_PROJECT)],
        cwd=FIXTURE_DBT_PROJECT,
        capture_output=True,
        text=True,
    )
    # `dbt.exe` can report a nonzero exit code on Windows from an unrelated duckdb/OpenSSL
    # DLL-teardown quirk even when parsing genuinely succeeded, so the exit code alone isn't
    # a reliable success signal here -- check for the manifest it should have written instead.
    if not manifest_path.exists():
        raise RuntimeError(
            "dbt parse did not produce a manifest for the fixture project.\n"
            f"returncode={result.returncode}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
