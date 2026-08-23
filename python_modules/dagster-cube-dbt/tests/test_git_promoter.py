"""Tests for `GitCubeFilePromoter`, exercised against real local bare git repos. A local-path
clone bypasses the network/SSH transport, but exercises the exact same
clone/write/add/commit/push code paths a real SSH or HTTPS remote would -- only the transport
and credential-injection mechanics differ, which is what the auth-mode-specific tests target
directly instead.
"""

import base64
import shutil
import subprocess
from pathlib import Path

import dagster as dg
import pytest

from dagster_cube_dbt.git_promoter import GitCubeFilePromoter


def _init_bare_repo(path: Path) -> None:
    subprocess.run(
        ["git", "init", "--bare", "--initial-branch=main", str(path)],
        check=True,
        capture_output=True,
    )


def _repo_files(bare_repo: Path, branch: str = "main") -> set[str]:
    result = subprocess.run(
        ["git", "--git-dir", str(bare_repo), "ls-tree", "-r", branch, "--name-only"],
        capture_output=True,
        text=True,
    )
    return set(result.stdout.split()) if result.returncode == 0 else set()


def _repo_file_content(bare_repo: Path, path: str, branch: str = "main") -> str:
    result = subprocess.run(
        ["git", "--git-dir", str(bare_repo), "show", f"{branch}:{path}"],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout


def _commit_count(bare_repo: Path, branch: str = "main") -> int:
    result = subprocess.run(
        ["git", "--git-dir", str(bare_repo), "log", "--oneline", branch],
        capture_output=True,
        text=True,
    )
    return len(result.stdout.splitlines()) if result.returncode == 0 else 0


@pytest.fixture
def bare_repo(tmp_path) -> Path:
    repo = tmp_path / "bare_remote.git"
    _init_bare_repo(repo)
    return repo


@pytest.fixture
def cubes_src(tmp_path) -> Path:
    src = tmp_path / "cubes_src"
    src.mkdir()
    (src / "journey_samples.yaml").write_text(
        "cubes:\n  - name: journey_samples\n    sql_table: db.schema.journey_samples\n"
    )
    return src


@pytest.fixture
def views_src(tmp_path) -> Path:
    src = tmp_path / "views_src"
    src.mkdir()
    return src


def test_first_promote_pushes_into_a_brand_new_empty_repo(bare_repo, cubes_src, views_src):
    """A completely empty repo has no branches at all yet -- `git clone --branch main` fails
    outright against one, which is why `_clone` clones unpinned first and creates the branch
    locally when it isn't found on the remote.
    """
    promoter = GitCubeFilePromoter(repo_url=str(bare_repo), branch="main", ssh_private_key="unused")

    promoter.promote(dg.build_asset_context(), cubes_src, views_src)

    assert _repo_files(bare_repo) == {"model/cubes/journey_samples.yaml"}
    assert _commit_count(bare_repo) == 1


def test_second_promote_with_no_changes_is_a_noop(bare_repo, cubes_src, views_src):
    promoter = GitCubeFilePromoter(repo_url=str(bare_repo), branch="main", ssh_private_key="unused")

    promoter.promote(dg.build_asset_context(), cubes_src, views_src)
    promoter.promote(dg.build_asset_context(), cubes_src, views_src)

    assert _commit_count(bare_repo) == 1


def test_promote_with_changed_content_pushes_a_new_commit(bare_repo, cubes_src, views_src):
    promoter = GitCubeFilePromoter(repo_url=str(bare_repo), branch="main", ssh_private_key="unused")
    promoter.promote(dg.build_asset_context(), cubes_src, views_src)

    (cubes_src / "journey_samples.yaml").write_text(
        "cubes:\n  - name: journey_samples\n    sql_table: db.schema.journey_samples_v2\n"
    )
    promoter.promote(dg.build_asset_context(), cubes_src, views_src)

    assert _commit_count(bare_repo) == 2
    assert "journey_samples_v2" in _repo_file_content(bare_repo, "model/cubes/journey_samples.yaml")


def test_promote_to_a_non_default_branch_that_already_has_commits(bare_repo, cubes_src, views_src):
    """Regression test: `bare_repo`'s default branch is "main" (`--initial-branch=main`). Seed a
    *different* branch, "dev", with a commit of its own -- the way a human might have already
    set one up -- then promote with `branch="dev"`. `--depth 1` clone implies `--single-branch`,
    so an *unpinned* clone only ever fetches the default branch ("main"); if `_clone` didn't pin
    `--branch` on the initial attempt, it would never see "dev"'s real tip, silently create an
    unrelated local "dev" off of "main" instead, and get the push rejected as non-fast-forward
    -- exactly the bug this guards against.
    """
    seed = bare_repo.parent / "seed"
    subprocess.run(["git", "clone", str(bare_repo), str(seed)], check=True, capture_output=True)
    (seed / "README.md").write_text("seeded by hand\n")
    subprocess.run(["git", "-C", str(seed), "checkout", "-b", "dev"], check=True, capture_output=True)
    subprocess.run(["git", "-C", str(seed), "add", "-A"], check=True, capture_output=True)
    subprocess.run(
        [
            "git", "-C", str(seed), "-c", "user.name=seed", "-c", "user.email=seed@localhost",
            "commit", "-m", "seed dev",
        ],
        check=True,
        capture_output=True,
    )
    subprocess.run(["git", "-C", str(seed), "push", "origin", "dev"], check=True, capture_output=True)

    promoter = GitCubeFilePromoter(repo_url=str(bare_repo), branch="dev", ssh_private_key="unused")
    promoter.promote(dg.build_asset_context(), cubes_src, views_src)

    assert _repo_files(bare_repo, branch="dev") == {"README.md", "model/cubes/journey_samples.yaml"}
    assert _commit_count(bare_repo, branch="dev") == 2


def test_http_auth_mode_pushes_successfully_too(bare_repo, cubes_src, views_src):
    promoter = GitCubeFilePromoter(
        repo_url=str(bare_repo), branch="main", http_username="user", http_token="token-value"
    )

    promoter.promote(dg.build_asset_context(), cubes_src, views_src)

    assert _repo_files(bare_repo) == {"model/cubes/journey_samples.yaml"}


def test_http_token_never_leaks_into_a_raised_error(cubes_src, views_src):
    """The HTTP token (and its base64-encoded header form -- an encoding, not encryption, so
    just as sensitive) must never appear in a raised error message, since that could end up in
    Dagster run logs. Forces a real failure (an unreachable host) rather than asserting on the
    redaction logic in isolation, so this catches a redaction gap even if the error message
    shape changes later.
    """
    promoter = GitCubeFilePromoter(
        repo_url="https://example.invalid/does/not/exist.git",
        branch="main",
        http_username="user",
        http_token="super-secret-token-value",
    )

    with pytest.raises(RuntimeError) as excinfo:
        promoter.promote(dg.build_asset_context(), cubes_src, views_src)

    message = str(excinfo.value)
    assert "super-secret-token-value" not in message
    assert base64.b64encode(b"user:super-secret-token-value").decode() not in message


def test_ssh_and_http_credentials_together_raises_at_construction():
    with pytest.raises(Exception, match="not both"):
        GitCubeFilePromoter(repo_url="x", ssh_private_key="k", http_username="u", http_token="t")


def test_neither_ssh_nor_http_credentials_raises_at_construction():
    with pytest.raises(Exception, match="GitCubeFilePromoter"):
        GitCubeFilePromoter(repo_url="x")


def test_partial_http_credentials_raises_at_construction():
    with pytest.raises(Exception, match="GitCubeFilePromoter"):
        GitCubeFilePromoter(repo_url="x", http_username="u")


def test_ssh_key_file_is_written_without_crlf_translation(tmp_path):
    """`Path.write_text`'s default newline handling translates every `\\n` to the platform line
    separator -- on Windows, that's `\\r\\n`. A CRLF-terminated PEM file fails to parse in
    OpenSSL, which ssh surfaces as an opaque "error in libcrypto" rather than anything
    mentioning line endings. Regression test for exactly that: the key must hit disk with its
    original `\\n`s untouched, on every platform.
    """
    key_content = "-----BEGIN OPENSSH PRIVATE KEY-----\nline-one\nline-two\n-----END OPENSSH PRIVATE KEY-----\n"
    promoter = GitCubeFilePromoter(repo_url="unused", ssh_private_key=key_content)

    promoter._build_auth(tmp_path)

    written = (tmp_path / "deploy_key").read_bytes()
    assert b"\r\n" not in written
    assert written == key_content.encode()


def test_missing_git_binary_raises_a_clear_error_before_touching_git(
    monkeypatch, cubes_src, views_src
):
    monkeypatch.setattr(shutil, "which", lambda name: None)
    promoter = GitCubeFilePromoter(repo_url="unused", ssh_private_key="k")

    with pytest.raises(RuntimeError, match="git.*binary"):
        promoter.promote(dg.build_asset_context(), cubes_src, views_src)
