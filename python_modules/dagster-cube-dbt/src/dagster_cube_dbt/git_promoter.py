"""`GitCubeFilePromoter`: a `CubeFilePromoter` that pushes generated cube/view YAML to a git
repository -- either:

  (a) an arbitrary repo (GitHub, GitLab, ...) that something else -- e.g. a
      manually-configured `kubernetes/git-sync` sidecar -- polls and syncs
      into a shared volume for a self-hosted Cube Core to read its schema
      from. Authenticate with a dedicated **SSH deploy key** (`ssh_private_key`).

  (b) Cube Cloud's own git remote directly, using its "Deploy with Git" mode
      (Settings -> Build & Deploy -> Deploy with Git -> Generate Git
      credentials). Cube Cloud's own docs set this up with
      `git config credential.helper store` -- i.e. **HTTP username+token**
      auth, not SSH -- so authenticate with `http_username`/`http_token`
      instead. Note this pattern also expects the standard Cube project
      layout (`model/cubes/`, `model/views/`), which is why those are this
      resource's `cubes_subdir`/`views_subdir` defaults.

Set exactly one of the two credential pairs; the resource raises clearly at
construction time if you set both or neither.

Requires the `git` binary (and, for the SSH option, an `ssh` client) to be
present on `PATH` wherever this actually runs -- this library depends on
neither via pip (there's no PyPI package to depend on that would install a
working `git` CLI; a git binary is not a Python package), the same way a
Postgres resource requires `libpq` or a dbt component requires the `dbt` CLI.
Install it the way you'd install any other system tool your image needs, e.g.
`apt-get install -y git openssh-client` on a Debian-based image.

One-time setup for the SSH deploy-key path (skip if using the HTTP option):

1. Generate a dedicated key pair (don't reuse your personal SSH key) --
   run in a terminal, not committed anywhere:
       ssh-keygen -t ed25519 -f "~/.ssh/cube_deploy_key" -N ""
   This writes two files: `cube_deploy_key` (private) and
   `cube_deploy_key.pub` (public -- safe to paste elsewhere).
2. On the git host (GitHub: repo -> Settings -> Deploy keys -> Add deploy
   key; GitLab: repo -> Settings -> Repository -> Deploy keys), paste the
   *public* key's contents and grant it write access.
3. Point `repo_url` at the repo's SSH clone URL (`git@github.com:org/repo.git`,
   not the https one -- https URLs don't use SSH keys at all).

Either way, store the credential (the private key's full contents, or the
HTTP token) as a secret your deployment injects as an environment variable,
and bind it via `dg.EnvVar(...)`, never as a literal string -- the same way
you'd bind any other credential-bearing resource field.
"""

from __future__ import annotations

import base64
import dataclasses
import os
import shutil
import stat
import subprocess
import tempfile
from pathlib import Path

import dagster as dg
from pydantic import model_validator

from dagster_cube_dbt.output import read_entities, write_entities
from dagster_cube_dbt.resources import CubeFilePromoter


@dataclasses.dataclass
class _GitAuth:
    """Everything one `promote()` call's git invocations need to authenticate, plus which
    literal strings must be redacted from any error message built from them -- an HTTP token
    (and, since it's just as easily reversible, its base64-encoded header form) would
    otherwise appear verbatim in a raised exception, and from there potentially in Dagster run
    logs. SSH auth has nothing to redact: the private key is never passed as a CLI argument at
    all, only ever written to a file `-i` points at.
    """

    env: dict[str, str]
    extra_config_args: list[str]
    secrets_to_redact: list[str]


class GitCubeFilePromoter(CubeFilePromoter):
    """On each `promote()` call: shallow-clones `repo_url` fresh into a throwaway temporary
    directory (deleted when the call returns), replaces `cubes_subdir`/`views_subdir` with
    whatever was just generated, commits only if something actually changed, and pushes.

    A fresh clone per run rather than a persistent, reused local checkout -- deliberately.
    The only thing a persistent checkout would save is re-fetching commit history on every
    call, and this resource never needs history at all: it only ever adds one new commit on
    top of whatever the remote's current tip is, so `--depth 1` (fetch just the tip, not the
    history behind it) already gets nearly all of that saving without keeping any local state
    around between runs. In exchange, there's no shared local directory for two concurrent
    runs to corrupt -- each run's clone is fully private to it. This also isn't just a nicety:
    in a container-per-run deployment (e.g. Kubernetes), a "persistent" local checkout path
    likely wouldn't even survive between runs anyway, since each run can land in a different
    ephemeral pod filesystem -- so there'd be nothing to persist across other than the name.

    This does *not* mean two concurrent runs are fully safe together, though: if both happen
    to push at nearly the same moment, one push will still be rejected by the remote as a
    non-fast-forward (the second run's clone was taken from a tip the first run has since
    moved past) -- a real failure, just a local one, not silent data loss. That's exactly what
    `CubeDbtProjectComponent`'s `promotion_pool` exists for: setting a max concurrency of 1 for
    it in the Dagster UI serializes runs so this never actually happens, rather than relying on
    the rejection-and-presumably-retry path.

    Attributes:
        repo_url: The git remote to push to. SSH form (`git@host:org/repo.git`) when using
            `ssh_private_key`; https form (`https://host/org/repo.git`, with no credentials
            embedded in it -- those are supplied separately) when using `http_username`/
            `http_token`.
        branch: Branch to push generated schema to.
        ssh_private_key: The *contents* of the private half of an SSH deploy key authorized to
            push to `repo_url` (not a file path). For an arbitrary repo (e.g. one a git-sync
            sidecar polls) authenticated via a deploy key. See the module docstring for how to
            generate and register one. Mutually exclusive with `http_username`/`http_token`.
        http_username / http_token: HTTP Basic Auth credentials for `repo_url`. For pushing
            directly to Cube Cloud's own git remote (its "Generate Git credentials" flow
            issues exactly this pair). Mutually exclusive with `ssh_private_key`.
        cubes_subdir: Path *within the repo* that generated cube YAML files are written to.
            Defaults to `model/cubes`, Cube's own standard project layout.
        views_subdir: Path *within the repo* that generated view YAML files are written to.
            Defaults to `model/views`, Cube's own standard project layout.
        author_name / author_email: Git identity used for the commit.
        commit_message: Commit message used for every push.
    """

    repo_url: str
    branch: str = "main"
    ssh_private_key: str | None = None
    http_username: str | None = None
    http_token: str | None = None
    cubes_subdir: str = "model/cubes"
    views_subdir: str = "model/views"
    author_name: str = "dagster-cube-dbt"
    author_email: str = "dagster-cube-dbt@localhost"
    commit_message: str = "Update generated Cube schema"

    @model_validator(mode="after")
    def _exactly_one_auth_mode(self) -> "GitCubeFilePromoter":
        has_ssh = self.ssh_private_key is not None
        has_http = self.http_username is not None or self.http_token is not None
        if has_ssh and has_http:
            raise ValueError(
                "GitCubeFilePromoter: set `ssh_private_key` or `http_username`+`http_token`, "
                "not both."
            )
        if not has_ssh and not (self.http_username is not None and self.http_token is not None):
            raise ValueError(
                "GitCubeFilePromoter: set `ssh_private_key` (pushing to an arbitrary repo, "
                "e.g. one a git-sync sidecar polls) or both `http_username` and `http_token` "
                "(pushing directly to Cube Cloud's own git remote)."
            )
        return self

    def _build_auth(self, tmp_dir: Path) -> _GitAuth:
        if self.ssh_private_key is not None:
            key_path = tmp_dir / "deploy_key"
            # newline="\n": write the key's line endings exactly as given, with no platform
            # translation. On Windows, the default text-mode write turns every `\n` into
            # `\r\n`, and a CRLF-terminated PEM file fails to parse in OpenSSL -- ssh then
            # reports it as "error in libcrypto" and silently has no usable key at all.
            key_content = self.ssh_private_key
            if not key_content.endswith("\n"):
                key_content += "\n"
            key_path.write_text(key_content, newline="\n")
            # ssh refuses to use a private key file that's readable by anyone but its owner --
            # matches what `ssh-keygen` itself produces on disk (0600), which nothing else
            # here guarantees for a file we just wrote ourselves.
            os.chmod(key_path, stat.S_IRUSR | stat.S_IWUSR)
            # IdentitiesOnly=yes: only try this key, ignoring anything already loaded in an
            # SSH agent (avoids the wrong key being offered first and the host rejecting it).
            # StrictHostKeyChecking=accept-new: trust a host's key the first time it's seen,
            # recorded to ~/.ssh/known_hosts for future connections -- though in an ephemeral
            # container that starts fresh every run, there often *is* no persistent
            # known_hosts to accumulate into, so every run effectively trusts the host's key
            # on faith rather than ever really pinning it long-term. Still strictly better
            # than `StrictHostKeyChecking=no`: on a host where known_hosts *does* persist (a
            # long-lived local checkout, local dev), a previously-trusted host whose key
            # suddenly changes is still rejected, not silently accepted.
            ssh_command = (
                f'ssh -i "{key_path}" -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new'
            )
            return _GitAuth(
                env={**os.environ, "GIT_SSH_COMMAND": ssh_command},
                extra_config_args=[],
                secrets_to_redact=[],
            )

        # HTTP Basic Auth via a per-invocation extra header, not a credential embedded in the
        # URL -- git sometimes echoes a credential-embedded URL back verbatim in its own error
        # output (e.g. "fatal: unable to access '<url-with-credentials>'"), which this avoids
        # entirely; only the header value itself needs redacting from *our own* error
        # messages, not anything git might independently print.
        assert self.http_username is not None and self.http_token is not None  # enforced above
        basic_auth = base64.b64encode(f"{self.http_username}:{self.http_token}".encode()).decode()
        return _GitAuth(
            env=dict(os.environ),
            extra_config_args=["-c", f"http.extraHeader=Authorization: Basic {basic_auth}"],
            # the base64 form isn't meaningfully more secret than the raw token (base64 is an
            # encoding, not encryption) -- redact both, since either could appear in output.
            secrets_to_redact=[self.http_token, basic_auth],
        )

    def _run_git(self, *args: str, cwd: Path, auth: _GitAuth) -> subprocess.CompletedProcess:
        full_args = [*auth.extra_config_args, *args]
        result = subprocess.run(
            ["git", *full_args],
            cwd=cwd,
            env=auth.env,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            message = (
                f"git {' '.join(full_args)} failed (exit {result.returncode}) in {cwd}:\n"
                f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
            )
            for secret in auth.secrets_to_redact:
                if secret:
                    message = message.replace(secret, "***REDACTED***")
            raise RuntimeError(message)
        return result

    def _clone(self, repo_dir: Path, auth: _GitAuth) -> None:
        # Pin `--branch` on the first attempt. `--depth` implies `--single-branch`, so an
        # *unpinned* clone only ever fetches the remote's default branch (e.g. "main") --
        # if `self.branch` is something else (e.g. "dev") that already has commits of its
        # own, an unpinned clone would never even see them, silently look like the branch
        # doesn't exist, and then push a same-named-but-unrelated local branch that the
        # remote correctly rejects as non-fast-forward.
        result = subprocess.run(
            ["git", *auth.extra_config_args, "clone", "--depth", "1", "--branch", self.branch,
             self.repo_url, str(repo_dir)],
            cwd=repo_dir.parent,
            env=auth.env,
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            return
        # `self.branch` doesn't exist on the remote yet -- also the only way a repo with no
        # commits/branches at all (a brand new, still-empty repo) can be cloned, since that
        # fails identically against any pinned branch name. Clone unpinned (whatever the
        # default branch is, or nothing at all if the repo is empty) and create the branch
        # locally instead; the first push then creates it remotely.
        self._run_git(
            "clone", "--depth", "1", self.repo_url, str(repo_dir), cwd=repo_dir.parent, auth=auth
        )
        self._run_git("checkout", "-b", self.branch, cwd=repo_dir, auth=auth)

    def promote(self, context: dg.AssetExecutionContext, cubes_dir: Path, views_dir: Path) -> None:
        if shutil.which("git") is None:
            raise RuntimeError(
                "GitCubeFilePromoter needs the `git` binary, but none was found on PATH. "
                "This library doesn't (and can't) depend on it via pip -- there's no PyPI "
                "package that installs a working `git` CLI, since it isn't a Python package -- "
                "so it has to come from the environment this actually runs in, the same way "
                "`dbt`/`libpq`/any other system tool would. Install it the way you would any "
                "other system dependency, e.g. `apt-get install -y git openssh-client` on a "
                "Debian-based image."
            )
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            auth = self._build_auth(tmp_path)

            # `git clone` creates this directory itself -- it must not already exist.
            repo_dir = tmp_path / "repo"
            self._clone(repo_dir, auth)

            write_entities(repo_dir / self.cubes_subdir, "cubes", read_entities(cubes_dir, "cubes"))
            write_entities(repo_dir / self.views_subdir, "views", read_entities(views_dir, "views"))

            self._run_git("add", "-A", cwd=repo_dir, auth=auth)
            status = self._run_git("status", "--porcelain", cwd=repo_dir, auth=auth)
            if not status.stdout.strip():
                context.log.info("Generated Cube schema is unchanged -- nothing to commit or push.")
                return

            self._run_git(
                "-c",
                f"user.name={self.author_name}",
                "-c",
                f"user.email={self.author_email}",
                "commit",
                "-m",
                self.commit_message,
                cwd=repo_dir,
                auth=auth,
            )
            self._run_git("push", "origin", self.branch, cwd=repo_dir, auth=auth)
            context.log.info(f"Pushed generated Cube schema to {self.repo_url}@{self.branch}")
