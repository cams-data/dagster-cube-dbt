# dagster-cube-dbt

[![PyPI](https://img.shields.io/pypi/v/dagster-cube-dbt)](https://pypi.org/project/dagster-cube-dbt/)
[![Docs](https://readthedocs.org/projects/dagster-cube-dbt/badge/?version=latest)](https://dagster-cube-dbt.readthedocs.io/)

A Dagster Component (`CubeDbtProjectComponent`) that extends `dagster_dbt.DbtProjectComponent`
to also generate a [Cube](https://cube.dev) semantic-layer schema from your dbt project's
manifest, exposing each generated cube as a virtual, pass-through Dagster asset.

## Install

```yaml
# defs.yaml
type: dagster_cube_dbt.CubeDbtProjectComponent
attributes:
  project: "{{ project_root }}/path/to/dbt_project"
```

`CubeDbtProjectComponent` generates cube/view data and caches it as component state, but has
no default way to *deliver* it anywhere a Cube instance can read it — there's no on-disk path
reachable by both a Dagster run and an independently-running Cube instance in any real
deployment (Dagster Cloud and most container-per-run setups give each run its own throwaway
filesystem). Delivery is a `CubeFilePromoter` **resource** you bind under the
`cube_file_promoter` key — see
[Promoting generated files](#promoting-generated-files-to-your-cube-server) below. For local
development, bind the `LocalFileCubeFilePromoter` this library ships, which writes straight to
a directory on disk.

`CubeDbtProjectComponent` **extends** `dagster_dbt.DbtProjectComponent` — every attribute the
dbt component accepts is still there, unchanged, and controls the dbt-build side exactly as it
would on a plain `DbtProjectComponent`:

| Attribute | Description |
|---|---|
| `project` | The path to the dbt project, or a mapping defining a `DbtProject`. |
| `cli_args` | Arguments to pass to the dbt CLI when executing. Defaults to `['build']`. |
| `select` | The dbt selection string for models in the project you want to include. |
| `exclude` | The dbt selection string for models in the project you want to exclude. |
| `selector` | The dbt selector for models in the project you want to include. |
| `translation` | Function to customize the generated `AssetSpec` for each *dbt* asset — analogous to `cube_translation` below, but for the dbt-build layer. |
| `translation_settings` | Enables/disables various features for translating dbt models into Dagster assets (e.g. `enable_source_tests_as_checks`). |
| `op` | Op-related arguments (name, tags, backfill policy) to set on the generated dbt-build op. |
| `include_metadata` | Optionally include additional metadata (`row_count`, `column_metadata`) in materializations generated while executing dbt models. |
| `prepare_if_dev` | Whether to prepare the dbt project every time in `dagster dev`/`dg` CLI calls. Defaults to `True`. |

See the [dagster-dbt library docs](https://docs.dagster.io/integrations/libraries/dbt/dagster-dbt)
for the authoritative, up-to-date reference — the table above is a summary, not a
replacement. On top of all of that, this component adds:

| Attribute | Description |
|---|---|
| `cube_select` | Optional `{paths, tags, names}` filter controlling which dbt models get turned into cubes at all. Defaults to every model. Use it to keep cubes scoped to, e.g., mart-layer models and skip staging/intermediate ones. |
| `cube_translation` | Optional function to customize the generated `AssetSpec` for each cube or view, analogous to `translation` above but for the cube/view layer. |
| `promoter_resource_key` | Resource key of the `CubeFilePromoter` this component delegates delivery to. Defaults to `"cube_file_promoter"` — only worth changing if a single project has more than one `CubeDbtProjectComponent`, each needing its own promoter. |
| `promotion_pool` | Dagster concurrency pool assigned to the cube/view multi-asset's promotion op. Defaults to a name scoped to this project (`f"{dbt_project.name}_cube_promotion"`), so a max concurrency of 1 can be set for it in the Dagster UI (Deployment > Concurrency) with no code change — see [Promoting generated files](#promoting-generated-files-to-your-cube-server) below for why. |
| `landing_check` | Optional `{api_url, api_token, verify_tls, resource_key, timeout_seconds, poll_interval_seconds}` config turning on a post-promotion poll against Cube's own REST API — see [Checking a promotion actually landed](#checking-a-promotion-actually-landed-in-cube) below. Off (`None`) by default. |

`cube_select` is independent of the inherited `select`/`exclude`/`selector` attributes:
those control which dbt models are actually built by `dbt build` (real data movement),
while `cube_select` controls which of those models additionally get a cube generated. A
model can be built by dbt without generating a cube, and — as long as it's still built —
a cube can be generated for a model without regenerating it here.

```yaml
attributes:
  project: "{{ project_root }}/path/to/dbt_project"
  select: "tag:marts"      # inherited from DbtProjectComponent -- what dbt actually builds
  cube_select:
    tags: ["cube"]
    # or: paths: ["marts"]
    # or: names: ["journey_samples", "destination_locations"]
```

`paths` matches against each model's `path` as recorded in the dbt manifest, which is
**relative to dbt's `model-paths` root** (`models/` by default) — a model at
`models/marts/journey_samples.sql` has manifest path `marts/journey_samples.sql`, so the
filter is `paths: ["marts"]`, not `paths: ["models/marts"]`. Matching is a plain string
prefix check against that path (using whatever path separator dbt recorded, `\` on
Windows), not a proper segment-aware path match.

## Generating cube files

Cube generation and merge-patching only happen when component state is refreshed:

```
uv run dg utils refresh-defs-state
```

(or automatically on `dg dev` / `dagster dev` if `prepare_if_dev` is left at its default).
This mirrors how `DbtProjectComponent` already treats compiling the dbt manifest as state,
not something that happens on every code location reload — the *result* (the merged cube/view
data) is cached as part of that state, not recomputed on every defs load.

That cached result isn't written anywhere on disk yet, though. Delivering it to wherever your
Cube deployment actually reads its schema from happens separately, at *materialization* time,
via the bound `CubeFilePromoter` resource (see
[below](#promoting-generated-files-to-your-cube-server)) — so materializing a cube/view asset
does real I/O, unlike a lot of other virtual/pass-through assets. Every cube and view asset
carries the `cube` [kind tag](https://docs.dagster.io/guides/build/assets/metadata-and-tags/kind-tags)
so they're visually identifiable in the Dagster UI.

### These are virtual assets

Cube and view assets are declared with `AssetSpec(is_virtual=True)` — currently a Dagster
[preview feature](https://docs.dagster.io/api/dagster/assets#dagster.AssetSpec). Dagster's own
staleness/freshness engine treats a virtual asset as transparent: a downstream asset (e.g. a
Cube pre-aggregation refresh) depending on a cube asset has its freshness resolved by looking
straight *through* the virtual layer to the nearest non-virtual ancestor — the dbt model the
cube is derived from — without the cube asset itself needing to be materialized at all. This
works recursively through chained virtual assets too: a view depending on a virtual cube
resolves all the way back to the real dbt model.

### When cube/view assets actually run

Cube and view assets still materialize automatically sometimes — that's what actually invokes
the bound promoter (below) and delivers the generated files, plus it gives materialization
history in the asset catalog — but that's independent of the freshness-propagation mechanism
above, which works whether or not they're ever materialized.

They deliberately do **not** use `AutomationCondition.eager()`, which would re-run on every
dbt model *data* update. A cube's generated content only changes when its dbt model or a
merge patch changes — not every time the model's data refreshes — so re-running on every data
update would be wasteful (needlessly re-triggering promotion, e.g. a git commit/push, on runs
where nothing about the schema actually changed). Instead, each spec's
`code_version` is a hash of its own generated YAML, and the automation condition runs once
when either the asset has never materialized (and its deps are ready) or its `code_version`
changes — not on every upstream data update:

```python
(
    (
        AutomationCondition.missing()
        & ~AutomationCondition.any_deps_missing()
        & ~AutomationCondition.any_deps_in_progress()
    ).newly_true().since_last_handled()
    | AutomationCondition.code_version_changed()
) & ~AutomationCondition.in_progress()
```

The "never materialized yet" clause wraps `newly_true().since_last_handled()` around the
*whole* "missing and deps are ready" state, not around bare `missing()` alone. That
distinction matters: wrapping `missing()` alone doesn't stop being true while the asset stays
unmaterialized, so it can start re-requesting a run that's already pending a couple of ticks
later; wrapping only the trigger without the deps gate can permanently miss its one chance to
fire if blocked at the exact tick the asset first becomes missing (the mechanism `eager()`
itself relies on `any_deps_updated()` as a second, independent trigger to recover from — which
isn't available here, since `code_version_changed()` doesn't care about dependency updates at
all). `code_version_changed()` needs no such wrapping — unlike a one-tick pulse, it already
stays true until an evaluation actually picks it up.

Like `eager()` itself, this only reacts to *transitions* from the baseline established at its
first-ever evaluation forward — a dbt model that's already materialized before that first
evaluation (e.g. adding this component to an existing, already-running dbt project) doesn't
trigger a cube's first materialization on its own; that first one needs a manual kick, same
as it would with `eager()`.

These assets are also targeted by their own `AutomationConditionSensorDefinition`
(`<dbt_project_name>_cube_automation_condition_sensor`), rather than relying on the
platform's single default sensor — Dagster automatically excludes any asset targeted by an
explicit sensor from the default one, so this doesn't cause double evaluation.

## Promoting generated files to your Cube server

Delivery is delegated to a **resource**, not an overridable component method — promotion
needs credentials and destination config (an S3 bucket, a git remote, ...), exactly what
Dagster resources are for, and it keeps `CubeDbtProjectComponent`'s own subclassing surface
reserved for asset-shape customization (`get_cube_asset_spec`/`get_view_asset_spec`) rather
than runtime dependencies. There's no default bound to `promoter_resource_key`
(`cube_file_promoter`) — there's no deployment topology where a fixed on-disk path is
reachable both by a Dagster run and an independently-running Cube instance (Dagster Cloud and
most container-per-run setups give each run its own throwaway filesystem; even self-hosting
both on the same Kubernetes cluster means going out of your way to mount a shared volume into
both pods, a real but unusual setup, not something worth defaulting to) — so materializing a
cube/view asset with nothing bound fails clearly rather than silently doing nothing.

Implement a `CubeFilePromoter` subclass to push the generated files wherever your Cube
deployment actually reads its schema from, and bind an instance under the `cube_file_promoter`
resource key anywhere in the project; it doesn't need to be declared alongside the component
itself, since Dagster merges resources from every `Definitions` in the project by key:

```python
# e.g. defs/resources.py -- any plain Python defs module works, auto-discovered
# the same way YAML component defs.yaml files are
import dagster as dg
from dagster_cube_dbt import GitCubeFilePromoter

@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "cube_file_promoter": GitCubeFilePromoter(
                repo_url="git@github.com:your-org/your-cube-repo.git",
                ssh_private_key=dg.EnvVar("CUBE_DEPLOY_SSH_KEY"),
            )
        }
    )
```

`promote(context, cubes_dir, views_dir)` is called once per materialization of the cube/view
multi-asset, with `cubes_dir`/`views_dir` holding every currently generated (and
merge-patched) cube/view file — not just whatever subset of assets was actually selected for
that run — staged in a temp directory that's deleted as soon as the call returns. It runs
before any `MaterializeResult` is yielded, so a failure here fails the run instead of
reporting a false materialization.

### Git-based promotion: `GitCubeFilePromoter`

Pushing generated cube/view YAML to a git repository is likely the most common production
setup, so this library ships a ready-to-use implementation covering both real patterns:

- **An arbitrary repo that something else syncs down** — e.g. a manually-configured
  [`kubernetes/git-sync`](https://github.com/kubernetes/git-sync) sidecar polling it into a
  shared volume for a self-hosted Cube Core to read its schema from. Authenticate with a
  dedicated **SSH deploy key** (`ssh_private_key`, the key file's contents — not a path).
- **Cube Cloud's own git remote directly**, via its
  ["Deploy with Git"](https://docs.cube.dev/admin/deployment/continuous-deployment) mode
  (Settings → Build & Deploy → Deploy with Git → Generate Git credentials). Cube Cloud's own
  docs set this up with `git config credential.helper store` — i.e. **HTTP username+token**
  auth, not SSH — so authenticate with `http_username`/`http_token` instead.

Set exactly one of the two credential pairs; `GitCubeFilePromoter` raises clearly at
construction time if you set both or neither. Each `promote()` call shallow-clones fresh into
a throwaway temporary directory (`--depth 1` — this resource never needs history, only ever
adding one new commit on top of the remote's current tip), writes the generated files under
`cubes_subdir`/`views_subdir` (defaulting to `model/cubes`/`model/views`, Cube's own standard
project layout), and commits + pushes only if something actually changed.

This resource shells out directly to the `git` binary (and, for the SSH option, `ssh`) — it
needs both present on `PATH` wherever it actually runs. Neither is a Python package this
library can depend on via pip (there's no PyPI package that installs a working `git` CLI,
since it isn't a Python package at all) — install it the same way you'd install any other
system tool your image needs, e.g. `apt-get install -y git openssh-client` on a Debian-based
image. A clear error is raised immediately if `git` isn't found, rather than a confusing
failure deep inside a subprocess call.

### Local development: `LocalFileCubeFilePromoter`

For local dev — where a `cube dev` process running on the same machine can just read files off
disk — bind `dagster_cube_dbt.LocalFileCubeFilePromoter(output_dir=..., views_output_dir=...)`
under `cube_file_promoter` instead of implementing your own. This only makes sense because the
Dagster process and the Cube process share a filesystem on a laptop; don't reach for it beyond
local dev/testing for the same reason there's no default promoter on the component.

One caveat specific to this promoter: if `output_dir`/`views_output_dir` happens to live
inside the same directory tree as the component's own `defs.yaml` (rather than a sibling
project-level directory, as in the example project), the files it writes on materialization
will get picked up as merge patches on the *next* state refresh, since patch-file discovery
recursively scans that whole tree and the component has no way to know what a bound resource
writes where. Keep promoter output outside the component's defs directory.

### Concurrency: the `promotion_pool`

Most `CubeFilePromoter` implementations mutate some shared external state — a persistent git
checkout, a fixed output directory, an S3 prefix — that two runs promoting at the same time
would corrupt or race on (a git-based promoter, for instance, resets its local checkout to a
known state before writing, which two concurrent runs would just fight over). Since this is
true of nearly every real implementation, not just one, the cube/view multi-asset's op is
assigned a Dagster [concurrency pool](https://docs.dagster.io/guides/operate/managing-concurrency/concurrency-pools)
by default (`promotion_pool`, scoped to the project by name) rather than leaving every project
to remember to configure this themselves.

Assigning the pool by itself changes nothing — a pool with no configured limit behaves exactly
like having no pool at all. To actually enforce single-flight promotion, set a maximum
concurrency of 1 for that pool name in the Dagster UI (Deployment → Concurrency), no code
change required. Override `promotion_pool` explicitly only if you want multiple
`CubeDbtProjectComponent`s that share the same underlying promoter/destination to also share
one pool, so they're mutually exclusive with *each other* too, not just internally.

## Checking a promotion actually landed in Cube

By default, a cube/view asset is considered materialized as soon as `CubeFilePromoter.promote`
returns — which only means the generated YAML was *handed off*, not that a running Cube
instance has actually picked it up yet. Hot-reload/propagation lag varies by deployment (a
self-hosted Cube watching a mounted volume, `git-sync` polling a repo, Cube Cloud's own build
pipeline after a git push), and none of that is visible to Dagster by default — a cube/view
asset can show green in the Dagster UI while the corresponding table still doesn't exist yet in
Cube's SQL API.

Set `landing_check` to close that gap: after promotion, this component stamps each promoted
cube/view's own `code_version` into `meta.dagster_cube_dbt.code_version` (merged alongside
whatever `meta` you've already set — see
[Setting cube attributes from dbt model meta](#setting-cube-attributes-from-dbt-model-meta) —
never overwriting it), then polls Cube's `GET /meta` REST endpoint until every cube/view
selected for that run echoes the matching value back, or fails the run once `timeout_seconds`
elapses.

Two ways to give `landing_check` a `CubeApiClient` to poll through, chosen by whether `api_url`
is set:

```yaml
# defs.yaml -- component-managed (the common case): set api_url/api_token directly and this
# component builds and owns its own CubeRestApiClient, no separate resource binding needed.
type: dagster_cube_dbt.CubeDbtProjectComponent
attributes:
  project: "{{ project_root }}/path/to/dbt_project"
  landing_check:
    api_url: "https://your-deployment.cubecloudapp.dev/cubejs-api/v1"
    api_token: "{{ env.CUBE_API_TOKEN }}"
    timeout_seconds: 60             # default
    poll_interval_seconds: 2        # default
```

```yaml
# defs.yaml -- external resource: leave api_url unset and bind a CubeApiClient yourself, for a
# test double, a non-REST implementation, or one instance shared across multiple components.
type: dagster_cube_dbt.CubeDbtProjectComponent
attributes:
  project: "{{ project_root }}/path/to/dbt_project"
  landing_check:
    resource_key: cube_api_client   # default; only worth changing with more than one instance
    timeout_seconds: 60
    poll_interval_seconds: 2
```

```python
# e.g. defs/resources.py -- only needed for the external-resource path above
import dagster as dg
from dagster_cube_dbt import CubeRestApiClient

@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "cube_api_client": CubeRestApiClient(
                api_url="https://your-deployment.cubecloudapp.dev/cubejs-api/v1",
                api_token=dg.EnvVar("CUBE_API_TOKEN"),
            )
        }
    )
```

`resource_key` currently still defaults to `"cube_api_client"` for backwards compatibility; a
future release will remove that default, requiring it to be set explicitly whenever the
external-resource path is what you actually want.

`api_token` is sent verbatim in the `Authorization` header — Cube's REST API takes a bare
token there, **not** a `Bearer <token>` scheme — typically a JWT signed with your deployment's
`CUBEJS_API_SECRET`. Generating/rotating that token, and deciding what security-context claims
it needs for your deployment's access rules, is left entirely to you; `CubeRestApiClient`
doesn't sign one itself. Implement `CubeApiClient` directly instead (the external-resource path)
if your setup needs something other than a straight `GET {api_url}/meta` call (e.g. a proxy in
front of Cube).

`verify_tls` (default `True`) controls certificate verification, passed straight through to
the underlying `requests.get(..., verify=...)` call in the component-managed path. Set it to
`False` only for a deployment you can't otherwise reach with a valid certificate — a
self-hosted instance behind a self-signed or internal-CA cert, most often — and treat it with
the same caution you would `requests`' own `verify=False`: it disables certificate
verification entirely for every request this resource makes.

Off by default: it needs Cube API credentials and adds latency (at least one HTTP round trip,
likely several while waiting for Cube to catch up) to every promotion, which not every project
needs. When off, promoted YAML is byte-identical to what it would be without this feature at
all — no `meta.dagster_cube_dbt` key is added. On timeout, the run fails outright (naming
exactly which cube(s)/view(s) never landed) rather than reporting a false materialization, the
same contract `CubeFilePromoter.promote` itself has — since a failed run leaves the asset's
`code_version` unchanged, the next automation evaluation just retries the whole
promote-then-poll cycle.

## Syncing views into Apache Superset

A separate, standalone component — `CubeSupersetSyncComponent` — syncs each generated Cube
**view** (not cubes — Cube's own convention is that views are the intended BI-facing query
layer) into a matching [Apache Superset](https://superset.apache.org/) dataset: column
`verbose_name`/`description`/groupby/filter flags from the view's dimensions, and one metric
per measure, expressed via Cube SQL API's own aggregation-pushdown syntax
(`MEASURE(<view>.<measure>)`) so Cube — not Superset — performs the aggregation. This gives BI
users column descriptions and pre-defined metrics for every view without anyone manually
configuring datasets in Superset by hand.

It's a separate component chained onto a `CubeDbtProjectComponent` via `context.load_component`
— reading that component's already-generated, cached state directly — rather than a subclass,
so `project:`/`cube_select:`/merge-patch config lives in exactly one `defs.yaml` block, not
duplicated across two:

Two ways to give this component a `SupersetResource` to sync through, chosen by whether
`base_url` is set:

```yaml
# defs.yaml -- component-managed (the common case): set base_url/username/password directly
# and this component builds and owns its own SupersetResource, no separate resource binding
# needed.
type: dagster_cube_dbt.CubeSupersetSyncComponent
attributes:
  dbt_cube_component: "../dbt_ingest"   # path to the CubeDbtProjectComponent's defs.yaml dir
  database_name: "Cube"                 # default; the Superset database connection's name
  base_url: "https://superset.example.com"
  username: "{{ env.SUPERSET_USERNAME }}"
  password: "{{ env.SUPERSET_PASSWORD }}"
```

```yaml
# defs.yaml -- external resource: leave base_url unset and bind a SupersetResource yourself,
# for a test double, or one instance shared across multiple components.
type: dagster_cube_dbt.CubeSupersetSyncComponent
attributes:
  dbt_cube_component: "../dbt_ingest"
  database_name: "Cube"
  superset_resource_key: "superset"    # default; only worth changing with more than one instance
```

```python
# e.g. defs/resources.py -- only needed for the external-resource path above
import dagster as dg
from dagster_cube_dbt import SupersetResource

@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "superset": SupersetResource(
                base_url="https://superset.example.com",
                username=dg.EnvVar("SUPERSET_USERNAME"),
                password=dg.EnvVar("SUPERSET_PASSWORD"),
            )
        }
    )
```

`superset_resource_key` currently still defaults to `"superset"` for backwards compatibility;
a future release will remove that default, requiring it to be set explicitly whenever the
external-resource path is what you actually want.

| Attribute | Description |
|---|---|
| `dbt_cube_component` | Path to the `CubeDbtProjectComponent`'s `defs.yaml` directory, resolved relative to the defs root. |
| `database_name` | Name of the Superset database connection pointed at Cube's SQL API. Defaults to `"Cube"`. This component doesn't create that connection — set it up once in Superset yourself (see above), the same way `CubeDbtProjectComponent` assumes a running Cube instance already exists rather than provisioning one. |
| `base_url` / `username` / `password` | Set together to have this component build and own its own `SupersetResource` directly. Leave all unset to fall back to `superset_resource_key` instead. |
| `superset_resource_key` | Resource key of the `SupersetResource` this component syncs through, when `base_url` is left unset. Defaults to `"superset"`. |
| `sync_pool` | Dagster concurrency pool assigned to the dataset-sync multi-asset's op. Defaults to a name scoped to `dbt_cube_component` — set a max concurrency of 1 for it in the Dagster UI if two runs syncing the same Superset dataset concurrently turns out to be a problem, mirroring `promotion_pool` above. |

Each dataset asset depends on the corresponding view asset (through the sibling component's own
`get_view_asset_spec`/`asset_key_for_view` — so a subclass renaming view keys is still
respected automatically) and reuses `GENERATED_ASSET_AUTOMATION_CONDITION`: a dataset only
needs updating when the view's own generated definition changes (its `code_version`), not on
every dbt data refresh underneath it.

`SupersetResource` handles the login/CSRF/find-or-create-dataset/refresh/update-columns flow
against Superset's own REST API, authenticating once per resource instance rather than once per
dataset. `password` (like `CubeRestApiClient.api_token`) is a plain `str` config field — bind it
from wherever your project already manages secrets, not a literal value in checked-in
`defs.yaml`: `{{ env.SOME_VAR }}` templating in the component-managed `defs.yaml` path above, or
`dg.EnvVar(...)` in the external-resource `resources.py` path.

## Base generation rules

For each dbt model, reading `manifest.json` directly (see `manifest.py` — this library has no
external dependency for dbt-manifest reading; see [Why no `cube_dbt` dependency](#why-no-cube_dbt-dependency)
below):

- `name`, `description`, `sql_table` — passed through from the model. `description` is only
  present if the dbt model has one. `sql_table` prefers the manifest's `relation_name` when
  present, falling back to a manually-built `` `database`.`schema`.`alias-or-name` `` otherwise.
- `dimensions` — one per surfaced column (see below), each with `description` passed through
  if the column has one, `type` inferred from `data_type`, and `primary_key: true` detected
  in strict priority order:
  1. A model-level dbt 1.5+ `primary_key` constraint (`constraints: [{type: primary_key,
     columns: [...]}]`) — the required form for composite keys. (Confirmed against a real
     `dbt parse`, dbt-core + DuckDB: this can never coexist with the column-level form below
     on the same model — dbt's own parser hard-errors if both are declared — so in practice
     only one of these two tiers is ever populated; the priority order exists for
     defensiveness, not because real manifests exercise it.)
  2. Otherwise, a column-level dbt 1.5+ constraint (`columns: [{name: ..., constraints:
     [{type: primary_key}]}]`).
  3. Otherwise, the model's `config.primary_key` (plain string or list). This is for
     warehouses like ClickHouse that have no SQL primary-key constraint at all — there,
     `config(primary_key=...)` on the model is the *only* way to declare one, and it's kept
     in a wholly separate manifest field from `constraints`, confirmed against a real
     `dbt parse` (dbt-clickhouse).
  4. Otherwise, the model's `config.order_by` (same plain-string-or-list shape). A ClickHouse
     MergeTree table's `ORDER BY` clause *is* its primary key whenever `PRIMARY KEY` isn't set
     explicitly (confirmed against ClickHouse's own docs: "If no primary key is defined...
     ClickHouse uses the sorting key as primary key"). `order_by` can also hold an arbitrary
     SQL expression rather than plain column names (e.g. a function call) — any entry that
     doesn't match an actual column on the model is silently dropped rather than treated as a
     bogus primary-key column.
  5. Otherwise, a column tagged `primary_key`, or covered by both a `unique` and a `not_null`
     test.
- **No measures or joins are generated.** Both are left entirely to merge patches.

### Every cube-generating model needs an enforced dbt contract

Any model matched by `cube_select` must have
[dbt contract enforcement](https://docs.getdbt.com/reference/resource-configs/contract) turned
on:

```yaml
# schema.yml
models:
  - name: journey_samples
    config:
      contract:
        enforced: true
    columns:
      - name: journey_sample_key
        data_type: integer
      ...
```

Without this, generation only ever sees whatever columns happen to be declared in
`schema.yml` — dbt doesn't back-fill a model's *actual* output columns into `manifest.json`
unless a contract is enforced. A model with an empty (or incomplete) `columns:` block would
otherwise silently produce a technically successful but useless cube (`dimensions: []` or
missing several), with no error pointing at the real cause. Enforcing a contract closes that
gap two ways: dbt itself refuses to even *parse* a contracted model unless every declared
column has a `data_type` (see below), and it validates at `dbt build` time that the model's
real output can't silently drift to have columns beyond what's declared — a guarantee this
library's own manifest-only checks can't provide on their own.

**Generation raises `UnenforcedContractError`** listing every `cube_select`-matched model
missing contract enforcement, collected in one pass. Fix it by enabling the contract (and
declaring `data_type` for every column dbt then requires), or by narrowing `cube_select` to
exclude models you don't want a cube for.

### Excluding a column from cube generation

A column can opt out of becoming a *dimension* entirely by setting `meta.cube.dimension:
false` in `schema.yml` — note this still needs a `data_type`, since dbt's contract
enforcement (above) has no notion of this flag and requires one on every declared column
regardless:

```yaml
# schema.yml
models:
  - name: journey_samples
    columns:
      - name: internal_row_hash
        data_type: varchar
        config:
          meta:
            cube:
              dimension: false
```

Excluded columns never appear as a dimension and are exempt from the required-`data_type`
check below — since no dimension is generated for them, there's nothing to type — but dbt's
own contract check still requires the `data_type` above regardless.

(`meta` nested under `config:` rather than declared bare on the column — dbt-core accepts
both and treats them identically, but the [dbt Fusion engine](https://docs.getdbt.com/guides/fusion)
only accepts the `config:`-nested form; this component reads the same manifest location
either way, so nesting under `config:` works with both engines.)

### Setting dimension attributes from dbt column meta

`meta.cube` is a reserved namespace on a dbt column. Besides `dimension: false`, three keys
are promoted straight onto the generated dimension as top-level attributes: `order`, `mask`,
and `public`:

```yaml
# schema.yml
models:
  - name: journey_samples
    columns:
      - name: journey_type
        config:
          meta:
            cube:
              order: 1
              public: false
```

produces:

```yaml
dimensions:
  - name: journey_type
    ...
    order: 1
    public: false
```

A fourth key, `type`, is also reserved under `meta.cube` — it overrides the dimension's
inferred type instead of adding a new attribute, so it's covered separately below (see
[Overriding the inferred type with `meta.cube.type`](#overriding-the-inferred-type-with-metacubetype)).

Any other key under `meta.cube` (not one of `dimension`/`order`/`mask`/`public`/`type`), and
any meta outside the `cube` namespace entirely, is left alone and passed through into the
dimension's own `meta:` field rather than dropped — only the recognized control/promoted
keys are consumed.

### Setting cube attributes from dbt model meta

The same `meta.cube` convention applies one level up, on the dbt **model** itself, not just
its columns. Two keys are promoted straight onto the generated cube as top-level attributes:
`public` and `title`:

```yaml
# schema.yml
models:
  - name: journey_samples
    config:
      meta:
        cube:
          public: false
          title: "Journey Samples"
```

produces:

```yaml
cubes:
  - name: journey_samples
    ...
    public: false
    title: "Journey Samples"
```

Any other key under the model's `meta.cube` (not `public`/`title`/`name`/`suffix` — the last
two are covered next), and any model `meta` outside the `cube` namespace entirely, is passed
through into the cube's own `meta:` field the same way it is for dimensions — nothing is
silently dropped.

### Renaming the generated cube with `meta.cube.name` / `meta.cube.suffix`

By default a cube's name is its dbt model's name. Two more `meta.cube` keys override that —
`name` (the cube's full name outright) or `suffix` (appended as-is to the model's own name,
no separator inserted, so include one in the value itself, e.g. `"_base"`) — **mutually
exclusive**; setting both on the same model raises `ConflictingCubeNameError`.

This supports a common Cube pattern: generate a suffixed, `public: false` "base" cube, then
hand-author a separate `extends:` cube (via a merge patch) that exposes it publicly under a
plain name:

```yaml
# schema.yml
models:
  - name: journey_samples
    config:
      meta:
        cube:
          suffix: "_base"
          public: false
```

```yaml
# a merge patch, e.g. patches/journey_samples.yaml -- one dbt model, reused under two
# different join roles, is a common reason for more than one extending cube
cubes:
  - name: origin_locations
    extends: journey_samples_base
    public: true
  - name: destination_locations
    extends: journey_samples_base
    public: true
```

Renaming happens before merge patches are applied, so patches (and any `extends:`) target the
*renamed* cube — whatever name is visible in the generated/promoted YAML is what a patch
needs to reference, same as for any other generated cube.

Each Dagster asset in a chain like this depends on exactly the thing directly upstream of it:
a cube that `extends` another depends on *that cube's own asset* (`origin_locations`/
`destination_locations` above both depend on the `journey_samples_base` cube asset), not the
dbt model directly, and `journey_samples_base` itself depends on the real dbt model asset. A
flatter dependency (every extending cube pointing straight at the dbt model) would be less
accurate — literally what each cube's SQL and dimensions actually come from is the cube it
extends, not the dbt model. This isn't a freshness/lineage gap either: cube/view assets are
`is_virtual`, and Dagster's own staleness engine already looks straight through a *chain* of
virtual assets to the nearest real ancestor, so freshness still propagates back to the dbt
model transitively through however many `extends` hops separate a cube from it.

### Dimension types are required, not inferred silently

A contract-enforced model (above) can't actually reach generation with an undeclared
`data_type` on any of its columns — dbt itself refuses to parse it. This check exists as a
second line of defense with a more specific, per-column error, and as the mechanism behind
*why* contracts help: dbt does **not** back-fill column types from the warehouse into
`manifest.json` during `parse`/`compile`; that introspected info lives only in
`catalog.json` (from `dbt docs generate`), which this component does not read, so an
undeclared `data_type` is a real gap dbt itself won't fill in for you.

Rather than silently typing an undeclared column as `string` (which would happen to be
wrong for most numeric/time columns), **generation raises an error** if any surfaced column
(i.e. one that isn't excluded via `meta.cube.dimension: false`) on a model selected by
`cube_select` has no `data_type` declared. The error lists every offending `model.column` in
one pass, so you can fix them all at once rather than rerunning state refresh after each fix.

### Overriding the inferred type with `meta.cube.type`

A column's dbt `data_type` is mapped to a Cube dimension type (`string`, `number`, `time`,
`boolean`, `geo`) using a fixed mapping table (`manifest.TYPE_MAPPINGS`) built around common
warehouse type names — Snowflake, BigQuery, Postgres/Redshift, and (explicitly, since it
leans on its own distinct type names pervasively, even for ordinary dimension tables)
ClickHouse: `UInt8`/`Int32`/etc., `Date32`, `DateTime64(...)`, `Bool`, `UUID`,
`FixedString(...)`, `Enum8`/`Enum16`, `IPv4`/`IPv6`. `Nullable(T)` and `LowCardinality(T)`
are unwrapped to whatever `T` is (recursively, so `LowCardinality(Nullable(String))` resolves
too) rather than naively stripped like precision/scale parameters (`Decimal(10,2)`) — since
either wrapper can wrap *any* type, blindly discarding the parenthesized content would
collapse `Nullable(String)` and `Nullable(Int32)` to the same bare `nullable`, losing the
real type entirely.

A warehouse type genuinely outside all of that has no entry, and **generation raises
`UnrecognizedColumnTypeError`**, collecting every such column in one pass, the same way
`MissingDimensionTypeError` does. Set `meta.cube.type` to bypass type inference entirely for
a column and use the given Cube dimension type directly:

```yaml
# schema.yml
models:
  - name: dates
    columns:
      - name: some_genuinely_unrecognized_type
        data_type: SomeExoticWarehouseType
        config:
          meta:
            cube:
              type: time
```

### `geo` columns aren't generated — and can't be via `meta.cube.type` either

`geo` is a real Cube dimension type (`TYPE_MAPPINGS` maps BigQuery's `GEOGRAPHY` to it), but
this library can never actually build one: per
[Cube's own docs](https://docs.cube.dev/reference/data-modeling/dimensions), a `geo`
dimension requires separate `latitude`/`longitude` SQL sub-expressions *instead of* a single
`sql` field — a structurally different shape from every other dimension generated here, which
always emit a single `sql`. There's no generic way to derive two SQL expressions from one dbt
column's declared type either: a ClickHouse `Point` is a `Tuple(Float64, Float64)`, while
`Polygon`/`MultiPolygon` don't even have a single lat/long point to extract.

A column that would resolve to `geo` — whether inferred (e.g. `GEOGRAPHY`) or via an explicit
`meta.cube.type: geo` override — makes generation raise **`UnsupportedGeoDimensionError`**,
collecting every such column the same way the other generation errors do. There are two ways
to handle it:

- Exclude the column with `meta.cube.dimension: false` (simplest, and the right call if the
  raw geometry isn't needed as a Cube dimension at all).
- Hand-author a `geo` dimension for it directly in a merge patch, with real
  `latitude`/`longitude` SQL expressions for your warehouse (e.g. a coordinate-extraction
  function). This is fully general and doesn't require any change to this library — merge
  patches operate on the generated cube's `sql_table` directly.

## Why no `cube_dbt` dependency

Earlier versions of this library used [`cube_dbt`](https://pypi.org/project/cube_dbt/), the
dbt-integration package Cube.dev itself publishes, for exactly the manifest-reading rules
above. It's now vendored (`manifest.py`) instead, for a few concrete reasons:

- Its public API didn't actually cover what this library needs — reading a column's raw,
  undecorated `data_type` (to tell "declared as `string`" apart from "nothing declared") and
  a model's dbt contract-enforcement status both required reaching into its private,
  undocumented attributes (`Column._column_dict`, `Model._model_dict`), which is fragile by
  construction: a future release renaming either one would silently break this library with
  no warning beyond a regression test failing.
- Its type-mapping table is a real, structural gap for anything outside mainstream
  warehouses (see `meta.cube.type` above) — and its release cadence (roughly every 5-14
  months as of writing) means waiting on an upstream fix isn't realistic; this library's own
  copy of the mapping table can be extended immediately whenever a real gap turns up.
- It ships its own Jinja string-dumping helpers (`as_cube()`/`as_dimensions()`) for a usage
  pattern (`cube_dbt` invoked live from inside Cube's own Jinja templates) this library never
  used — it always built cube/dimension dicts directly from manifest data instead, so the
  actual reusable surface was already small (model/column filtering, primary-key detection,
  a handful of property passthroughs) well before this change.

## Merge patches

Any `*.yml`/`*.yaml` file found underneath the directory containing the component's
`defs.yaml` (other than `defs.yaml` itself) is treated as a merge patch and folded into the
generated base document, in path-sorted order, using a
strategic merge keyed by each list item's `name` field — the same approach as
[`pyyaml-merger`](https://github.com/john-wd/pyyaml-merger) / Kustomize's strategic merge
patch (this package vendors that small algorithm on top of `deepmerge` rather than depending
on the unpublished, unmaintained original).

Given the generated base:

```yaml
cubes:
  - name: journey_samples
    sql_table: ch_transport_silver.journey_samples
    dimensions:
      - name: journey_sample_key
        type: string
        primary_key: true
      - name: journey_type
        type: string
      - name: direction
        type: string
```

A patch file anywhere under the `defs.yaml` directory can both remove a dimension and add the
measures/joins that generation deliberately leaves out:

```yaml
cubes:
  - name: journey_samples
    $mergeStrategy: patch
    dimensions:
      - name: journey_type
        $mergeStrategy: remove
    measures:
      - name: count
        type: count
    joins:
      - name: destination_locations
        relationship: many_to_one
        sql: "{CUBE}.destination_location_key = {destination_locations.geographic_location_key}"
```

`$mergeStrategy` also supports `replace` (swap the whole matched item) and `merge` (the
default: deep-merge fields of the matched item).

### `patch`: guarding against a target that's since disappeared

`$mergeStrategy: patch` (on the `journey_samples` cube above) marks an item as expected to
match something that already exists — behaving exactly like the default `merge` strategy when
it does, but **raising a generation-time error** if it doesn't, instead of silently creating a
new, broken entry.

That silent-creation failure mode is real, not theoretical: without `patch`, if the
`journey_samples` dbt model were later renamed or dropped, the cube-level match above would
fail, and the *whole item* — including the unprocessed nested `dimensions: [{name:
journey_type, $mergeStrategy: remove}]` — would get appended as a new "cube" verbatim. The
result is genuinely broken output: a cube missing `sql_table`, still carrying a literal,
meaningless `$mergeStrategy` key nested inside it (nested `$mergeStrategy` keys are only
stripped when an item actually goes through the merge machinery, not when the whole parent
item is appended as-is) — silently written to disk and pushed toward your Cube server to fail
there, with no indication at generation time of what actually went wrong.

`patch` is deliberately **per-item, not per-file**: a single file can patch an existing cube
and introduce brand new ones below it, which is a common, reasonable thing to want in one
file (e.g. patching a cube to add a join, then defining a couple of smaller cubes that build
on it) — nothing here requires splitting that across files.

`$mergeStrategy: remove` targeting something that no longer exists is different: it's a
no-op, not an error, since the outcome converges either way (the target isn't in the output
whether the `remove` matched or was already moot) — but it's surfaced as a Python warning
(visible in `dg utils refresh-defs-state` output) since an unmatched `remove` is usually a
sign the patch has gone stale and is worth cleaning up.

## Cubes and views beyond dbt

A merge-patch file isn't limited to patching cubes generated from a dbt model. Since the
merge is purely name-keyed, a file that references a `name` not present in the generated base
is simply appended rather than merged — so a patch file can define an entirely new,
hand-written cube with no backing dbt model, or introduce a top-level `views:` list (Cube's
concept of a virtual grouping over several cubes), and both are picked up the same way:

```yaml
# a purely hand-written cube, no corresponding dbt model
cubes:
  - name: exchange_rates
    sql: "SELECT * FROM some_external_source"
    dimensions:
      - name: currency
        sql: currency
        type: string

views:
  - name: journeys_overview
    cubes:
      - join_path: journey_samples
        includes: "*"
      - join_path: destination_locations
        includes: "*"
```

Every cube and view in the final merged output gets its own Dagster asset — not just ones
generated from a dbt model:

- A cube's `deps` are wired to a dbt model asset only if its `name` matches a dbt model
  somewhere in the manifest (checked by name alone, independent of `cube_select`). A cube
  with no matching model — like `exchange_rates` above — gets `deps=[]`.
- A view's `deps` are the cube assets listed in its own `cubes:` composition list (e.g.
  `journeys_overview` above depends on the `journey_samples` and `destination_locations`
  cube assets). Unlike a cube's `joins` (query-time only, not a dependency), a view's
  `cubes:` list is what the view is actually composed of, so this is modeled as a real
  dependency edge.
- Both materialize the same way as generated cubes: the bound promoter runs, then a
  `MaterializeResult` is yielded.

## `extends` and asset specs

A cube can use Cube's own [`extends`](https://docs.cube.dev/reference/data-modeling/cube#extends)
to reuse another cube's fields, typically introduced via a merge patch:

```yaml
cubes:
  - name: journey_samples_summary
    extends: journey_samples
    title: Journey Samples (Summary View)
```

The generated/promoted YAML keeps `extends:` exactly as written — Cube resolves it itself at
its own runtime, the same as it always has. But `get_cube_asset_spec` sees each cube with
`extends` chains already resolved (parent fields folded in, following multi-level chains, the
cube's own fields winning on conflicts — the same semantics Cube itself uses): so
`journey_samples_summary` above, despite declaring only `title`, still gets
`journey_samples`' `description` and other fields reflected in its `AssetSpec`'s `description`/
metadata/`code_version` if you don't override `get_cube_asset_spec`, and remains easily
available (as ordinary fields on the same `cube` dict) if you do. One consequence: a cube's
`code_version` now also changes whenever an ancestor's fields change, not just its own — its
*effective* definition changed either way, so it's correct for
`GENERATED_ASSET_AUTOMATION_CONDITION` to still pick that up and re-run it once.

An `extends` target not found among the cubes this component itself generated/patched (e.g.
a hand-authored cube living in a completely different part of the Cube project) is left
unresolved for spec-building purposes — there's no visibility into it, though Cube will still
resolve it correctly in the real output at its own runtime.

`deps` is the one thing deliberately *not* flattened through `extends` chains this way: a
cube's dependency is always its immediate `extends` parent's own cube asset (a single hop),
not whatever dbt model sits at the root of the chain — see
[Renaming the generated cube](#renaming-the-generated-cube-with-metacubename-metacubesuffix)
above for why.

## Column schema metadata

Every cube asset's `AssetSpec` carries `dagster/column_schema` metadata (a `TableSchema`) built
from its dimensions and measures — name, Cube's own type (`string`/`number`/`time`/... for a
dimension, `count`/`sum`/... for a measure), and description where present, each tagged
`dagster_cube_dbt/member_type: dimension` or `measure` so the two are distinguishable in the
Dagster UI's column view. It's static, part of the `AssetSpec` itself (built in
`get_cube_asset_spec`, extends-resolved like everything else there) — visible immediately for
every cube asset, including ones never materialized, same as everything else about these
virtual assets.

This is deliberately **not** `dagster/column_lineage` (`TableColumnLineage`) — that metadata
key is for real column-level dependency edges onto *specific upstream asset columns*, which
would mean tracing which dbt model column(s) actually feed each dimension, and, for measures,
which columns an aggregation like `sum` or `count` touches. `TableColumnLineage` also has no
`type`/`description` fields at all — it's purely a graph of edges, so it couldn't carry what's
actually useful here even if column-level lineage were in scope. `dagster/column_schema` is
the metadata key that actually models "this asset has these columns, of these types."

The one constraint actually populated is which dimension(s) make up the cube's primary key
(`dimension.primary_key: true` — set on more than one dimension for a composite key, exactly
mirroring how Cube's own schema represents one). A single-column key gets column-level
`unique: true`/`nullable: false` and `other: ["primary key"]`; a composite key's individual
dimensions are each `nullable: false` but **not** individually `unique` (no single column in
a composite key is unique on its own — only the combination of all of them is), so the
composite relationship is instead spelled out as a table-level constraint:
`"primary key: (col_a, col_b)"`. Every other column gets the ordinary default constraints
(nullable, not unique) — this library doesn't otherwise infer nullability, uniqueness, or
any other constraint from dbt.

## Full example

Every piece above is exercised together in
[`dagster-cube-dbt-tests`](https://github.com/cams-data/dagster-cube-dbt/tree/main/python_modules/dagster-cube-dbt-tests),
a small real dbt + Dagster + Cube project used to develop and test this library against
(`dg dev`-able, not just unit tests):

- [`defs/dbt_cubes/defs.yaml`](https://github.com/cams-data/dagster-cube-dbt/blob/main/python_modules/dagster-cube-dbt-tests/src/dagster_cube_dbt_tests/defs/dbt_cubes/defs.yaml) —
  the component itself, scoped to mart-layer models via `cube_select`.
- [`defs/dbt_cubes/patches/`](https://github.com/cams-data/dagster-cube-dbt/tree/main/python_modules/dagster-cube-dbt-tests/src/dagster_cube_dbt_tests/defs/dbt_cubes/patches) —
  four merge-patch files: adding measures/joins to a dbt-derived cube
  (`journey_samples.yaml`), removing a dimension (`remove_journey_type.yaml`), a wholly
  hand-written cube with no backing dbt model (`exchange_rates.yaml`), and a `views:` entry
  composing two cubes (`journeys_overview_view.yaml`) — one concept per file, though nothing
  stops you from combining them.
- [`defs/cube_promoter.py`](https://github.com/cams-data/dagster-cube-dbt/blob/main/python_modules/dagster-cube-dbt-tests/src/dagster_cube_dbt_tests/defs/cube_promoter.py) —
  a plain Python defs module (not YAML) binding `LocalFileCubeFilePromoter` under
  `cube_file_promoter`, the pattern any real `CubeFilePromoter` implementation follows.

## Development

```
uv run pytest tests/
```

runs the suite against dbt-core (via `dbt-duckdb` in `dependency-groups.dev`). The same suite
also runs against the [dbt Fusion engine](https://docs.getdbt.com/guides/fusion) — Fusion
can't be installed alongside dbt-core in the same environment (it's published as a
pre-release of a distribution literally named `dbt`, colliding with dbt-core's own console
script), so it's driven through separate [Hatch](https://hatch.pypa.io) matrix environments
instead:

```
hatch run test.core:run     # dbt-core (same as `uv run pytest tests/`)
hatch run test.fusion:run   # dbt Fusion
```

Both must pass for any change touching dbt-manifest reading, schema.yml conventions used by
the fixture, or anything that shells out to the dbt CLI.

## Releasing

Versioning, [CHANGELOG.md](https://github.com/cams-data/dagster-cube-dbt/blob/main/python_modules/dagster-cube-dbt/CHANGELOG.md), git tags, and GitHub Releases are all generated
automatically by [python-semantic-release](https://python-semantic-release.readthedocs.io/)
from [Conventional Commits](https://www.conventionalcommits.org/) — nobody hand-edits the
version or the changelog. This means every commit message on `main`/`next` actually matters:

- `fix: ...` → patch release (`0.1.0` → `0.1.1`)
- `feat: ...` → minor release (`0.1.1` → `0.2.0`)
- `feat!: ...` / a `BREAKING CHANGE:` footer → normally a major release, but while this
  library is still `0.x` (`major_on_zero = false` in `pyproject.toml`), breaking changes bump
  the minor version instead — 1.0 is meant to be a deliberate milestone, not an accidental
  side effect of a commit message.
- Anything else (`chore:`, `docs:`, `refactor:`, `test:`, ...) → no release.

Pushing to `main` publishes the resulting version straight to PyPI. Pushing to `next` instead
publishes a **prerelease** (`0.2.0rc1`, `0.2.0rc2`, ...) to the same PyPI project — safe by
construction, since `pip install dagster-cube-dbt` never resolves to a prerelease on its own;
trying one requires `pip install --pre dagster-cube-dbt` or an exact version pin. Merge `next`
into `main` when a preview is ready to become the real release. See
[`.github/workflows/release.yml`](https://github.com/cams-data/dagster-cube-dbt/blob/main/.github/workflows/release.yml).

Publishing itself uses PyPI's [Trusted Publishing](https://docs.pypi.org/trusted-publishers/)
(OIDC) — the workflow authenticates directly to PyPI with a short-lived token scoped to this
exact repo/workflow/environment, and no PyPI API token is stored as a repo secret at all. This
needs a one-time, manual setup on PyPI's side that only a maintainer with a PyPI account can
do (not something CI or an agent can do on your behalf):

1. In the target GitHub repo, create an **Environment** named `pypi` (Settings → Environments
   → New environment). Optionally require a reviewer on it for extra protection on real
   releases.
2. On PyPI, go to [Publishing → Add a new pending publisher](https://pypi.org/manage/account/publishing/)
   (works even before the project exists yet — the first successful publish from this workflow
   creates it) and register:
   - PyPI Project Name: `dagster-cube-dbt`
   - Owner: `cams-data`
   - Repository name: `dagster-cube-dbt`
   - Workflow name: `release.yml`
   - Environment name: `pypi`

After that, every push to `main`/`next` that contains a releasable commit publishes itself —
no further manual steps, aside from the release GitHub App setup below (needed once `main`/
`next` are ruleset-protected).

### Letting the release bot push past branch protection

Once a repository ruleset requiring PRs is active on `main`/`next`, the default
`GITHUB_TOKEN` the `release` job used to push with is blocked too: it authenticates as
`github-actions[bot]`, and the ruleset has no way to tell that push apart from an accidental
direct one. Rather than exempt that generic bot identity outright, or fall back to a personal
access token (ties the pipeline to one person's account, a long-lived secret, and stops working
if that account changes), `release.yml` mints a short-lived token from a dedicated GitHub App
instead -- its own distinct bot identity, explicitly exempted, expiring in about an hour rather
than sitting in secrets indefinitely. One-time setup, manual (only a maintainer with admin
access can do this):

1. Create a GitHub App (Settings → Developer settings → GitHub Apps → New GitHub App) with
   **Contents: Read and write** repository permission (covers both pushing commits and
   creating GitHub Releases via the API -- no other permission needed) and no webhook.
2. Install it on this repository only.
3. On the App's settings page, **Generate a private key** (downloads a `.pem` file).
4. Store the App's **Client ID** as a repo **variable** named `RELEASE_APP_CLIENT_ID`, and the
   private key's full contents -- including the `-----BEGIN`/`-----END` lines -- as a repo
   **secret** named `RELEASE_APP_PRIVATE_KEY`. Use the **Client ID**, not the numeric App ID --
   `actions/create-github-app-token`'s `app-id` input is deprecated in favor of `client-id`,
   and in this project's own setup, using `app-id` (with a real, already-generated private key)
   produced a `401 Integration must generate a public key` error that `client-id` didn't --
   confirmed by fixing it, not fully explained by GitHub's error message, which points at a
   missing key even though one existed.
5. Add the App to the ruleset's bypass list (Settings → Rules → Rulesets → the ruleset
   targeting `main`/`next` → Bypass list → Apps), with bypass mode **Exempt** -- "Always"
   still evaluates the ruleset as an interactive "break glass" confirmation, which a
   non-interactive `git push` from CI has no way to respond to.

## Documentation

The doc site (`mkdocs.yml`, `docs/`, both at the repo root -- outside this package directory,
since `mkdocstrings` needs a repo-root-relative view to point at `src/` correctly) is built
with [MkDocs](https://www.mkdocs.org/) + [Material](https://squidfunk.github.io/mkdocs-material/)
+ [mkdocstrings](https://mkdocstrings.github.io/), hosted on
[Read the Docs](https://readthedocs.org/). `docs/index.md` and `docs/changelog.md` aren't
hand-written -- they pull in this README and `CHANGELOG.md` verbatim at build time (via
`pymdownx.snippets`), so there's exactly one source of truth for the prose content; only
`docs/reference.md` (the API reference, generated from this library's own docstrings) has
real content of its own. Build locally with:

```
uv run --project python_modules/dagster-cube-dbt --extra docs mkdocs build --strict
```

(run from the repo root -- `pymdownx.snippets`' `base_path` resolves relative to the working
directory `mkdocs` is invoked from, not `mkdocs.yml`'s location). `--strict` fails the build on
any broken link, anchor, or snippet reference; CI runs exactly this on every PR (`docs` job in
`ci.yml`).

One-time, manual setup on Read the Docs' side (only a maintainer with an RTD account can do
this):

1. Sign in to [readthedocs.org](https://readthedocs.org/) with GitHub and import this repo as
   a new project. RTD auto-detects `.readthedocs.yaml` at the repo root.
2. Confirm the project slug is `dagster-cube-dbt` (or update the badge URLs in this README and
   `mkdocs.yml`'s `site_url` if it ends up different).

After that, every push triggers a doc rebuild automatically, and RTD serves versioned docs
(`latest`, plus one per released tag) without further configuration.
