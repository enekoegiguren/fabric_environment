# fabric_environment

A custom Microsoft Fabric **Environment**, deployed as code. One item per workspace, set as the workspace default, that configures five things for every notebook:

1. **Runtime**: 1.3 (Spark 3.5) or 2.0 (Spark 4), per stage.
2. **Spark properties**: set once, applied when every session starts.
3. **Spark pool and compute**: node size and executors per workspace, small in dev and sized for the load in production.
4. **Native Execution Engine (NEE)**: on for every runtime, because it's off by default on 1.3 *and* 2.0.
5. **Your enterprise functions**: shared code packaged as a versioned Python library (`.whl`), imported in one line.

Points 1 to 4 are configuration and live in `deploy/environment_<stage>.jsonc`. Point 5 is code and lives in `src/fabric_utils`. All five go from dev to production through Azure DevOps.

This is the deliverable for **Build · Module 4 · Part 1: Custom Environment** on [Inside Microsoft Fabric](https://insidemicrosoftfabric.com/build/04-pyspark-notebooks/01-custom-environment/). The **[Custom Environment Builder](https://insidemicrosoftfabric.com/tools/custom-environment/)** generates the config files in `deploy/` (points 1 to 4) for your own workspaces. Point 5 is your own code, so it's written here, in the repo.

## What it replaces

| Before | After |
|---|---|
| `%run Packages`, `%run Connections`, `%run Functions` at the top of every notebook | `from fabric_utils import *` |
| `spark.conf.set(...)` copied into notebooks | `sparkProperties` in the Environment, applied when each session starts |
| NEE enabled (or forgotten) by hand, per workspace | `"spark.native.enabled": "true"` in the config, on every runtime |
| `env = "dev"` edited by hand before promoting a notebook | Variable Library resolves `dev` / `pro` per workspace |
| Environment changes made in the UI, nobody knows what's live | Git history + pipeline with approval for production |

## Structure

```
fabric_environment/
  pyproject.toml                 # the wheel: fabric-enterprise-utils -> import fabric_utils
  src/fabric_utils/
    connections.py               # get_env(), get_paths(): workspaces + OneLake paths per env
    spark_config.py              # check_environment(), session_config(), native_disabled()
    load_save_data.py            # load_data(), save_table()
    schema.py                    # print_schema(), enforce_destination_schema()
    transform.py                 # rename_columns(), trim_string_columns(), deduplicate()...
    loggers.py                   # NotebookLogger -> control lakehouse
    business.py                  # add_surrogate_key(): template for your business rules
  deploy/
    fabric_api.py                # shared: auth, REST client, config validation
    dependencies.py              # shared: wheel dependencies vs each runtime's Python
    check_dependencies.py        # proposes (and applies) compatible versions in pyproject.toml
    list_workspaces.py           # fills workspaces_*.json from Fabric (names + ids)
    bootstrap_environments.py    # once per workspace, with your user
    deploy.py                    # every release, with the Service Principal (+ runtime changes)
    environment_dev.jsonc        # names, runtime, Spark properties, compute (documented)
    environment_pro.jsonc
    workspaces_dev.json          # workspace name -> workspace id
    workspaces_pro.json
    examples/environment_runtime_2_0.jsonc
  notebooks/
    nb_check_environment.ipynb   # run after each deploy
    nb_silver_customers.ipynb    # a Silver notebook using the library
  azure-pipelines.yml            # Build -> DeployDev -> (approval) -> DeployPro
```

## What lives where

| # | What | Scope | Defined in | Seen in Fabric |
|---|---|---|---|---|
| 1 | Runtime | Per stage (dev / pro) | `runtimeVersion` | Environment > Runtime |
| 2 | Spark properties | Per stage | `sparkProperties` | Environment > Spark compute > Spark properties |
| 3 | Spark pool and compute | Per workspace | `compute.<workspace>` | Environment > Spark compute > Compute |
| 4 | NEE | Per stage | `"spark.native.enabled": "true"` in `sparkProperties` | Environment > Spark compute > Acceleration |
| 5 | Enterprise functions (`.whl`) | Every workspace | `src/fabric_utils` + `version` in `pyproject.toml` | Environment > Custom libraries |
| | Current env (`dev` / `pro`) | Per workspace | Variable Library value set | `vl_environment` item |

## 1. Runtime

`runtimeVersion` is `"1.3"` (Spark 3.5, Delta 3.2, GA) or `"2.0"` (Spark 4, Delta 4, public preview). It can differ per stage, e.g. 2.0 in dev while you validate and 1.3 in production.

On the first deploy the Environment is still empty, so `deploy.py` sets the runtime from the file. After that it only **checks** it by default: once a library is published, changing the runtime directly fails with `LibraryManagementError`, so the change needs `deploy.py --allow-runtime-change`, run by hand (see [step 6 of the setup](#6-change-runtime-one-off)).

## 2. Spark properties

`sparkProperties` is the **complete list**: a property added by hand in the UI is removed on the next deploy. Values are always strings (`"true"`, not `true`). What you need depends on the runtime, because 2.0 turns on several Delta optimizations that are opt-in on 1.3:

| Property | Runtime 1.3 | Runtime 2.0 |
|---|---|---|
| `spark.native.enabled` (NEE) | **Set it** | **Set it**: still off by default |
| `spark.databricks.delta.autoCompact.enabled` | **Set it** | **Set it** |
| `spark.microsoft.delta.targetFileSize.adaptive.enabled` | Set it | On by default |
| `spark.microsoft.delta.optimize.fast.enabled` | Set it | On by default |
| `spark.microsoft.delta.optimize.fileLevelTarget.enabled` | Set it | On by default |
| `spark.databricks.delta.properties.defaults.enableDeletionVectors` | Set it | On by default |
| `spark.microsoft.delta.snapshot.driverMode.enabled` | Set it | On by default |

Runtime 2.0 is in public preview: re-check defaults against Microsoft Learn before moving production to it. The comments in `environment_dev.jsonc` list other common properties (resource profile, V-Order, timezone), and `deploy/examples/environment_runtime_2_0.jsonc` shows a 2.0 file.

A config that only one notebook or one read needs stays in code, scoped so it doesn't leak into the rest of the session:

```python
with session_config(spark, spark__sql__parquet__datetimeRebaseModeInRead="LEGACY"):
    df_legacy = spark.read.parquet(paths["path_bronze_files"] + "legacy/")
```

## 3. Spark pool and compute

`compute` has one entry per workspace (key = name in `workspaces_<stage>.json`). `{}` leaves that workspace as it is.

```jsonc
"dev_insidemicrosoftfabric_ws_silver": {
  "instancePool": {"name": "Starter Pool", "type": "Workspace"},
  "driverCores": 4, "driverMemory": "28g", "executorCores": 4, "executorMemory": "28g",
  "dynamicExecutorAllocation": {"enabled": true, "minExecutors": 1, "maxExecutors": 2}
}
```

- `instancePool` is the Starter Pool or a custom pool that already exists (`"type": "Workspace"` or `"Capacity"`).
- Cores and memory go in pairs: 4/28g, 8/56g, 16/112g, 32/224g, 64/400g, and can't exceed the pool's node size. The Starter Pool uses Medium nodes (8 cores / 56g), so anything bigger needs a custom pool.
- Requires *Customize compute configurations for items* ON in the workspace's Spark settings.

## 4. Native Execution Engine

NEE runs Spark SQL and DataFrame operations in native, vectorized code instead of the JVM. It's **off by default on every runtime**, so both example files set `"spark.native.enabled": "true"`.

- **Check it**: `check_environment(spark)` prints the effective value and warns if it's off. The Spark UI's *Queries for Native Engine* tab shows which operators ran natively (green) and which fell back to the JVM (light blue).
- **If one query regresses**, don't turn it off for everyone. Wrap that block:

```python
with native_disabled(spark):
    save_table(df_problematic, paths["path_silver"], "customers_history")
```

## 5. Your enterprise functions

The shared code that used to live in `%run Functions` notebooks goes in `src/fabric_utils` and ships as a wheel: `fabric-enterprise-utils` as the file, `import fabric_utils` in notebooks. It's the one point the Custom Environment Builder doesn't generate, because it's your code, not configuration.

**Why a wheel and not a notebook:**

- **Versioned**: every release has a number (`version` in `pyproject.toml`), and `check_environment()` shows which one is live in each workspace.
- **Reviewed and tested**: every change goes through a pull request, and the pipeline builds the wheel before anything reaches Fabric.
- **Same code everywhere**: the Environment installs the same wheel in every workspace, and production only gets it after an approval. No copies drifting between workspaces.
- **No hidden state**: a notebook can't quietly depend on a variable another `%run` notebook defined.

**What goes in:** anything more than one notebook uses: paths, read/write, schema enforcement, logging, alerts, and business rules such as surrogate keys. **What stays out:** the transformation logic for one specific table, which stays in its notebook.

### Add a function

1. Write it in the module where it belongs, or in a new one. `business.py` is the template for business rules:

   ```python
   # src/fabric_utils/business.py
   def add_surrogate_key(df, keys, name="sk"):
       parts = [F.coalesce(F.col(k).cast("string"), F.lit("<null>")) for k in keys]
       return df.withColumn(name, F.xxhash64(*parts))
   ```

2. Import it in `src/fabric_utils/__init__.py`, so `from fabric_utils import *` exposes it:

   ```python
   from fabric_utils.business import add_surrogate_key
   ```

3. Bump `version` in `pyproject.toml` and `__version__` in `__init__.py` (e.g. `0.1.0` -> `0.2.0`).
4. Open a pull request. The pipeline builds the new wheel, deploys it to dev and, after approval, to production.

Keep `dependencies` in `pyproject.toml` short: Fabric installs each one from PyPI when the Environment publishes. PySpark, Delta, pandas and `notebookutils` are already in the runtime.

### Dependencies and the runtime's Python

Each runtime has its own Python: **1.3 -> Python 3.11**, **2.0 -> Python 3.13** (`RUNTIME_PYTHON` in `deploy/fabric_api.py`). If a dependency doesn't support it, the publish fails with `Spark_User_LPS_PIP_PACKAGE_NOT_FOUND`, e.g. `semantic-link-labs==0.14.3` only supports Python < 3.13.

```bash
python deploy/check_dependencies.py            # checks dev + pro, proposes and asks before changing anything
python deploy/check_dependencies.py --yes      # apply the proposal without asking
python deploy/check_dependencies.py --check    # report only (exit 1 if incompatible), what the pipeline runs
```

It looks up every dependency on PyPI and, if one can't be installed on a target's Python, proposes the **minimum** later version that works on all of them (the same wheel goes to dev and pro). If you accept, it updates `pyproject.toml` and bumps the patch version (and `__version__`). Then rebuild the wheel and deploy.

`deploy.py` runs the same check on the wheel before touching Fabric and stops if something doesn't fit, so a runtime change never leaves the Environment empty because of a publish that was going to fail. `--skip-dependency-check` turns it off (e.g. without PyPI access).

Only direct dependencies are checked: Fabric's publish is still the final check, and a version jump can bring API changes (test what uses that library in dev).

### Use it in a notebook

No `%run`, no `%pip install`. The Environment is the workspace default, so the library is already installed:

```python
from fabric_utils import *

paths = get_paths()                      # "dev" or "pro", from the Variable Library
log = NotebookLogger("nb_silver_customers", layer="silver")

df = load_data(paths["path_bronze"], "customers")
df = deduplicate(trim_string_columns(df), keys=["customer_id"], order_by="modified_at")
df = add_surrogate_key(df, keys=["customer_id"], name="sk_customer")
save_table(df, paths["path_silver"], "customers")
log.end()
```

The full example is `notebooks/nb_silver_customers.ipynb`.

**Trade-off:** sessions on an Environment with custom libraries don't get the Starter Pool's near-instant start, because the library is installed first (about a minute or more instead of a few seconds). For scheduled pipelines it rarely matters.

## Setup

### 0. Prerequisites

- **A Variable Library `vl_environment` in every workspace** with a String variable `environment`: `"dev"` in the default value set (active in dev workspaces) and `"pro"` in a value set called `pro` (active in production). `get_paths()` depends on it. If you don't have it yet, `bootstrap_environments.py` creates it (step 2). It's part of the platform architecture ([Module 2](https://insidemicrosoftfabric.com/build/02-architecture/)).
- Python 3.11+ locally: `pip install build azure-identity requests packaging`
- A Service Principal (app registration) with **Contributor** on every workspace in `workspaces_*.json`, and the tenant setting *Service principals can use Fabric APIs* enabled for it.
- In each workspace: **Workspace settings > Data Engineering/Science > Spark settings > Pool > Customize compute configurations for items** ON (only needed if you use `compute`).

### 1. Fill in your workspaces and config

Use the [Custom Environment Builder](https://insidemicrosoftfabric.com/tools/custom-environment/) to generate them, or edit by hand:

- `deploy/workspaces_dev.json` / `workspaces_pro.json`: name -> workspace id. Don't copy the ids by hand, let `list_workspaces.py` read them from Fabric (below).
- `deploy/environment_dev.jsonc` / `environment_pro.jsonc`: names, runtime, Spark properties, compute.
- `src/fabric_utils/connections.py`: your workspace and lakehouse names in `LAYERS`.

Get the workspace names and ids from Fabric (browser login with your user). It keeps the workspaces whose name starts with `dev_` / `pro_`:

```bash
python deploy/list_workspaces.py --target dev --target pro            # print: paste into the Builder
python deploy/list_workspaces.py --target dev --target pro --write    # or write workspaces_*.json directly

# Narrow it down, e.g. only the layer workspaces of one company:
python deploy/list_workspaces.py --target dev --target pro --prefix "{target}_insidemicrosoftfabric_ws_" --write

# With --spn it uses the Service Principal: what it lists is exactly what deploy.py can reach
python deploy/list_workspaces.py --target pro --spn
```

`--write` prints what was added, removed or changed compared with the existing file. If a workspace is removed, remove it from `compute` in the `.jsonc` too.

Check the files without touching Fabric:

```bash
python deploy/deploy.py --target dev --validate-only
python deploy/deploy.py --target pro --validate-only
```

### 2. Bootstrap (once, with your user)

```bash
python deploy/bootstrap_environments.py --target dev --target pro
```

Creates the `Utils` folder, the empty Environment and the Variable Library in every workspace. It's done with your user because creating items with a Service Principal can return `403 FeatureNotAvailable`.

### 3. Build and deploy

```bash
python -m build --wheel          # dist/fabric_enterprise_utils-0.1.0-py3-none-any.whl

export FABRIC_TENANT_ID=...      # PowerShell: $env:FABRIC_TENANT_ID = "..."
export FABRIC_CLIENT_ID=...
export FABRIC_CLIENT_SECRET=...

python deploy/deploy.py --target dev --whl dist/fabric_enterprise_utils-0.1.0-py3-none-any.whl
```

Per workspace: runtime check -> replace wheel -> stage Spark properties + compute -> publish (3-6 min) -> set as workspace default -> activate the value set.

On the first deploy the Environment is still empty, so deploy.py sets `runtimeVersion` from the file (1.3 or 2.0) before uploading. After that, changing the runtime needs `--allow-runtime-change` (step 6).

### 4. Verify

Import `notebooks/nb_check_environment.ipynb` into a workspace and run it. It prints the runtime, the library version and every key property, and flags NEE if it's off.

### 5. Automate with Azure DevOps

1. **Pipelines > Library**: variable group `fabric-deploy-secrets` with `FABRIC_TENANT_ID`, `FABRIC_CLIENT_ID`, `FABRIC_CLIENT_SECRET` (secret). Linking it to Azure Key Vault is better.
2. **Pipelines > Environments**: create `fabric-dev` and `fabric-pro`; on `fabric-pro` add **Approvals and checks > Approvals**.
3. **Pipelines > New pipeline** -> this repo -> existing `azure-pipelines.yml`.

```
push to main (src/, deploy/, pyproject.toml)
   -> Build: validate config + build wheel
   -> DeployDev: automatic
   -> [approval on fabric-pro]
   -> DeployPro
```

Bump `version` in `pyproject.toml` (and `__version__` in `src/fabric_utils/__init__.py`) on every release, so `check_environment()` shows which version is live.

### 6. Change runtime (one-off)

1. Set `runtimeVersion` in `environment_<target>.jsonc` and review `sparkProperties` (see `deploy/examples/environment_runtime_2_0.jsonc`).
2. `python deploy/check_dependencies.py` and, if it changed anything, `python -m build --wheel` (see [Dependencies and the runtime's Python](#dependencies-and-the-runtimes-python)).
3. `python deploy/deploy.py --target dev --whl dist/<wheel>.whl --allow-runtime-change`
4. Test the notebooks in dev, then repeat with `--target pro`.

It runs Microsoft's sequence to avoid `LibraryManagementError` (remove libraries -> publish empty -> change runtime) and then carries on with the normal deploy (upload the wheel, stage Spark properties + compute, publish). One command, two publishes.

- While it runs the Environment is published **empty** for a few minutes: a notebook starting then won't have `fabric_utils`. Run it when nothing is scheduled.
- It removes **every** custom library in the Environment, not just this one, and only uploads the wheel again. It lists what it removes.
- If it stops halfway, re-run it: with the Environment already empty the runtime change is direct, and workspaces already migrated are left alone.
- The pipeline never passes the flag: if `runtimeVersion` changes in the file before you've migrated by hand, the pipeline's deploy fails on the runtime check instead of emptying an Environment unattended.

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `403 FeatureNotAvailable` creating an item | SPN can't create items in your tenant | Run `bootstrap_environments.py` (uses your user) |
| `Environment is on runtime X, config says Y` | Runtime changed in the file or the UI | `deploy.py --allow-runtime-change`, or fix the file |
| `LibraryManagementError` | Runtime changed with a library published | Use `deploy.py --allow-runtime-change` |
| `Spark_User_LPS_PIP_PACKAGE_NOT_FOUND` on publish | A dependency doesn't support the runtime's Python | `python deploy/check_dependencies.py`, rebuild, deploy |
| `publish operation in progress` | Someone else is publishing | deploy.py skips that workspace; re-run later |
| Property added in the UI disappeared | `sparkProperties` is declarative | Add it to the `.jsonc` file |
| `Could not read 'environment' from the Variable Library` | No `vl_environment` in that workspace | Run bootstrap for that workspace |
