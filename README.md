# fabric_environment

A custom Microsoft Fabric **Environment**, deployed as code: one shared Python library, the Spark properties (Native Execution Engine included) and the compute for every workspace, from dev to production through Azure DevOps.

This is the deliverable for **Build · Module 4 · Part 1: Custom Environment** on [Inside Microsoft Fabric](https://insidemicrosoftfabric.com/build/04-pyspark-notebooks/01-custom-environment/). The **[Custom Environment Builder](https://insidemicrosoftfabric.com/tools/custom-environment/)** tool generates the config files in `deploy/` for your own workspaces.

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
  deploy/
    fabric_api.py                # shared: auth, REST client, config validation
    bootstrap_environments.py    # once per workspace, with your user
    deploy.py                    # every release, with the Service Principal
    migrate_runtime.py           # one-off: change runtime (1.3 -> 2.0)
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

| What | Scope | Defined in | Seen in Fabric |
|---|---|---|---|
| Library (`.whl`) | Every workspace | `src/fabric_utils` | Environment > Custom libraries |
| Runtime | Per target (dev / pro) | `runtimeVersion` | Environment > Runtime |
| Spark properties | Per target | `sparkProperties` | Environment > Spark compute > Spark properties |
| Compute | Per workspace | `compute.<workspace>` | Environment > Spark compute > Compute |
| Current env (`dev` / `pro`) | Per workspace | Variable Library value set | `vl_environment` item |

## Runtime 1.3 vs 2.0: which Spark properties you need

| Property | Runtime 1.3 | Runtime 2.0 |
|---|---|---|
| `spark.native.enabled` (NEE) | **Set it** | **Set it**: still off by default |
| `spark.databricks.delta.autoCompact.enabled` | **Set it** | **Set it** |
| `spark.microsoft.delta.targetFileSize.adaptive.enabled` | Set it | On by default |
| `spark.microsoft.delta.optimize.fast.enabled` | Set it | On by default |
| `spark.microsoft.delta.optimize.fileLevelTarget.enabled` | Set it | On by default |
| `spark.databricks.delta.properties.defaults.enableDeletionVectors` | Set it | On by default |
| `spark.microsoft.delta.snapshot.driverMode.enabled` | Set it | On by default |

Runtime 2.0 is in public preview: re-check defaults against Microsoft Learn before moving production to it.

## Setup

### 0. Prerequisites

- Python 3.10+ locally: `pip install build azure-identity requests`
- A Service Principal (app registration) with **Contributor** on every workspace in `workspaces_*.json`, and the tenant setting *Service principals can use Fabric APIs* enabled for it.
- In each workspace: **Workspace settings > Data Engineering/Science > Spark settings > Pool > Customize compute configurations for items** ON (only needed if you use `compute`).

### 1. Fill in your workspaces and config

Use the [Custom Environment Builder](https://insidemicrosoftfabric.com/tools/custom-environment/) to generate them, or edit by hand:

- `deploy/workspaces_dev.json` / `workspaces_pro.json`: name -> workspace id (from the URL: `.../groups/<workspace-id>/...`).
- `deploy/environment_dev.jsonc` / `environment_pro.jsonc`: names, runtime, Spark properties, compute.
- `src/fabric_utils/connections.py`: your workspace and lakehouse names in `LAYERS`.

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

On the first deploy the Environment is still empty, so deploy.py sets `runtimeVersion` from the file (1.3 or 2.0) before uploading. After that, changing the runtime goes through `migrate_runtime.py`.

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

1. Set `runtimeVersion` in `environment_<target>.jsonc` (see `deploy/examples/environment_runtime_2_0.jsonc`).
2. `python deploy/migrate_runtime.py --target dev --whl dist/<wheel>.whl`
3. Test the notebooks in dev, then repeat with `--target pro`.

It runs Microsoft's sequence (remove libraries -> publish -> change runtime -> upload -> publish) to avoid `LibraryManagementError`. It removes **every** custom library in the Environment, not just this one.

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `403 FeatureNotAvailable` creating an item | SPN can't create items in your tenant | Run `bootstrap_environments.py` (uses your user) |
| `Environment is on runtime X, config says Y` | Runtime changed in the file or the UI | `migrate_runtime.py`, or fix the file |
| `LibraryManagementError` | Runtime changed with a library published | Use `migrate_runtime.py` |
| `publish operation in progress` | Someone else is publishing | deploy.py skips that workspace; re-run later |
| Property added in the UI disappeared | `sparkProperties` is declarative | Add it to the `.jsonc` file |
| `Could not read 'environment' from the Variable Library` | No `vl_environment` in that workspace | Run bootstrap for that workspace |
