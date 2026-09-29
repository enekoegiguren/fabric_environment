"""
Workspaces and OneLake paths per environment (dev / pro).

The environment is NOT hardcoded in notebooks. Each workspace has a Variable
Library (created by deploy/bootstrap_environments.py) with a variable called
`environment`, and deploy.py activates the right value set per workspace:
dev workspaces resolve "dev", production workspaces resolve "pro". The same
notebook code therefore runs unchanged in both.

Usage in a notebook:
    from fabric_utils import *
    paths = get_paths()            # env auto-detected
    paths = get_paths(env="dev")   # or forced explicitly
"""

from typing import Dict, Optional

# Must match "variableLibrary.name" in deploy/environment_<target>.jsonc.
VARIABLE_LIBRARY = "vl_environment"

_ONELAKE = "abfss://{ws}@onelake.dfs.fabric.microsoft.com/{lh}.Lakehouse/{folder}/"

# One entry per environment and layer: (workspace name, lakehouse name).
# Replace with your own names. These follow the site's naming convention:
# {env}_{company}_ws_{layer} for workspaces, {env}_{company}_{layer}_lh for lakehouses.
LAYERS: Dict[str, Dict[str, tuple]] = {
    "dev": {
        "control": ("dev_insidemicrosoftfabric_ws_control", "dev_insidemicrosoftfabric_control_lh"),
        "bronze": ("dev_insidemicrosoftfabric_ws_bronze", "dev_insidemicrosoftfabric_bronze_lh"),
        "silver": ("dev_insidemicrosoftfabric_ws_silver", "dev_insidemicrosoftfabric_silver_lh"),
        "gold": ("dev_insidemicrosoftfabric_ws_gold", "dev_insidemicrosoftfabric_gold_lh"),
    },
    "pro": {
        "control": ("pro_insidemicrosoftfabric_ws_control", "pro_insidemicrosoftfabric_control_lh"),
        "bronze": ("pro_insidemicrosoftfabric_ws_bronze", "pro_insidemicrosoftfabric_bronze_lh"),
        "silver": ("pro_insidemicrosoftfabric_ws_silver", "pro_insidemicrosoftfabric_silver_lh"),
        "gold": ("pro_insidemicrosoftfabric_ws_gold", "pro_insidemicrosoftfabric_gold_lh"),
    },
}

# Values that are the same in every environment (Key Vault URLs, shared IDs...).
CONSTANTS: Dict[str, str] = {
    "key_vault": "https://kv-insidemicrosoftfabric.vault.azure.net/",
}


def get_env() -> str:
    """
    Read the current environment from the workspace's Variable Library.
    Only works inside a Fabric session; outside Fabric, pass env explicitly.
    """
    try:
        import notebookutils
        return notebookutils.variableLibrary.getLibrary(VARIABLE_LIBRARY).environment
    except Exception as e:
        raise ValueError(
            f"Could not read 'environment' from the Variable Library '{VARIABLE_LIBRARY}'. "
            "Run deploy/bootstrap_environments.py for this workspace, or pass env explicitly: "
            f"get_paths(env='dev'). Original error: {e}"
        )


def get_paths(env: Optional[str] = None) -> Dict[str, str]:
    """
    Return workspace, lakehouse and OneLake paths for every layer.

    For each layer in LAYERS you get:
        workspace_<layer>, lakehouse_<layer>, path_<layer> (Tables/), path_<layer>_files (Files/)
    plus "env" and everything in CONSTANTS.
    """
    env = env or get_env()
    if env not in LAYERS:
        raise ValueError(f"Unknown environment '{env}'. Expected one of: {sorted(LAYERS)}")

    paths: Dict[str, str] = {"env": env}
    for layer, (workspace, lakehouse) in LAYERS[env].items():
        paths[f"workspace_{layer}"] = workspace
        paths[f"lakehouse_{layer}"] = lakehouse
        paths[f"path_{layer}"] = _ONELAKE.format(ws=workspace, lh=lakehouse, folder="Tables")
        paths[f"path_{layer}_files"] = _ONELAKE.format(ws=workspace, lh=lakehouse, folder="Files")
    paths.update(CONSTANTS)
    return paths
