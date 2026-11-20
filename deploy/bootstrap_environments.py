"""
Step 1 -- run ONCE per workspace, with YOUR user (browser login).

Creates, in every workspace of workspaces_<target>.json:
  - the folder from "folder" (e.g. Utils)
  - an empty Environment named "environmentName"
  - the Variable Library "variableLibrary.name" with one String variable,
    `environment`: "dev" in the default value set, "pro" in a value set called "pro"
    (deploy.py adds `fabric_utils_libs` later if the config has "pythonLibs")

Why a user and not the Service Principal: creating new items with an SPN can
return 403 FeatureNotAvailable, depending on tenant settings. Once the items
exist, deploy.py only updates them, which works fine with the SPN.

Usage:
    python deploy/bootstrap_environments.py --target dev
    python deploy/bootstrap_environments.py --target dev --target pro
"""

import argparse
import base64
import json
import uuid

from fabric_api import (
    TARGETS, FabricClient, ensure_environment, ensure_folder, get_credential, load_target,
)

VL_SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/item/variableLibrary/definition"


def _b64(obj: dict) -> str:
    return base64.b64encode(json.dumps(obj, indent=2).encode("utf-8")).decode("ascii")


def _part(path: str, obj: dict) -> dict:
    return {"path": path, "payload": _b64(obj), "payloadType": "InlineBase64"}


def ensure_variable_library(client: FabricClient, ws_id: str, name: str, folder_id: str) -> None:
    r = client.get(f"/workspaces/{ws_id}/variableLibraries", context="list variable libraries")
    if any(v["displayName"] == name for v in r.json().get("value", [])):
        print(f"    [=] Variable Library '{name}' exists (not modified)")
        return

    # Same format as a Variable Library exported to Git by Fabric.
    variables = {
        "$schema": f"{VL_SCHEMA}/variables/1.0.0/schema.json",
        "variables": [{
            "name": "environment",
            "note": "Current environment. Read by fabric_utils.get_env() / get_paths().",
            "type": "String",
            "value": "dev",
        }],
    }
    value_set_pro = {
        "$schema": f"{VL_SCHEMA}/valueSet/1.0.0/schema.json",
        "name": "pro",
        "variableOverrides": [{"name": "environment", "value": "pro"}],
    }
    settings = {
        "$schema": f"{VL_SCHEMA}/settings/1.0.0/schema.json",
        "valueSetsOrder": ["pro"],
    }
    platform = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
        "metadata": {"type": "VariableLibrary", "displayName": name},
        "config": {"version": "2.0", "logicalId": str(uuid.uuid4())},
    }
    body = {
        "displayName": name,
        "folderId": folder_id,
        "definition": {"parts": [
            _part("variables.json", variables),
            _part("valueSets/pro.json", value_set_pro),
            _part("settings.json", settings),
            _part(".platform", platform),
        ]},
    }
    client.wait(client.post(f"/workspaces/{ws_id}/variableLibraries", json=body, context="create variable library"))
    print(f"    [+] Variable Library '{name}' created (dev = default value set, pro = 'pro')")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--target", choices=TARGETS, action="append", required=True,
                        help="Repeatable: --target dev --target pro")
    args = parser.parse_args()

    client = FabricClient(get_credential(interactive=True))
    errors = []
    for target in args.target:
        workspaces, config = load_target(target)
        for label, ws_id in workspaces.items():
            print(f"\n=== [{target}] {label} ({ws_id}) ===")
            try:
                folder_id = ensure_folder(client, ws_id, config["folder"])
                ensure_environment(client, ws_id, config["environmentName"], folder_id)
                ensure_variable_library(client, ws_id, config["variableLibrary"]["name"], folder_id)
            except Exception as e:
                print(f"    [!] ERROR: {e}")
                errors.append(f"{target}/{label}: {e}")

    if errors:
        raise SystemExit("\nFailed workspaces:\n  - " + "\n  - ".join(errors))
    print("\nBootstrap done. From now on deploy.py (Service Principal) handles everything else.")


if __name__ == "__main__":
    main()
