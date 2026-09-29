"""
Step 2 -- deploy the fabric_utils wheel + Spark config to every workspace of
a target. Runs with the Service Principal, locally or in Azure Pipelines.

For each workspace in workspaces_<target>.json:
  1. Find the Environment (created by bootstrap_environments.py)
  2. Check its runtime matches runtimeVersion (a fresh, empty Environment
     gets it set here; one with a library published -> migrate_runtime.py)
  3. Replace the old wheel with the new one in staging
  4. Stage sparkProperties + compute.<workspace>, only if something changed
  5. Publish (3-6 min) and wait
  6. Set it as the workspace's default Environment
  7. Activate the Variable Library value set for this target

Usage:
    export FABRIC_TENANT_ID=... FABRIC_CLIENT_ID=... FABRIC_CLIENT_SECRET=...
    python deploy/deploy.py --target dev --whl dist/fabric_enterprise_utils-0.1.0-py3-none-any.whl

    # Validate the config files only (no API calls, no credentials needed):
    python deploy/deploy.py --target dev --validate-only
"""

import argparse
from pathlib import Path

import requests

from fabric_api import (
    TARGETS, FabricClient, find_environment, get_credential, get_spark_compute, list_custom_libraries,
    load_target, patch_spark_compute, publish, remove_libraries, upload_library,
)


def _prop_str(value) -> str:
    return str(value).lower() if isinstance(value, bool) else str(value)


def _props_to_dict(props) -> dict:
    """The GA API returns sparkProperties as [{key, value}], the beta one as a dict."""
    if isinstance(props, dict):
        return {k: _prop_str(v) for k, v in props.items()}
    return {p["key"]: _prop_str(p["value"]) for p in props or []}


def _differs(current, desired) -> bool:
    """Compare only the keys present in desired (the API returns extra fields, e.g. the pool id)."""
    if isinstance(desired, dict):
        return not isinstance(current, dict) or any(_differs(current.get(k), v) for k, v in desired.items())
    return current != desired


def build_compute_patch(current: dict, spark_properties, compute: dict) -> dict:
    """
    sparkProperties is DECLARATIVE: the file is the complete list, so a
    property added by hand in the UI is removed on the next deploy. Remove the
    whole "sparkProperties" key from the file if you don't want deploy.py to
    manage them. compute only touches the keys you set ({} = leave as is).
    """
    patch = {}
    if spark_properties is not None:
        desired = {k: _prop_str(v) for k, v in spark_properties.items()}
        if _props_to_dict(current.get("sparkProperties")) != desired:
            patch["sparkProperties"] = [{"key": k, "value": v} for k, v in desired.items()]
    for key, value in compute.items():
        if _differs(current.get(key), value):
            patch[key] = value
    return patch


def deploy_workspace(client: FabricClient, label: str, ws_id: str, whl: Path, config: dict) -> None:
    print(f"\n=== {label} ({ws_id}) ===")
    name = config["environmentName"]
    env_id = find_environment(client, ws_id, name)
    if not env_id:
        raise RuntimeError(f"Environment '{name}' not found. Run bootstrap_environments.py for this workspace first.")

    current = get_spark_compute(client, ws_id, env_id)
    runtime, wanted = current.get("runtimeVersion"), config["runtimeVersion"]
    if runtime != wanted:
        if list_custom_libraries(client, ws_id, env_id):
            # Changing the runtime with a custom library published fails with
            # LibraryManagementError, so deploy.py never does it.
            raise RuntimeError(f"Environment is on runtime {runtime}, config says {wanted}. Run migrate_runtime.py.")
        # Fresh Environment from bootstrap (no libraries yet): set the runtime
        # before the first upload. Same as steps 3-5 of the migration sequence.
        patch_spark_compute(client, ws_id, env_id, {"runtimeVersion": wanted})
        print(f"    [+] Empty Environment: runtime {runtime} -> {wanted}")
    else:
        print(f"    [=] Runtime {runtime}")

    try:
        # Wheel files are named <dist_name>-<version>-..., so this removes every version of our package only.
        remove_libraries(client, ws_id, env_id, prefix=whl.name.split("-")[0] + "-")
        upload_library(client, ws_id, env_id, whl)
    except requests.HTTPError as e:
        if e.response is not None and "publish operation in progress" in e.response.text.lower():
            print("    [i] A publish is already running in this Environment -- skipping this workspace.")
            return
        raise

    patch = build_compute_patch(current, config.get("sparkProperties"), config.get("compute", {}).get(label, {}))
    if patch:
        patch_spark_compute(client, ws_id, env_id, patch)
        print(f"    [+] Staged: {', '.join(patch)}")
    else:
        print("    [=] Spark properties and compute already up to date")

    publish(client, ws_id, env_id)

    client.patch(f"/workspaces/{ws_id}/spark/settings", json={"environment": {"name": name}},
                 context="set default environment")
    print(f"    [+] '{name}' is the workspace default Environment")

    set_value_set(client, ws_id, config["variableLibrary"])


def set_value_set(client: FabricClient, ws_id: str, vl_config: dict) -> None:
    r = client.get(f"/workspaces/{ws_id}/variableLibraries", context="list variable libraries")
    vl = next((v for v in r.json().get("value", []) if v["displayName"] == vl_config["name"]), None)
    if vl is None:
        print(f"    [!] Variable Library '{vl_config['name']}' not found -- run bootstrap_environments.py")
        return
    wanted = vl_config["activeValueSet"]
    if vl.get("properties", {}).get("activeValueSetName") == wanted:
        print(f"    [=] Active value set: '{wanted}'")
        return
    client.patch(f"/workspaces/{ws_id}/variableLibraries/{vl['id']}",
                 json={"properties": {"activeValueSetName": wanted}}, context="set active value set")
    print(f"    [+] Active value set -> '{wanted}'")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--target", choices=TARGETS, required=True)
    parser.add_argument("--whl", type=Path)
    parser.add_argument("--validate-only", action="store_true", help="Check the config files and exit")
    args = parser.parse_args()

    workspaces, config = load_target(args.target)
    if args.validate_only:
        print(f"[ok] {args.target}: {len(workspaces)} workspaces, runtime {config['runtimeVersion']}, "
              f"{len(config.get('sparkProperties') or {})} Spark properties")
        return
    if not args.whl or not args.whl.exists():
        raise SystemExit(f"Wheel not found: {args.whl} (build it with: python -m build)")

    client = FabricClient(get_credential())
    errors = []
    for label, ws_id in workspaces.items():
        try:
            deploy_workspace(client, label, ws_id, args.whl, config)
        except Exception as e:
            print(f"    [!] ERROR: {e}")
            errors.append(f"{label}: {e}")

    if errors:
        raise SystemExit("\nFailed workspaces:\n  - " + "\n  - ".join(errors))
    print("\nDeploy done.")


if __name__ == "__main__":
    main()
