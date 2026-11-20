"""
Step 2 -- deploy the fabric_utils wheel + Spark config to every workspace of
a target. Runs with the Service Principal, locally or in Azure Pipelines.

Before touching Fabric it checks on PyPI that the wheel's dependencies install
on the runtime's Python (see dependencies.py), and stops if not: a runtime
change never leaves the Environment empty because of a publish that was going
to fail. check_dependencies.py proposes the versions to use.

For each workspace in workspaces_<target>.json:
  1. Find the Environment (created by bootstrap_environments.py)
  2. Runtime: if it doesn't match runtimeVersion
       - Empty Environment (no libraries): set it directly
       - Wheel already published: only with --allow-runtime-change, which runs
         Microsoft's sequence to avoid LibraryManagementError (remove
         libraries -> publish empty -> change runtime -> carry on with the
         normal deploy, which uploads the wheel again and publishes). Without
         the flag it fails before touching anything.
  3. Replace the old wheel with the new one in staging
  4. Stage sparkProperties + compute.<workspace>, only if something changed
  5. Publish (3-6 min) and wait
  6. Set it as the workspace's default Environment
  7. Activate the Variable Library value set for this target

Then, for Python notebooks (they can't use an Environment), if the config has
"pythonLibs":
  8. Upload the wheel to that lakehouse folder (Files/libs/fabric_utils) and
     delete the older versions, so the folder always holds exactly one
  9. Keep the variable fabric_utils_libs in every workspace's Variable Library:
     default value = the dev folder, value set "pro" = the pro folder. Python
     notebooks read it to find the wheel.

Usage:
    export FABRIC_TENANT_ID=... FABRIC_CLIENT_ID=... FABRIC_CLIENT_SECRET=...
    python deploy/deploy.py --target dev --whl dist/fabric_enterprise_utils-0.2.0-py3-none-any.whl

    # Change runtime (edit runtimeVersion in environment_<target>.jsonc first):
    python deploy/deploy.py --target dev --whl dist/fabric_enterprise_utils-0.2.0-py3-none-any.whl --allow-runtime-change

    # Only the wheel for Python notebooks (Environments untouched; seconds instead of minutes):
    python deploy/deploy.py --target dev --whl dist/fabric_enterprise_utils-0.2.0-py3-none-any.whl --python-libs-only

    # Validate the config files only (no API calls, no credentials needed):
    python deploy/deploy.py --target dev --validate-only

--allow-runtime-change is NOT used in azure-pipelines.yml on purpose: during
the migration the Environment is published empty for a few minutes, and any
notebook starting then won't have fabric_utils. Run it by hand, dev first.
"""

import argparse
import json
from pathlib import Path

import requests

from dependencies import check_requirements, format_issues, wheel_requirements
from fabric_api import (
    RUNTIME_PYTHON, TARGETS, FabricClient, OneLakeClient, find_environment, find_variable_library,
    get_credential, get_spark_compute, get_variable_library_definition, list_custom_libraries, load_target,
    patch_spark_compute, publish, python_libs_abfss, remove_libraries, update_variable_library_definition,
    upload_library,
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
        existing = _props_to_dict(current.get("sparkProperties"))
        if existing != desired:
            # The API's PATCH MERGES properties: the ones not sent stay. To remove
            # one it has to be sent with a null value.
            patch["sparkProperties"] = (
                [{"key": k, "value": v} for k, v in desired.items()]
                + [{"key": k, "value": None} for k in existing if k not in desired]
            )
    for key, value in compute.items():
        if _differs(current.get(key), value):
            patch[key] = value
    return patch


def ensure_runtime(client: FabricClient, ws_id: str, env_id: str, current: dict, wanted: str,
                   allow_change: bool) -> dict:
    """
    Leave the staging runtime on `wanted`. Returns the current sparkcompute
    (re-read if something changed). If a run stops halfway, just re-run it:
    with the Environment already empty the change is direct.
    """
    runtime = current.get("runtimeVersion")
    if runtime == wanted:
        print(f"    [=] Runtime {runtime}")
        return current

    libraries = list_custom_libraries(client, ws_id, env_id)
    if libraries:
        if not allow_change:
            raise RuntimeError(
                f"Environment is on runtime {runtime}, config says {wanted}. Changing it with libraries "
                f"published fails with LibraryManagementError: re-run by hand with --allow-runtime-change."
            )
        # Microsoft's sequence removes EVERY library, not just the wheel: list them,
        # because deploy.py only uploads the wheel again.
        print(f"    [i] Migrating runtime {runtime} -> {wanted}. Removing: {', '.join(libraries)}")
        remove_libraries(client, ws_id, env_id)
        print("    [i] Publishing the empty Environment (notebooks starting now won't have fabric_utils)")
        publish(client, ws_id, env_id)

    patch_spark_compute(client, ws_id, env_id, {"runtimeVersion": wanted})
    print(f"    [+] Runtime {runtime} -> {wanted}")
    return get_spark_compute(client, ws_id, env_id)


def deploy_workspace(client: FabricClient, label: str, ws_id: str, whl: Path, config: dict,
                     allow_runtime_change: bool) -> None:
    print(f"\n=== {label} ({ws_id}) ===")
    name = config["environmentName"]
    env_id = find_environment(client, ws_id, name)
    if not env_id:
        raise RuntimeError(f"Environment '{name}' not found. Run bootstrap_environments.py for this workspace first.")

    current = get_spark_compute(client, ws_id, env_id)
    current = ensure_runtime(client, ws_id, env_id, current, config["runtimeVersion"], allow_runtime_change)

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


# --------------------------------------------------------------------------- #
# Python notebooks: wheel in a lakehouse folder + fabric_utils_libs variable
# --------------------------------------------------------------------------- #

LIBS_VARIABLE = "fabric_utils_libs"
LIBS_VARIABLE_NOTE = "Folder with the fabric_utils wheel for Python notebooks (deploy.py > pythonLibs)"
PRO_VALUE_SET = "valueSets/pro.json"


def deploy_python_libs(onelake: OneLakeClient, libs: dict, whl: Path) -> None:
    """
    Put the wheel in the Python notebooks' folder, then delete the older
    versions (after the upload, so the folder is never empty).
    """
    workspace = libs["workspace"]
    folder = f"{libs['lakehouse']}.Lakehouse/{libs['folder'].strip('/')}"
    print(f"\n=== Python notebooks: {workspace}/{folder} ===")
    onelake.upload_file(workspace, f"{folder}/{whl.name}", whl.read_bytes())
    print(f"    [+] Uploaded {whl.name}")
    prefix = whl.name.split("-")[0] + "-"
    for name in onelake.list_files(workspace, folder):
        if name != whl.name and name.startswith(prefix) and name.endswith(".whl"):
            onelake.delete_file(workspace, f"{folder}/{name}")
            print(f"    [-] Deleted {name}")


def _json_bytes(obj: dict) -> bytes:
    return (json.dumps(obj, indent=2, ensure_ascii=False) + "\n").encode("utf-8")


def ensure_libs_variable(client: FabricClient, ws_id: str, vl_name: str, dev_value: str, pro_value: str) -> None:
    """
    fabric_utils_libs in the workspace's Variable Library: dev_value by default,
    pro_value in the "pro" value set. Only writes if it's missing or different.
    """
    vl = find_variable_library(client, ws_id, vl_name)
    if vl is None:
        print(f"    [!] Variable Library '{vl_name}' not found -- {LIBS_VARIABLE} not added")
        return
    parts = get_variable_library_definition(client, ws_id, vl["id"])
    changed = []

    variables = json.loads(parts["variables.json"])
    var = next((v for v in variables.setdefault("variables", []) if v.get("name") == LIBS_VARIABLE), None)
    if var is None:
        variables["variables"].append({"name": LIBS_VARIABLE, "note": LIBS_VARIABLE_NOTE, "type": "String",
                                       "value": dev_value})
        changed.append("new variable")
    elif var.get("value") != dev_value:
        var["value"] = dev_value
        changed.append("default value")

    value_set = None
    if PRO_VALUE_SET in parts:
        value_set = json.loads(parts[PRO_VALUE_SET])
        overrides = value_set.setdefault("variableOverrides", [])
        over = next((o for o in overrides if o.get("name") == LIBS_VARIABLE), None)
        if over is None:
            overrides.append({"name": LIBS_VARIABLE, "value": pro_value})
            changed.append("pro value")
        elif over.get("value") != pro_value:
            over["value"] = pro_value
            changed.append("pro value")
    else:
        print(f"    [!] '{vl_name}' has no 'pro' value set: {LIBS_VARIABLE} only gets the dev value")

    if not changed:
        print(f"    [=] {LIBS_VARIABLE} up to date in '{vl_name}'")
        return
    parts["variables.json"] = _json_bytes(variables)
    if value_set is not None:
        parts[PRO_VALUE_SET] = _json_bytes(value_set)
    update_variable_library_definition(client, ws_id, vl["id"], parts)
    print(f"    [+] {LIBS_VARIABLE} in '{vl_name}' ({', '.join(changed)})")


def sync_libs_variable(client: FabricClient, workspaces: dict, vl_name: str, errors: list) -> None:
    """fabric_utils_libs in every workspace of the target, with the pythonLibs folders of dev and pro."""
    dev_libs = load_target("dev")[1].get("pythonLibs")
    pro_libs = load_target("pro")[1].get("pythonLibs")
    if not dev_libs or not pro_libs:
        print(f"\n[!] pythonLibs missing in environment_dev.jsonc or environment_pro.jsonc: "
              f"{LIBS_VARIABLE} not touched")
        return
    dev_value, pro_value = python_libs_abfss(dev_libs), python_libs_abfss(pro_libs)
    print(f"\n=== Variable {LIBS_VARIABLE} (Variable Library '{vl_name}') ===")
    for label, ws_id in workspaces.items():
        print(f"  {label}")
        try:
            ensure_libs_variable(client, ws_id, vl_name, dev_value, pro_value)
        except Exception as e:
            print(f"    [!] ERROR: {e}")
            errors.append(f"{label} ({LIBS_VARIABLE}): {e}")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--target", choices=TARGETS, required=True)
    parser.add_argument("--whl", type=Path)
    parser.add_argument("--allow-runtime-change", action="store_true",
                        help="Allow migrating the runtime of an Environment with the wheel published (not in the pipeline)")
    parser.add_argument("--validate-only", action="store_true", help="Check the config files and exit")
    parser.add_argument("--skip-dependency-check", action="store_true",
                        help="Don't check the wheel's dependencies on PyPI (e.g. no internet access)")
    parser.add_argument("--python-libs-only", action="store_true",
                        help="Only upload the wheel for Python notebooks (pythonLibs); Environments untouched")
    args = parser.parse_args()

    workspaces, config = load_target(args.target)
    if args.validate_only:
        print(f"[ok] {args.target}: {len(workspaces)} workspaces, runtime {config['runtimeVersion']}, "
              f"{len(config.get('sparkProperties') or {})} Spark properties")
        return
    if not args.whl or not args.whl.exists():
        raise SystemExit(f"Wheel not found: {args.whl} (build it with: python -m build)")

    libs = config.get("pythonLibs")
    if args.python_libs_only:
        if not libs:
            raise SystemExit(f"environment_{args.target}.jsonc has no pythonLibs")
        credential = get_credential()
        errors = []
        deploy_python_libs(OneLakeClient(credential), libs, args.whl)
        sync_libs_variable(FabricClient(credential), workspaces, config["variableLibrary"]["name"], errors)
        if errors:
            raise SystemExit("\nErrors:\n  - " + "\n  - ".join(errors))
        print("\nDeploy done (Python notebooks only).")
        return

    if not args.skip_dependency_check:
        python = RUNTIME_PYTHON[config["runtimeVersion"]]
        print(f"Checking the dependencies of {args.whl.name} for Python {python} "
              f"(runtime {config['runtimeVersion']})...")
        issues = check_requirements(wheel_requirements(args.whl), [python])
        if issues:
            raise SystemExit(f"\n[!] The publish would fail, Fabric not touched:\n{format_issues(issues)}\n\n"
                             f"Fix it with: python deploy/check_dependencies.py  (then rebuild the wheel)")
        print("[ok] Dependencies compatible")

    credential = get_credential()
    client = FabricClient(credential)
    errors = []
    for label, ws_id in workspaces.items():
        try:
            deploy_workspace(client, label, ws_id, args.whl, config, args.allow_runtime_change)
        except Exception as e:
            print(f"    [!] ERROR: {e}")
            errors.append(f"{label}: {e}")

    if libs:
        try:
            deploy_python_libs(OneLakeClient(credential), libs, args.whl)
        except Exception as e:
            print(f"    [!] ERROR in pythonLibs: {e}")
            errors.append(f"pythonLibs: {e}")
        sync_libs_variable(client, workspaces, config["variableLibrary"]["name"], errors)

    if errors:
        raise SystemExit("\nFailed workspaces:\n  - " + "\n  - ".join(errors))
    print("\nDeploy done.")


if __name__ == "__main__":
    main()
