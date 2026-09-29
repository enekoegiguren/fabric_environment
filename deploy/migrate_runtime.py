"""
One-off: move the Environment to another Spark runtime (e.g. 1.3 -> 2.0).
Not part of the normal deploy, and not in the pipeline on purpose.

Changing the runtime of an Environment that has a published custom library
fails with LibraryManagementError, so this follows Microsoft's sequence:
    1. Remove every custom library
    2. Publish (empty Environment)
    3. Change the runtime
    4. Upload the wheel again
    5. Publish again

Usage:
    1. Change runtimeVersion in environment_<target>.jsonc (and review
       sparkProperties: see the runtime notes in that file)
    2. python deploy/migrate_runtime.py --target dev --whl dist/fabric_enterprise_utils-0.1.0-py3-none-any.whl
    3. Test the notebooks in dev, then do the same with --target pro
"""

import argparse
from pathlib import Path

from fabric_api import (
    TARGETS, FabricClient, find_environment, get_credential, get_spark_compute, load_target,
    patch_spark_compute, publish, remove_libraries, upload_library,
)


def migrate_workspace(client: FabricClient, label: str, ws_id: str, config: dict, whl: Path) -> None:
    print(f"\n=== {label} ({ws_id}) ===")
    version = config["runtimeVersion"]
    env_id = find_environment(client, ws_id, config["environmentName"])
    if not env_id:
        raise RuntimeError(f"Environment '{config['environmentName']}' not found")

    if get_spark_compute(client, ws_id, env_id).get("runtimeVersion") == version:
        print(f"    [=] Already on runtime {version}")
        return

    print("    [1/5] Removing custom libraries")
    remove_libraries(client, ws_id, env_id)
    print("    [2/5] Publishing the empty Environment")
    publish(client, ws_id, env_id)
    print(f"    [3/5] Setting runtime {version} (+ sparkProperties from the file)")
    patch = {"runtimeVersion": version}
    if config.get("sparkProperties") is not None:
        patch["sparkProperties"] = [{"key": k, "value": v} for k, v in config["sparkProperties"].items()]
    patch_spark_compute(client, ws_id, env_id, patch)
    print("    [4/5] Uploading the wheel again")
    upload_library(client, ws_id, env_id, whl)
    print("    [5/5] Publishing on the new runtime")
    publish(client, ws_id, env_id)
    print(f"    [ok] {label} is on runtime {version}")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--target", choices=TARGETS, required=True)
    parser.add_argument("--whl", type=Path, required=True)
    args = parser.parse_args()

    if not args.whl.exists():
        raise SystemExit(f"Wheel not found: {args.whl}")
    workspaces, config = load_target(args.target)

    client = FabricClient(get_credential())
    errors = []
    for label, ws_id in workspaces.items():
        try:
            migrate_workspace(client, label, ws_id, config, args.whl)
        except Exception as e:
            print(f"    [!] ERROR: {e}")
            errors.append(f"{label}: {e}")

    if errors:
        raise SystemExit("\nFailed workspaces:\n  - " + "\n  - ".join(errors))
    print("\nRuntime migration done. The next deploy.py run will pass the runtime check.")


if __name__ == "__main__":
    main()
