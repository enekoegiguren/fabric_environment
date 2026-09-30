"""
Step 0 -- fill workspaces_<target>.json from Fabric instead of copying ids by hand.

Lists every workspace the identity can see (GET /v1/workspaces), keeps the
ones whose name starts with the target prefix (dev_ / pro_ by default) and
prints them as:
  - "name = id" lines, ready to paste into the Custom Environment Builder
  - the JSON for workspaces_<target>.json (written to disk with --write)

Usage:
    # Your user (browser login): every workspace you have access to
    python deploy/list_workspaces.py --target dev --target pro

    # Write deploy/workspaces_dev.json and workspaces_pro.json
    python deploy/list_workspaces.py --target dev --target pro --write

    # Only the workspaces of one company / project
    python deploy/list_workspaces.py --target dev --prefix dev_insidemicrosoftfabric_ws_

    # With the Service Principal: shows exactly the workspaces deploy.py can reach
    python deploy/list_workspaces.py --target dev --spn
"""

import argparse
import json
import re
from typing import Dict, List

from fabric_api import CONFIG_DIR, TARGETS, FabricClient, get_credential


def list_workspaces(client: FabricClient) -> List[dict]:
    """Every workspace the identity can see, following continuationUri pages. Skips 'My workspace'."""
    items, url = [], "/workspaces"
    while url:
        body = client.get(url, context="list workspaces").json()
        items += body.get("value", [])
        url = body.get("continuationUri")
    return [w for w in items if w.get("type", "Workspace") == "Workspace"]


def select(workspaces: List[dict], prefix: str, pattern: str = "") -> Dict[str, str]:
    """name -> id for the workspaces matching prefix (case-insensitive) and the optional regex, sorted by name."""
    rx = re.compile(pattern, re.IGNORECASE) if pattern else None
    picked = {w["displayName"]: w["id"] for w in workspaces
              if w["displayName"].lower().startswith(prefix.lower())
              and (rx is None or rx.search(w["displayName"]))}
    return dict(sorted(picked.items()))


def write_target(target: str, selected: Dict[str, str]) -> None:
    path = CONFIG_DIR / f"workspaces_{target}.json"
    old = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    added = sorted(set(selected) - set(old))
    removed = sorted(set(old) - set(selected))
    changed = sorted(k for k in set(old) & set(selected) if old[k] != selected[k])
    path.write_text(json.dumps(selected, indent=2) + "\n", encoding="utf-8")
    print(f"[+] Wrote {path.name}: {len(selected)} workspaces")
    for label, names in (("added", added), ("removed", removed), ("id changed", changed)):
        if names:
            print(f"    {label}: {', '.join(names)}")
    if removed:
        print(f"    [i] Remove them from 'compute' in environment_{target}.jsonc too, or --validate-only fails.")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--target", choices=TARGETS, action="append", required=True,
                        help="Repeatable: --target dev --target pro")
    parser.add_argument("--prefix", help="Name prefix to keep (default: '<target>_'). "
                                         "With several targets, '{target}' is replaced, e.g. '{target}_contoso_ws_'")
    parser.add_argument("--match", default="", help="Extra regex the name must contain, e.g. '_ws_'")
    parser.add_argument("--write", action="store_true", help="Overwrite deploy/workspaces_<target>.json")
    parser.add_argument("--spn", action="store_true",
                        help="Use the Service Principal (FABRIC_* env vars) instead of your user")
    args = parser.parse_args()

    client = FabricClient(get_credential(interactive=not args.spn))
    workspaces = list_workspaces(client)
    print(f"[i] {len(workspaces)} workspaces visible to {'the Service Principal' if args.spn else 'your user'}")

    empty = []
    for target in args.target:
        prefix = (args.prefix or "{target}_").replace("{target}", target)
        selected = select(workspaces, prefix, args.match)
        print(f"\n=== {target}: names starting with '{prefix}'{f' and matching /{args.match}/' if args.match else ''} ===")
        if not selected:
            print("    [!] None found. Check --prefix / --match, and that this identity is a member of them.")
            empty.append(target)
            continue

        print("\n# Paste into the Custom Environment Builder:")
        for name, ws_id in selected.items():
            print(f"{name} = {ws_id}")
        if args.write:
            print()
            write_target(target, selected)
        else:
            print(f"\n# workspaces_{target}.json (add --write to save it):")
            print(json.dumps(selected, indent=2))

    if empty:
        raise SystemExit(f"\nNo workspaces for: {', '.join(empty)}")


if __name__ == "__main__":
    main()
