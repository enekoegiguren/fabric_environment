"""
Shared helpers for bootstrap_environments.py, deploy.py and migrate_runtime.py:
authentication, a thin Fabric REST client (error bodies + long-running
operations), config loading/validation, and the idempotent ensure_* calls.

Requirements:
    pip install azure-identity requests
"""

import json
import os
import time
from pathlib import Path
from typing import Dict, Optional, Tuple

import requests

FABRIC_API = "https://api.fabric.microsoft.com/v1"
SCOPE = "https://api.fabric.microsoft.com/.default"
CONFIG_DIR = Path(__file__).parent
TARGETS = ("dev", "pro")

RUNTIMES = ("1.3", "2.0")
# Node sizes: cores -> memory. Driver/executor must use one of these pairs.
CORE_MEMORY = {4: "28g", 8: "56g", 16: "112g", 32: "224g", 64: "400g"}


# --------------------------------------------------------------------------- #
# Auth + HTTP
# --------------------------------------------------------------------------- #

def get_credential(interactive: bool = False):
    """
    interactive=True  -> your user, via browser (bootstrap: creating items with
                          a Service Principal can fail with 403 FeatureNotAvailable).
    interactive=False -> Service Principal from FABRIC_TENANT_ID / FABRIC_CLIENT_ID /
                          FABRIC_CLIENT_SECRET (deploy + migrate, and the pipeline).
    """
    if interactive:
        from azure.identity import InteractiveBrowserCredential
        return InteractiveBrowserCredential()

    from azure.identity import ClientSecretCredential
    missing = [v for v in ("FABRIC_TENANT_ID", "FABRIC_CLIENT_ID", "FABRIC_CLIENT_SECRET") if not os.environ.get(v)]
    if missing:
        raise SystemExit(f"Missing environment variables: {', '.join(missing)}")
    return ClientSecretCredential(
        tenant_id=os.environ["FABRIC_TENANT_ID"],
        client_id=os.environ["FABRIC_CLIENT_ID"],
        client_secret=os.environ["FABRIC_CLIENT_SECRET"],
    )


class FabricClient:
    """
    Minimal Fabric REST client. A token is requested per call; azure-identity
    caches and refreshes it, so long runs (several 3-6 min publishes) don't
    fail on an expired token.
    """

    def __init__(self, credential):
        self._credential = credential

    def _headers(self, content_type: str = "application/json") -> dict:
        token = self._credential.get_token(SCOPE).token
        return {"Authorization": f"Bearer {token}", "Content-Type": content_type}

    def request(self, method: str, path: str, context: str = "", allow: Tuple[int, ...] = (),
                content_type: str = "application/json", **kwargs) -> requests.Response:
        url = path if path.startswith("http") else f"{FABRIC_API}{path}"
        r = requests.request(method, url, headers=self._headers(content_type), timeout=300, **kwargs)
        if not r.ok and r.status_code not in allow:
            # raise_for_status() alone hides Fabric's error body, which holds the real reason.
            print(f"    [!] HTTP {r.status_code}{f' ({context})' if context else ''}: {r.text}")
            r.raise_for_status()
        return r

    def get(self, path, **kw): return self.request("GET", path, **kw)
    def post(self, path, **kw): return self.request("POST", path, **kw)
    def patch(self, path, **kw): return self.request("PATCH", path, **kw)
    def delete(self, path, **kw): return self.request("DELETE", path, **kw)

    def wait(self, r: requests.Response, timeout_s: int = 1200) -> requests.Response:
        """Follow a 202 + Location long-running operation until it finishes."""
        if r.status_code != 202:
            return r
        op_url = r.headers["Location"]
        delay = int(r.headers.get("Retry-After", "10"))
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            time.sleep(delay)
            status = self.get(op_url).json().get("status")
            if status == "Succeeded":
                res = self.get(op_url + "/result", allow=(404,))
                return res if res.ok else r
            if status in ("Failed", "Cancelled"):
                raise RuntimeError(f"Operation {status}: {self.get(op_url).json()}")
        raise TimeoutError(f"Operation did not finish in {timeout_s}s: {op_url}")


# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #

def load_jsonc(path: Path) -> dict:
    """JSON that allows full-line '//' comments (not trailing ones, so values containing '//' are safe)."""
    lines = path.read_text(encoding="utf-8").splitlines()
    return json.loads("\n".join(l for l in lines if not l.lstrip().startswith("//")))


def load_target(target: str, config_dir: Path = CONFIG_DIR) -> Tuple[Dict[str, str], dict]:
    """Load and validate workspaces_<target>.json + environment_<target>.jsonc."""
    workspaces = json.loads((config_dir / f"workspaces_{target}.json").read_text(encoding="utf-8"))
    config = load_jsonc(config_dir / f"environment_{target}.jsonc")
    errors = validate_config(target, workspaces, config)
    if errors:
        raise SystemExit("Config errors:\n  - " + "\n  - ".join(errors))
    return workspaces, config


def validate_config(target: str, workspaces: Dict[str, str], config: dict) -> list:
    """Catch typos before calling the API. Returns a list of error messages."""
    errors = []
    for key in ("environmentName", "folder", "variableLibrary", "runtimeVersion"):
        if key not in config:
            errors.append(f"environment_{target}.jsonc: missing '{key}'")

    if config.get("runtimeVersion") not in RUNTIMES:
        errors.append(f"runtimeVersion must be one of {RUNTIMES}, got {config.get('runtimeVersion')!r}")

    for label, ws_id in workspaces.items():
        if len(ws_id) != 36 or ws_id.count("-") != 4:
            errors.append(f"workspaces_{target}.json: '{label}' has an invalid workspace id: {ws_id!r}")

    props = config.get("sparkProperties") or {}
    for k, v in props.items():
        if not isinstance(v, str):
            errors.append(f"sparkProperties['{k}'] must be a string (\"{str(v).lower()}\"), got {v!r}")

    compute = config.get("compute", {})
    unknown = set(compute) - set(workspaces)
    if unknown:
        errors.append(f"compute has workspaces not in workspaces_{target}.json: {sorted(unknown)}")
    for label, c in compute.items():
        for role in ("driver", "executor"):
            cores, mem = c.get(f"{role}Cores"), c.get(f"{role}Memory")
            if cores is not None and CORE_MEMORY.get(cores) != mem:
                errors.append(f"compute.{label}: {role}Cores {cores} needs {role}Memory "
                              f"{CORE_MEMORY.get(cores, '?')!r}, got {mem!r}")
        dyn = c.get("dynamicExecutorAllocation") or {}
        if dyn.get("enabled") and dyn.get("minExecutors", 1) > dyn.get("maxExecutors", 1):
            errors.append(f"compute.{label}: minExecutors > maxExecutors")
    return errors


# --------------------------------------------------------------------------- #
# Idempotent item helpers
# --------------------------------------------------------------------------- #

def ensure_folder(client: FabricClient, ws_id: str, name: str) -> str:
    for f in client.get(f"/workspaces/{ws_id}/folders", context="list folders").json().get("value", []):
        if f["displayName"] == name and not f.get("parentFolderId"):
            print(f"    [=] Folder '{name}' exists")
            return f["id"]
    r = client.post(f"/workspaces/{ws_id}/folders", json={"displayName": name}, context="create folder")
    print(f"    [+] Folder '{name}' created")
    return r.json()["id"]


def find_environment(client: FabricClient, ws_id: str, name: str) -> Optional[str]:
    r = client.get(f"/workspaces/{ws_id}/environments", context="list environments")
    return next((e["id"] for e in r.json().get("value", []) if e["displayName"] == name), None)


def ensure_environment(client: FabricClient, ws_id: str, name: str, folder_id: Optional[str]) -> str:
    env_id = find_environment(client, ws_id, name)
    if env_id:
        print(f"    [=] Environment '{name}' exists")
        return env_id
    body = {"displayName": name, **({"folderId": folder_id} if folder_id else {})}
    r = client.wait(client.post(f"/workspaces/{ws_id}/environments", json=body, context="create environment"))
    print(f"    [+] Environment '{name}' created")
    return r.json()["id"]


def library_name(lib) -> str:
    """The staging libraries API has returned both plain strings and objects over time."""
    if isinstance(lib, str):
        return lib
    return lib.get("name") or lib.get("fileName") or lib.get("displayName") or ""


def list_custom_libraries(client: FabricClient, ws_id: str, env_id: str) -> list:
    r = client.get(f"/workspaces/{ws_id}/environments/{env_id}/staging/libraries",
                   params={"beta": "false"}, allow=(404,), context="list staging libraries")
    if r.status_code == 404:  # EnvironmentLibrariesNotFound = empty Environment, not an error
        return []
    body = r.json()
    return [library_name(l) for l in (body.get("customLibraries") or body.get("value") or body.get("libraries") or [])]


def remove_libraries(client: FabricClient, ws_id: str, env_id: str, prefix: str = "") -> None:
    for name in list_custom_libraries(client, ws_id, env_id):
        if name and name.startswith(prefix):
            client.delete(f"/workspaces/{ws_id}/environments/{env_id}/staging/libraries/{name}",
                          context=f"delete {name}")
            print(f"    [-] Removed {name}")


def upload_library(client: FabricClient, ws_id: str, env_id: str, whl: Path) -> None:
    with open(whl, "rb") as f:
        client.post(f"/workspaces/{ws_id}/environments/{env_id}/staging/libraries/{whl.name}",
                    data=f, content_type="application/octet-stream", context="upload library")
    print(f"    [+] Uploaded {whl.name}")


def get_spark_compute(client: FabricClient, ws_id: str, env_id: str) -> dict:
    return client.get(f"/workspaces/{ws_id}/environments/{env_id}/staging/sparkcompute",
                      params={"beta": "false"}, context="get spark compute").json()


def patch_spark_compute(client: FabricClient, ws_id: str, env_id: str, patch: dict) -> None:
    client.patch(f"/workspaces/{ws_id}/environments/{env_id}/staging/sparkcompute",
                 params={"beta": "false"}, json=patch, context="update spark compute")


def publish(client: FabricClient, ws_id: str, env_id: str) -> None:
    print("    [~] Publishing (usually 3-6 minutes)...")
    client.wait(client.post(f"/workspaces/{ws_id}/environments/{env_id}/staging/publish", context="publish"))
    print("    [+] Published")
