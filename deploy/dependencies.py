"""
Checks that the wheel's dependencies can be installed on the Python version
of each Fabric runtime. Used by check_dependencies.py (on pyproject.toml, with
an upgrade proposal) and by deploy.py (on the wheel, before touching Fabric).

For each dependency it looks up on PyPI the versions its specifier allows
(e.g. ==0.14.3 -> only that one) and checks that one of them installs on that
Python: the package's requires_python, plus an sdist or a Linux x86_64 wheel
for that Python. If none does, it proposes the MINIMUM later version that
works on every requested Python (the same wheel goes to dev and pro), so the
jump is as small as possible.

Limitation: only the wheel's direct dependencies are checked, not the
transitive ones. It covers the common case (an old pin that doesn't support
the new Python), but Fabric's publish is still the final check.

Requirements:
    pip install requests packaging
"""

import tomllib
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import requests
from packaging.requirements import Requirement
from packaging.specifiers import InvalidSpecifier, SpecifierSet
from packaging.utils import InvalidWheelFilename, parse_wheel_filename
from packaging.version import InvalidVersion, Version

PYPI = "https://pypi.org/pypi"


@dataclass
class Issue:
    requirement: str            # as written in pyproject.toml / METADATA
    pythons: List[str]          # Python versions it can't be installed on
    proposed: Optional[str]     # new requirement, or None if no version works
    latest: Optional[str]       # latest compatible version, for reference


# --------------------------------------------------------------------------- #
# Read dependencies
# --------------------------------------------------------------------------- #

def pyproject_requirements(pyproject: Path) -> List[str]:
    return tomllib.loads(pyproject.read_text(encoding="utf-8-sig"))["project"].get("dependencies", [])


def wheel_requirements(whl: Path) -> List[str]:
    """Requires-Dist from the wheel's METADATA: what Fabric will actually install."""
    with zipfile.ZipFile(whl) as z:
        metadata = next(n for n in z.namelist() if n.endswith(".dist-info/METADATA"))
        text = z.read(metadata).decode("utf-8")
    return [line.split(":", 1)[1].strip() for line in text.splitlines() if line.startswith("Requires-Dist:")]


# --------------------------------------------------------------------------- #
# PyPI
# --------------------------------------------------------------------------- #

def _fetch_releases(name: str) -> Dict[Version, list]:
    """Stable releases that have files (no pre-releases)."""
    r = requests.get(f"{PYPI}/{name}/json", timeout=30)
    r.raise_for_status()
    releases = {}
    for v, files in r.json()["releases"].items():
        try:
            version = Version(v)
        except InvalidVersion:
            continue
        if files and not version.is_prerelease:
            releases[version] = files
    return releases


def _file_installs_on(f: dict, python: str) -> bool:
    if f.get("yanked"):
        return False
    try:
        if f.get("requires_python") and Version(python) not in SpecifierSet(f["requires_python"]):
            return False
    except InvalidSpecifier:
        pass  # broken metadata on PyPI: pip ignores it, so do we
    if f["packagetype"] == "sdist":
        return True
    if f["packagetype"] != "bdist_wheel":
        return False
    try:
        tags = parse_wheel_filename(f["filename"])[3]
    except InvalidWheelFilename:
        return False
    minor = int(python.split(".")[1])
    for t in tags:
        # Fabric clusters are Linux x86_64
        if t.platform != "any" and not (t.platform.startswith(("manylinux", "linux")) and "x86_64" in t.platform):
            continue
        if t.interpreter.startswith("py3") or t.interpreter == f"cp3{minor}":
            return True
        if t.abi == "abi3" and t.interpreter.startswith("cp3") and int(t.interpreter[3:]) <= minor:
            return True
    return False


def _installs_on(files: list, python: str) -> bool:
    return any(_file_installs_on(f, python) for f in files)


# --------------------------------------------------------------------------- #
# Check
# --------------------------------------------------------------------------- #

def _with_version(req: Requirement, op: str, version: Version) -> str:
    extras = f"[{','.join(sorted(req.extras))}]" if req.extras else ""
    marker = f"; {req.marker}" if req.marker else ""
    return f"{req.name}{extras}{op}{version}{marker}"


def check_requirements(requirements: Iterable[str], pythons: List[str]) -> List[Issue]:
    """Returns the dependencies that can't be installed on some of the Python versions."""
    issues = []
    for raw in requirements:
        req = Requirement(raw)
        # Dependencies with a marker (e.g. '; python_version < "3.12"' or extras) only count where they apply
        relevant = [p for p in pythons if req.marker is None or req.marker.evaluate({"python_version": p, "extra": ""})]
        if not relevant:
            continue

        releases = _fetch_releases(req.name)
        candidates = [v for v in releases if req.specifier.contains(v)]
        # pip picks the highest version in the specifier that installs on that Python
        failing = [p for p in relevant if not any(_installs_on(releases[v], p) for v in candidates)]
        if not failing:
            continue

        compatible = sorted(v for v in releases if all(_installs_on(releases[v], p) for p in relevant))
        newer = [v for v in compatible if not candidates or v > max(candidates)]
        proposed = None
        if newer:
            pinned = len(req.specifier) == 1 and next(iter(req.specifier)).operator == "=="
            # Pin -> the minimum later version (smallest jump). Pin to a version that doesn't exist -> the latest.
            target = newer[0] if candidates else newer[-1]
            proposed = _with_version(req, "==" if pinned else ">=", target)
        issues.append(Issue(raw, failing, proposed, str(compatible[-1]) if compatible else None))
    return issues


def format_issues(issues: List[Issue]) -> str:
    lines = []
    for i in issues:
        line = f"  - {i.requirement}: can't be installed on Python {', '.join(i.pythons)}"
        if i.proposed:
            line += f"\n      proposal: {i.proposed}  (latest compatible: {i.latest})"
        else:
            line += "\n      no compatible version on PyPI: remove or replace it by hand"
        lines.append(line)
    return "\n".join(lines)
