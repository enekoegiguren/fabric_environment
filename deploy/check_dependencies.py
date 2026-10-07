"""
Checks that the dependencies in pyproject.toml can be installed on the
Python version of each target's runtime (runtimeVersion in
environment_<target>.jsonc). If one can't, it proposes the minimum version
compatible with EVERY target (the same wheel goes to dev and pro) and, if you
confirm, updates pyproject.toml and bumps the package's patch version.

Run it when you change runtimeVersion or the dependencies, before the build.
deploy.py runs the same check on the wheel, but only to stop: the fix is
made here.

Usage:
    python deploy/check_dependencies.py                 # dev + pro, asks before changing anything
    python deploy/check_dependencies.py --target dev    # only dev's runtime
    python deploy/check_dependencies.py --yes           # apply the proposal without asking
    python deploy/check_dependencies.py --check         # report only; exit 1 if incompatible (CI)

Requirements:
    pip install requests packaging
"""

import argparse
import re
import tomllib
from pathlib import Path

from packaging.version import Version

from dependencies import check_requirements, format_issues, pyproject_requirements
from fabric_api import RUNTIME_PYTHON, TARGETS, load_target

PROJECT_DIR = Path(__file__).parent.parent
VERSION_RE = re.compile(r'^(version\s*=\s*")([^"]+)(")', re.MULTILINE)
INIT_VERSION_RE = re.compile(r'^(__version__\s*=\s*")([^"]+)(")', re.MULTILINE)


def bump_patch(version: str) -> str:
    v = Version(version)
    return f"{v.major}.{v.minor}.{v.micro + 1}"


def apply_changes(pyproject: Path, issues: list) -> str:
    """Replace the requirements in pyproject.toml and bump the version. Returns the new version."""
    text = pyproject.read_text(encoding="utf-8-sig")
    for i in issues:
        old, new = f'"{i.requirement}"', f'"{i.proposed}"'
        if text.count(old) != 1:
            raise SystemExit(f"Can't find {old} exactly once in {pyproject.name}: change it to {new} by hand")
        text = text.replace(old, new)

    match = VERSION_RE.search(text)
    if not match:
        raise SystemExit(f"Can't find 'version = \"...\"' in {pyproject.name}")
    new_version = bump_patch(match.group(2))
    text = VERSION_RE.sub(rf"\g<1>{new_version}\g<3>", text, count=1)
    pyproject.write_text(text, encoding="utf-8")

    # The package's __version__, if it has one, so it stays in sync with pyproject.toml
    for init in (pyproject.parent / "src").glob("*/__init__.py"):
        init_text = init.read_text(encoding="utf-8")
        if INIT_VERSION_RE.search(init_text):
            init.write_text(INIT_VERSION_RE.sub(rf"\g<1>{new_version}\g<3>", init_text, count=1), encoding="utf-8")
            print(f"[+] {init.relative_to(pyproject.parent)}: __version__ = {new_version}")
    return new_version


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--target", choices=TARGETS, action="append", help="Default: all")
    parser.add_argument("--pyproject", type=Path, default=PROJECT_DIR / "pyproject.toml")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--yes", action="store_true", help="Apply the proposal without asking")
    mode.add_argument("--check", action="store_true", help="Report only; exit 1 if anything is incompatible")
    args = parser.parse_args()

    runtimes = {t: load_target(t)[1]["runtimeVersion"] for t in (args.target or TARGETS)}
    pythons = sorted({RUNTIME_PYTHON[r] for r in runtimes.values()}, key=Version)
    print("Runtimes: " + ", ".join(f"{t} {r} (Python {RUNTIME_PYTHON[r]})" for t, r in runtimes.items()))

    requirements = pyproject_requirements(args.pyproject)
    print(f"Checking {len(requirements)} dependencies from {args.pyproject.name} on PyPI...")
    issues = check_requirements(requirements, pythons)
    if not issues:
        print(f"[ok] All of them install on Python {', '.join(pythons)}")
        return

    print(f"\n[!] Incompatible dependencies:\n{format_issues(issues)}\n")
    if any(not i.proposed for i in issues):
        raise SystemExit("Some dependencies have no compatible version: fix pyproject.toml by hand.")
    if args.check:
        raise SystemExit("Apply the proposal with: python deploy/check_dependencies.py")

    if not args.yes:
        try:
            answer = input(f"Apply the proposal to {args.pyproject.name} and bump the package version? [y/N] ")
        except EOFError:
            answer = ""
        if answer.strip().lower() not in ("y", "yes"):
            print("No changes.")
            return

    new_version = apply_changes(args.pyproject, issues)
    for i in issues:
        print(f"[+] {i.requirement} -> {i.proposed}")
    print(f"[+] version -> {new_version}")

    dist_name = re.sub(r"[-.]+", "_", tomllib.loads(args.pyproject.read_text(encoding="utf-8-sig"))["project"]["name"])
    print("\nNext steps:")
    print("  python -m build --wheel")
    print(f"  python deploy/deploy.py --target dev --whl dist/{dist_name}-{new_version}-py3-none-any.whl")


if __name__ == "__main__":
    main()
