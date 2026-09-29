"""
Spark configuration helpers.

Configs that every session needs (NEE, Delta write/optimize behaviour,
timezone...) live in the Environment's Spark properties
(deploy/environment_<target>.jsonc) and are applied when the session starts.
Notebooks should NOT repeat them with spark.conf.set().

What stays in code is the exception: a config only one notebook or one
query needs. Use session_config() for that, so the change is scoped and
undone afterwards instead of leaking into the rest of the session.
"""

from contextlib import contextmanager
from typing import Dict, Iterator, Optional

# Spark major.minor -> Fabric runtime
_RUNTIMES = {"3.4": "1.2", "3.5": "1.3", "4.0": "2.0", "4.1": "2.0"}

# Properties worth printing in check_environment(). "default_on" = runtimes
# where Fabric already enables it without any config. The Native Execution
# Engine is off by default on every runtime, including 2.0.
KEY_PROPERTIES = {
    "spark.native.enabled": {"label": "Native Execution Engine", "default_on": []},
    "spark.databricks.delta.autoCompact.enabled": {"label": "Auto compaction", "default_on": []},
    "spark.microsoft.delta.targetFileSize.adaptive.enabled": {"label": "Adaptive target file size", "default_on": ["2.0"]},
    "spark.microsoft.delta.optimize.fast.enabled": {"label": "Fast optimize", "default_on": ["2.0"]},
    "spark.microsoft.delta.optimize.fileLevelTarget.enabled": {"label": "File-level compaction targets", "default_on": ["2.0"]},
    "spark.databricks.delta.properties.defaults.enableDeletionVectors": {"label": "Deletion vectors (new tables)", "default_on": ["2.0"]},
    "spark.microsoft.delta.snapshot.driverMode.enabled": {"label": "Driver-mode snapshot", "default_on": ["2.0"]},
    "spark.fabric.resourceProfile": {"label": "Resource profile", "default_on": []},
    "spark.sql.session.timeZone": {"label": "Session timezone", "default_on": []},
}


def fabric_runtime(spark) -> str:
    """Fabric runtime version derived from the Spark version (e.g. '1.3', '2.0')."""
    major_minor = ".".join(spark.version.split(".")[:2])
    return _RUNTIMES.get(major_minor, f"unknown (Spark {spark.version})")


def check_environment(spark, expected: Optional[Dict[str, str]] = None) -> Dict[str, Optional[str]]:
    """
    Print the runtime, whether the fabric_utils library loaded, and the
    effective value of the key Spark properties. Run it once after a deploy
    to confirm the Environment was published and attached.

    Args:
        expected: optional {property: value}. Any mismatch is flagged.

    Returns:
        {property: effective value or None if unset}
    """
    import fabric_utils

    runtime = fabric_runtime(spark)
    print(f"Fabric runtime : {runtime} (Spark {spark.version})")
    print(f"fabric_utils   : {fabric_utils.__version__}")
    print()

    values: Dict[str, Optional[str]] = {}
    for key, meta in KEY_PROPERTIES.items():
        value = spark.conf.get(key, None)
        values[key] = value
        shown = value if value is not None else ("(unset, on by default)" if runtime in meta["default_on"] else "(unset)")
        flag = ""
        if expected and key in expected and str(value).lower() != str(expected[key]).lower():
            flag = f"   <-- expected {expected[key]}"
        print(f"  {meta['label']:<32} {shown}{flag}")

    if str(values.get("spark.native.enabled")).lower() != "true":
        print("\n[!] NEE is not enabled. Add \"spark.native.enabled\": \"true\" to sparkProperties "
              "in environment_<target>.jsonc and redeploy (it is off by default on 1.3 AND 2.0).")
    return values


@contextmanager
def session_config(spark, **overrides: str) -> Iterator[None]:
    """
    Temporarily override Spark configs for a block of code, then restore them.
    Keys use '__' instead of '.' because Python keyword arguments can't contain dots:

        with session_config(spark, spark__sql__parquet__datetimeRebaseModeInRead="LEGACY"):
            df = spark.read.parquet(legacy_path)
    """
    keys = {k.replace("__", "."): str(v) for k, v in overrides.items()}
    previous = {k: spark.conf.get(k, None) for k in keys}
    try:
        for k, v in keys.items():
            spark.conf.set(k, v)
        yield
    finally:
        for k, v in previous.items():
            if v is None:
                spark.conf.unset(k)
            else:
                spark.conf.set(k, v)


@contextmanager
def native_disabled(spark) -> Iterator[None]:
    """
    Run one block on the regular JVM engine instead of NEE, e.g. a query that
    regressed or relies on an operator NEE doesn't support. Better than
    disabling NEE for the whole Environment:

        with native_disabled(spark):
            df_problematic.write.mode("overwrite").save(path)
    """
    with session_config(spark, spark__native__enabled="false"):
        yield
