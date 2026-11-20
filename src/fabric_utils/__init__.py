"""
fabric_utils

Everything a notebook used to get from `%run Packages`, `%run Connections`
and `%run Functions`, packaged as one library:

    from fabric_utils import *
    paths = get_paths()          # env is read from the Variable Library
    check_environment(spark)     # optional (Spark): confirm runtime, NEE and properties

Two modes, picked automatically at import time:

- spark:  pyspark and delta-spark are installed (Spark notebooks). The custom
          Environment installs the wheel. Exports everything below.
- python: they aren't (Fabric Python notebooks, no Spark). Python notebooks
          can't use an Environment, so the wheel is installed in the session
          from a lakehouse folder with `pip install --no-deps` (see README,
          "Python notebooks"). Exports get_paths, NotebookLogger and the
          DuckDB / deltalake helpers of py_engine.

FABRIC_UTILS_ENGINE=spark|python (environment variable) forces a mode, for tests.
"""

import os as _os
from importlib.util import find_spec as _find_spec

__version__ = "0.2.0"


def _detect_engine() -> str:
    forced = _os.environ.get("FABRIC_UTILS_ENGINE", "").strip().lower()
    if forced in ("spark", "python"):
        return forced
    try:
        has_spark = _find_spec("pyspark") is not None and _find_spec("delta") is not None
    except (ImportError, ValueError):
        has_spark = False
    return "spark" if has_spark else "python"


FABRIC_UTILS_ENGINE = _detect_engine()

# --- Both modes ---
from datetime import datetime, date, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

import json
import re
import time

# Connections: workspaces and OneLake paths per environment
from fabric_utils.connections import get_env, get_paths

# Run log: Spark or deltalake underneath, same table
from fabric_utils.loggers import NotebookLogger

if FABRIC_UTILS_ENGINE == "spark":
    # No try/except on purpose: in a Spark notebook a missing dependency must fail here.

    # --- Common imports (what the old "Packages" notebook did) ---
    from pyspark.sql import functions as F
    from pyspark.sql.types import *  # StructType, StructField, StringType, DateType...
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.window import Window
    from delta.tables import DeltaTable

    # --- Spark: session-level overrides and environment checks ---
    from fabric_utils.spark_config import (
        check_environment,
        session_config,
        native_disabled,
    )

    # --- Functions: read / write / schema / transforms ---
    from fabric_utils.load_save_data import load_data, save_table
    from fabric_utils.schema import print_schema, enforce_destination_schema
    from fabric_utils.transform import (
        rename_columns,
        clean_datetime_columns,
        add_date_load,
        trim_string_columns,
        deduplicate,
    )

    # --- Your enterprise functions: shared business rules ---
    from fabric_utils.business import add_surrogate_key

else:
    # --- Python notebooks: DuckDB reads, deltalake writes ---
    from fabric_utils.py_engine import (
        duckdb_connect,
        register_delta_views,
        save_table_arrow,
        get_storage_options,
    )

# Everything defined above is exported by `from fabric_utils import *`,
# except names starting with "_". New imports are picked up automatically.
__all__ = [name for name in dir() if not name.startswith("_")]
