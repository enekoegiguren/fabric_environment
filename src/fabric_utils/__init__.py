"""
fabric_utils

Everything a notebook used to get from `%run Packages`, `%run Connections`
and `%run Functions`, packaged as one library that the custom Environment
installs for every notebook in the workspace:

    from fabric_utils import *
    paths = get_paths()          # env is read from the Variable Library
    check_environment(spark)     # optional: confirm runtime, NEE and properties
"""

__version__ = "0.1.0"

# --- Common imports (what the old "Packages" notebook did) ---
from pyspark.sql import functions as F
from pyspark.sql.types import *  # StructType, StructField, StringType, DateType...
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window
from delta.tables import DeltaTable

from datetime import datetime, date, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

import json
import re
import time

# --- Connections: workspaces and OneLake paths per environment ---
from fabric_utils.connections import get_env, get_paths

# --- Spark: session-level overrides and environment checks ---
from fabric_utils.spark_config import (
    check_environment,
    session_config,
    native_disabled,
)

# --- Functions: read / write / schema / transforms / logging ---
from fabric_utils.load_save_data import load_data, save_table
from fabric_utils.schema import print_schema, enforce_destination_schema
from fabric_utils.transform import (
    rename_columns,
    clean_datetime_columns,
    add_date_load,
    trim_string_columns,
    deduplicate,
)
from fabric_utils.loggers import NotebookLogger

# Everything defined above is exported by `from fabric_utils import *`,
# except names starting with "_". New imports are picked up automatically.
__all__ = [name for name in dir() if not name.startswith("_")]
