"""
Your company's shared business rules.

This is the module to copy when you add your own enterprise functions: rules
that more than one notebook must apply the same way (surrogate keys, fiscal
calendars, status mappings, currency conversion...). Written once here,
reviewed in a pull request and shipped in the wheel, every workspace runs
the same version of them.

To add one:
    1. Write the function here (or in a new module next to this one).
    2. Import it in __init__.py so `from fabric_utils import *` exposes it.
    3. Bump the version in pyproject.toml and __init__.py.
    4. Open a pull request: the pipeline builds the wheel and deploys it.
"""

from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_surrogate_key(df: DataFrame, keys: List[str], name: str = "sk") -> DataFrame:
    """
    Add a deterministic surrogate key (64-bit hash of the business keys).

    Same keys give the same value in every run, notebook and environment,
    so Gold facts and dimensions join on it without a lookup table.
    """
    parts = [F.coalesce(F.col(k).cast("string"), F.lit("<null>")) for k in keys]
    return df.withColumn(name, F.xxhash64(*parts))
