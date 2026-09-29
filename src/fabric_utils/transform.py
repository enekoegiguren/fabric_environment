"""
Generic DataFrame transformations shared by Silver and Gold notebooks.
"""

from datetime import datetime
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType


def rename_columns(df: DataFrame, mapping: Dict[str, str]) -> DataFrame:
    """Rename columns with an {old: new} mapping. Missing columns are ignored."""
    for old, new in mapping.items():
        df = df.withColumnRenamed(old, new)
    return df


def clean_datetime_columns(df: DataFrame, min_date: str = "1901-01-01") -> DataFrame:
    """
    Replace timestamps before min_date (e.g. 0001-01-01 placeholders from
    source systems) with min_date and cast every timestamp column to date.

    If the source parquet files were written with the legacy calendar, read
    them inside session_config(spark, spark__sql__parquet__datetimeRebaseModeInRead="LEGACY")
    instead of setting that config for the whole session.
    """
    for field in df.schema.fields:
        if isinstance(field.dataType, TimestampType):
            col = F.col(field.name)
            df = df.withColumn(
                field.name,
                F.when(col < F.lit(min_date), F.lit(min_date).cast("date")).otherwise(col.cast("date")),
            )
    return df


def add_date_load(df: DataFrame, tz: str = "UTC", column: str = "date_load") -> DataFrame:
    """Add a load timestamp column in the given timezone (e.g. 'Europe/Madrid')."""
    now = datetime.now(ZoneInfo(tz)).replace(tzinfo=None)
    return df.withColumn(column, F.lit(now))


def trim_string_columns(df: DataFrame, empty_as_null: bool = True) -> DataFrame:
    """Trim every string column; optionally turn empty strings into nulls."""
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            col = F.trim(F.col(field.name))
            if empty_as_null:
                col = F.when(col == "", None).otherwise(col)
            df = df.withColumn(field.name, col)
    return df


def deduplicate(df: DataFrame, keys: List[str], order_by: Optional[str] = None) -> DataFrame:
    """
    Keep one row per key. With order_by, keep the most recent row by that
    column (descending); without it, an arbitrary one (dropDuplicates).
    """
    if order_by is None:
        return df.dropDuplicates(keys)
    w = Window.partitionBy(*keys).orderBy(F.col(order_by).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
