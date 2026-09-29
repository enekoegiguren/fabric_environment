"""
Read and write Delta tables by OneLake path (see connections.get_paths()).
"""

from datetime import date, datetime
from typing import List, Optional, Union

from pyspark.sql import DataFrame, SparkSession


def load_data(
    source_path: str,
    table_name: str,
    filter_column: Optional[str] = None,
    filter_from: Optional[Union[str, int, date, datetime]] = None,
    columns: Optional[List[str]] = None,
    format: str = "delta",
) -> DataFrame:
    """
    Load a table from a path such as paths["path_bronze"].

    Args:
        filter_column / filter_from: optional incremental filter (filter_column >= filter_from).
        columns: optional list of columns to select.
    """
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.format(format).load(f"{source_path.rstrip('/')}/{table_name}")

    if filter_column and filter_from is not None:
        value = filter_from if isinstance(filter_from, int) else f"'{filter_from}'"
        df = df.filter(f"{filter_column} >= {value}")

    if columns:
        df = df.select(*columns)
    return df


def save_table(
    df: DataFrame,
    path: str,
    table_name: str,
    mode: str = "overwrite",
    overwrite_schema: bool = True,
    partition_by: Optional[List[str]] = None,
) -> str:
    """
    Write df as a Delta table at <path>/<table_name> and return the full path.

    Write behaviour (optimize write, V-Order, auto compaction...) is NOT set
    here: it comes from the Environment's Spark properties, so every notebook
    writes the same way.
    """
    full_path = f"{path.rstrip('/')}/{table_name}"
    writer = df.write.format("delta").mode(mode)

    if overwrite_schema and mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)

    writer.save(full_path)
    print(f"Table {table_name} written to {full_path} (mode={mode})")
    return full_path
