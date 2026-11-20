"""
Minimal run log: one row per notebook run in <control lakehouse>/Tables/notebook_logs.

In a Spark notebook it writes with Spark. In a Python notebook it writes the
same columns and types with deltalake, so both kinds of notebook share one table.
"""

from datetime import datetime
from typing import Optional

from fabric_utils.connections import get_paths


def _spark_session():
    """The Spark session in spark mode (see fabric_utils.__init__); None in python mode."""
    from fabric_utils import FABRIC_UTILS_ENGINE

    if FABRIC_UTILS_ENGINE != "spark":
        return None
    from pyspark.sql import SparkSession
    return SparkSession.builder.getOrCreate()


class NotebookLogger:
    """
    Usage (Spark and Python notebooks alike):
        log = NotebookLogger("nb_silver_customers", layer="silver")
        try:
            ...
            log.end()
        except Exception as e:
            log.log_error(e)
            raise
    """

    def __init__(self, notebook_name: str, layer: Optional[str] = None, env: Optional[str] = None):
        self.spark = _spark_session()
        paths = get_paths(env)
        self.env = paths["env"]
        self.target = f"{paths['path_control'].rstrip('/')}/notebook_logs"
        self.notebook_name = notebook_name
        self.layer = layer
        self.start_time = datetime.now()

    def end(self, status: str = "Success") -> None:
        end_time = datetime.now()
        row = (self.env, self.layer, self.notebook_name, self.start_time, end_time,
               (end_time - self.start_time).total_seconds(), status)
        if self.spark is None:
            self._write_deltalake(row)
            return

        from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

        schema = StructType([
            StructField("env", StringType(), True),
            StructField("layer", StringType(), True),
            StructField("notebook_name", StringType(), True),
            StructField("start_time", TimestampType(), True),
            StructField("end_time", TimestampType(), True),
            StructField("duration_sec", DoubleType(), True),
            StructField("status", StringType(), True),
        ])
        self.spark.createDataFrame([row], schema).write.format("delta").mode("append").save(self.target)

    def _write_deltalake(self, row: tuple) -> None:
        """The same row as the Spark branch, written with deltalake (Python notebooks)."""
        import pyarrow as pa
        from deltalake import write_deltalake

        from fabric_utils.py_engine import get_storage_options, naive_to_utc

        env, layer, name, start, end, duration, status = row
        schema = pa.schema([
            pa.field("env", pa.string()),
            pa.field("layer", pa.string()),
            pa.field("notebook_name", pa.string()),
            # Spark stores a naive datetime as UTC (Fabric sessions run in UTC); do the same.
            pa.field("start_time", pa.timestamp("us", tz="UTC")),
            pa.field("end_time", pa.timestamp("us", tz="UTC")),
            pa.field("duration_sec", pa.float64()),
            pa.field("status", pa.string()),
        ])
        table = pa.table([[env], [layer], [name], [naive_to_utc(start)], [naive_to_utc(end)],
                          [float(duration)], [status]], schema=schema)
        write_deltalake(self.target, table, mode="append", storage_options=get_storage_options())

    def log_error(self, error: Exception) -> None:
        self.end(status=f"Error: {error}")
