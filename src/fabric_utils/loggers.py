"""
Minimal run log: one row per notebook run in <control lakehouse>/Tables/notebook_logs.
"""

from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from fabric_utils.connections import get_paths

_SCHEMA = StructType([
    StructField("env", StringType(), True),
    StructField("layer", StringType(), True),
    StructField("notebook_name", StringType(), True),
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("duration_sec", DoubleType(), True),
    StructField("status", StringType(), True),
])


class NotebookLogger:
    """
    Usage:
        log = NotebookLogger("nb_silver_customers", layer="silver")
        try:
            ...
            log.end()
        except Exception as e:
            log.log_error(e)
            raise
    """

    def __init__(self, notebook_name: str, layer: Optional[str] = None, env: Optional[str] = None):
        self.spark = SparkSession.builder.getOrCreate()
        paths = get_paths(env)
        self.env = paths["env"]
        self.target = f"{paths['path_control'].rstrip('/')}/notebook_logs"
        self.notebook_name = notebook_name
        self.layer = layer
        self.start_time = datetime.now()

    def end(self, status: str = "Success") -> None:
        end_time = datetime.now()
        row = [(self.env, self.layer, self.notebook_name, self.start_time, end_time,
                (end_time - self.start_time).total_seconds(), status)]
        self.spark.createDataFrame(row, _SCHEMA).write.format("delta").mode("append").save(self.target)

    def log_error(self, error: Exception) -> None:
        self.end(status=f"Error: {error}")
