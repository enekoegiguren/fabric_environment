"""
Schema helpers: print a DataFrame schema as ready-to-paste StructType code,
and force a DataFrame onto a canonical destination schema.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


def print_schema(df: DataFrame) -> None:
    """Print df's schema as StructType code, to use with enforce_destination_schema()."""
    print("destination_schema = StructType([")
    for field in df.schema.fields:
        print(f"    StructField('{field.name}', {field.dataType}, True),")
    print("])")


def enforce_destination_schema(df: DataFrame, schema: StructType) -> DataFrame:
    """Cast columns to the destination schema, add missing ones as nulls, drop the rest."""
    return df.select([
        (F.col(f.name) if f.name in df.columns else F.lit(None)).cast(f.dataType).alias(f.name)
        for f in schema.fields
    ])
