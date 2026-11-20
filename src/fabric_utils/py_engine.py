"""
Python notebooks (no Spark): read Delta tables from OneLake with DuckDB and
write them with deltalake (delta-rs).

Spark mode -> Python mode:
    load_data                              -> duckdb_connect + register_delta_views
    enforce_destination_schema + save_table -> save_table_arrow(..., schema=pa.schema([...]))

Usage in a Python notebook:
    from fabric_utils import *
    paths = get_paths()
    con = duckdb_connect()
    register_delta_views(con, paths["path_silver"], ["orders", "customers"])
    rel = con.sql("SELECT ... FROM orders JOIN customers USING (customer_id)")
    save_table_arrow(rel, paths["path_gold"], "open_orders", schema=destination_schema)

duckdb, pyarrow and deltalake ship with the Fabric Python runtime. They are
imported inside each function, so importing fabric_utils never depends on them
(and the Spark side never needs them).
"""

from datetime import datetime, timezone
from typing import Dict, Iterable, Optional, Union


def _storage_token() -> str:
    import notebookutils
    return notebookutils.credentials.getToken("storage")


def get_storage_options(token: Optional[str] = None) -> Dict[str, str]:
    """deltalake storage_options for OneLake, with the token of the identity running the notebook."""
    return {
        "bearer_token": token or _storage_token(),
        "use_fabric_endpoint": "true",
        "allow_unsafe_rename": "true",
    }


def duckdb_connect(token: Optional[str] = None, timezone_name: str = "UTC"):
    """
    DuckDB connection ready for OneLake: session time zone (UTC by default,
    like Spark in Fabric) and an Azure secret with the notebook's token.
    """
    import duckdb

    con = duckdb.connect()
    con.execute(f"SET TimeZone = '{timezone_name}'")
    con.execute(
        "CREATE OR REPLACE SECRET onelake "
        f"(TYPE AZURE, PROVIDER ACCESS_TOKEN, ACCESS_TOKEN '{token or _storage_token()}')"
    )
    return con


def register_delta_views(con, path: str, tables: Union[Iterable[str], Dict[str, str]]) -> None:
    """
    One DuckDB view per Delta table: SELECT * FROM delta_scan('<path><table>').
    tables: a list of table names (the view gets the same name) or a dict {view: table}.
    DuckDB only reads the columns and rows the query actually uses.
    """
    pairs = tables.items() if isinstance(tables, dict) else ((t, t) for t in tables)
    base = path if path.endswith("/") else f"{path}/"
    for view, table in pairs:
        con.execute(f'CREATE OR REPLACE VIEW "{view}" AS SELECT * FROM delta_scan(\'{base}{table}\')')
        print(f"View {view} -> {base}{table}")


def naive_to_utc(value: datetime) -> datetime:
    """A naive datetime read as UTC: what Spark in Fabric (session in UTC) does when it saves a TimestampType."""
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _to_arrow(data):
    """pyarrow.Table from a Table or a DuckDB relation."""
    import pyarrow as pa

    if isinstance(data, pa.Table):
        return data
    for method in ("to_arrow_table", "fetch_arrow_table"):   # DuckDB relation (2.x / 1.x)
        if hasattr(data, method):
            return getattr(data, method)()
    raise TypeError(f"Expected a pyarrow.Table or a DuckDB relation, got {type(data).__name__}")


def save_table_arrow(
    data,
    path: str,
    table_name: str,
    schema=None,
    mode: str = "overwrite",
    overwrite_schema: bool = True,
    storage_options: Optional[Dict[str, str]] = None,
) -> int:
    """
    Write a Delta table with deltalake: the Python-mode save_table.

    data: a pyarrow.Table or a DuckDB relation.
    schema: destination pyarrow.Schema. If given, the columns are selected in
        its order and cast to its types (fails if one is missing), like
        enforce_destination_schema.
    overwrite_schema: with mode="overwrite", replace the table's schema.
    Returns the number of rows written.

    Tables written here must not use deletion vectors if Spark also writes
    them: delta-rs can't write tables that have them.
    """
    from deltalake import write_deltalake

    table = _to_arrow(data)
    if schema is not None:
        table = table.select(schema.names).cast(schema)

    full_path = f"{path.rstrip('/')}/{table_name}"
    schema_mode = "overwrite" if overwrite_schema and mode == "overwrite" else None
    write_deltalake(
        full_path,
        table,
        mode=mode,
        schema_mode=schema_mode,
        storage_options=storage_options if storage_options is not None else get_storage_options(),
    )
    print(f"Table {table_name} written at {full_path} (mode={mode}): {table.num_rows} rows")
    return table.num_rows
