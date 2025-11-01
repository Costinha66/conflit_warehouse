import duckdb
from typing import Dict


def _table_cols(con: duckdb.DuckDBPyConnection, table: str) -> Dict[str, str]:
    """Return {name_lower: type_name_upper} for a table."""
    rows = con.execute(f"PRAGMA table_info({table})").fetchall()
    # schema: cid, name, type, notnull, dflt_value, pk
    return {r[1].lower(): (r[2] or "").upper() for r in rows}


def _count(con: duckdb.DuckDBPyConnection, sql: str, params: list | None = None) -> int:
    return con.execute(sql, params or []).fetchone()[0]
