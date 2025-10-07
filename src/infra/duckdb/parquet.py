import duckdb
from pathlib import Path


def duckdb_parquet_rowcount(path: Path) -> int:
    # reuse an existing connection if you can; avoid per-call connect
    con = duckdb.connect("warehouse/database.db")
    try:
        return con.execute(
            "SELECT COUNT(*) FROM read_parquet(?)", [str(path)]
        ).fetchone()[0]
    finally:
        con.close()
