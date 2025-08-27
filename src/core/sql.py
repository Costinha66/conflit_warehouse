import duckdb


def conn(db_path: str | None = None):
    return duckdb.connect(database=db_path or ":memory:")


def read_parquet_glob(c, glob_pattern: str):
    return c.execute(f"SELECT * FROM read_parquet('{glob_pattern}')").df()
