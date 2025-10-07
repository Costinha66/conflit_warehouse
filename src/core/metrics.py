from pathlib import Path
from typing import Callable, Optional
from src.core.hasing import file_sha256


def collect_file_metrics(
    path: Path, parquet_rowcount: Optional[Callable[[Path], Optional[int]]] = None
) -> dict:
    rowcount = None
    if parquet_rowcount and path.suffix == ".parquet":
        rowcount = parquet_rowcount(path)
    return {
        "bytes": path.stat().st_size,
        "hash": file_sha256(path),
        "records": rowcount,
    }
