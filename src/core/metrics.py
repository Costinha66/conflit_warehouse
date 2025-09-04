from dataclasses import dataclass
from pathlib import Path
import hashlib
import time
import duckdb


@dataclass
class DQResult:
    passed: bool
    level: str = "MINOR"  # "MINOR"|"MAJOR"|"CRITICAL"
    metrics: dict = None  # e.g., {"negatives": 0, "iso_unmapped": 12}


def file_sha256(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def parquet_rowcount(p: Path) -> int:
    return (
        duckdb.connect()
        .execute("SELECT COUNT(*) FROM read_parquet(?)", [str(p)])
        .fetchone()[0]
    )


def collect_file_metrics(path: Path) -> dict:
    return {
        "bytes": path.stat().st_size,
        "hash": file_sha256(path),
        "records": parquet_rowcount(path) if path.suffix == ".parquet" else None,
    }


class StageTimer:
    def __enter__(self):
        self.t0 = time.time()
        self.duration_sec = None
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.duration_sec = round(time.time() - self.t0, 3)
