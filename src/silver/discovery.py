import os
import hashlib
from pathlib import Path
from datetime import datetime


def compute_file_hash(path: Path, chunk_size=65536) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(chunk_size):
            h.update(chunk)
    return h.hexdigest()


def discovery(path_warehouse: Path, chunk_size=65536):
    records = []
    for root, _, files in os.walk(path_warehouse):
        for file in files:
            if file.endswith(".parquet"):
                file_path = Path(root) / file
                records.append(
                    {
                        "file_path": str(file_path),
                        "content_hash": compute_file_hash(file_path, chunk_size),
                        "file_size": file_path.stat().st_size,
                        "discovered_at": datetime.now(),
                        "source_partition": next(
                            (p for p in file_path.parts if p.startswith("date=")), None
                        ),
                    }
                )
    return records


# Example usage
bronze_path = Path("/home/faacosta0245695/conflit/conflit_warehouse/data/bronze")
files = discovery(bronze_path)
