from src.core.types import DQResult
from typing import Mapping, Any


def DQBuilderBronze(
    metrics: Mapping[str, Any], cutoff_year: int, start_year: int
) -> DQResult:
    """
    Bronze DQ policy (lean):
    - years: start <= cutoff
    - data presence: records > 0  (flip to >= 0 if you want to allow empty snapshots)
    - file integrity: bytes > 0 and non-empty content hash
    """
    # --- normalize without mutating caller's dict ---
    records = int(metrics.get("records") or 0)
    bytes_ = int(metrics.get("bytes") or 0)
    hash_ = str(metrics.get("hash") or "")
    norm_metrics = {**metrics, "records": records, "bytes": bytes_, "hash": hash_}

    # --- checks & reasons (great for observability) ---
    reasons: list[str] = []
    if start_year > cutoff_year:
        reasons.append("cutoff_invalid")
    # If you want to allow empty snapshots, change to: if records < 0: ...
    if records <= 0:
        reasons.append("zero_records")
    if bytes_ <= 0:
        reasons.append("zero_bytes")
    if not hash_:
        reasons.append("empty_hash")

    passed = len(reasons) == 0
    level = "MINOR" if passed else "CRITICAL"

    return DQResult(passed=passed, level=level, metrics=norm_metrics)
