import pandas as pd
import hashlib
import json
import math
from typing import List, Iterator, Optional
import os
import fnmatch
from pathlib import Path


def to_int(series):
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def _norm_val(x):
    # normalize NAs
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return ""  # null token; keep stable
    # timestamps / dates
    if hasattr(x, "isoformat"):
        return x.isoformat()
    # dict/list → stable JSON
    if isinstance(x, (dict, list)):
        return json.dumps(x, sort_keys=True, separators=(",", ":"))
    # pandas NA types
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    # everything else → str
    return str(x)


def stable_hash(df_subset: pd.DataFrame) -> pd.Series:
    if not isinstance(df_subset, pd.DataFrame):
        raise TypeError("stable_hash expects a DataFrame with the columns to hash.")

    cols = list(df_subset.columns)  # deterministic column order
    # convert each cell to a normalized string
    normalized = df_subset[cols].map(_norm_val)
    # join row-wise
    joined = normalized.agg("|".join, axis=1)
    # hash (returns Series aligned to original index)
    return joined.apply(lambda s: hashlib.sha256(s.encode("utf-8")).hexdigest())


def ensure_columns(df: pd.DataFrame, cols_order: list) -> pd.DataFrame:
    for c in cols_order:
        if c not in df.columns:
            df[c] = pd.NA
    return df[cols_order]


_NORMALIZE = {
    "strip": lambda s: s.astype("string").str.strip(),
    "upper": lambda s: s.astype("string").str.upper(),
    "lower": lambda s: s.astype("string").str.lower(),
    "title": lambda s: s.astype("string").str.title(),
}


def _cast(series: pd.Series, typ: str) -> pd.Series:
    if typ == "int":
        return pd.to_numeric(series, errors="coerce").astype("Int64")
    if typ in ("float", "double"):
        return pd.to_numeric(series, errors="coerce").astype("Float64")
    if typ in ("string", "text"):
        return series.astype("string")
    if typ in ("date", "datetime"):
        # Expect ISO strings or yyyy-mm-dd; adjust if you have different formats in YAML
        return pd.to_datetime(series, errors="coerce", utc=(typ == "datetime"))
    if typ == "bool":
        # very light boolean caster; extend if needed
        return (
            series.astype("string")
            .str.lower()
            .map({"true": True, "false": False, "1": True, "0": False})
            .astype("boolean")
        )
    raise ValueError(f"Unsupported type in schema: {typ}")


def _apply_normalize(series: pd.Series, ops: List[str] | None) -> pd.Series:
    if not ops:
        return series
    out = series
    for op in ops:
        if op not in _NORMALIZE:
            raise ValueError(f"Unknown normalize op: {op}")
        out = _NORMALIZE[op](out)
    return out


def read_table(path):
    return pd.read_parquet(path)


def _normalize_on(on, df_cols, dim_cols):
    if isinstance(on, (list, tuple)):
        left_keys = right_keys = list(on)
    elif isinstance(on, dict):
        left_keys = on.get("left") or on.get("l") or on.get("left_on")
        right_keys = on.get("right") or on.get("r") or on.get("right_on")
        if isinstance(left_keys, str):
            left_keys = [left_keys]
        if isinstance(right_keys, str):
            right_keys = [right_keys]
    else:
        left_keys = right_keys = [on]
    # existence checks
    for c in left_keys:
        if c not in df_cols:
            raise KeyError(f"Left key '{c}' missing.")
    for c in right_keys:
        if c not in dim_cols:
            raise KeyError(f"Right key '{c}' missing.")
    return left_keys, right_keys


def _sha256(path: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while b := f.read(chunk):
            h.update(b)
    return h.hexdigest()


def _iter_parquet(root: Path) -> Iterator[Path]:
    for r, _, files in os.walk(root):
        for f in files:
            if f.endswith(".parquet"):
                yield Path(r) / f


def _extract_source_id(fp: Path) -> Optional[str]:
    parts = fp.parts
    if "bronze" in parts:
        i = parts.index("bronze")
        return parts[i + 1] if i + 1 < len(parts) else None
    return None


def _glob_to_regex(pat: str) -> str:
    """Translate a glob to a Python/DuckDB-compatible regex (anchored)."""
    rx = fnmatch.translate(pat)
    if not rx.startswith("(?s:"):
        return rx
    inner = rx[len("(?s:") : -4]  # strip (?s:  )\Z
    return "^" + inner + "$"
