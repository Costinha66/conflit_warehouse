from datetime import datetime, timezone
import pandas as pd
import hashlib
import json
import math
from typing import List, Dict
import duckdb


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def json_safe(o):
    if isinstance(o, datetime):
        return iso(o)
    return str(o)


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


def _quote_ident(name: str) -> str:
    # minimal identifier quoting
    return '"' + name.replace('"', '""') + '"'


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


class Rejects:
    def __init__(self):
        self._parts = []

    def add(self, df_part: pd.DataFrame):
        if df_part is not None and not df_part.empty:
            # ensure a 'reject_reasons' column exists
            if "reject_reasons" not in df_part.columns:
                df_part = df_part.copy()
                df_part["reject_reasons"] = "unspecified"
            self._parts.append(df_part)

    def concat(self) -> pd.DataFrame:
        if not self._parts:
            return pd.DataFrame()
        cols = list(self._parts[0].columns)
        return pd.concat(self._parts, ignore_index=True)[cols]


def _table_cols(con: duckdb.DuckDBPyConnection, table: str) -> Dict[str, str]:
    """Return {name_lower: type_name_upper} for a table."""
    rows = con.execute(f"PRAGMA table_info({table})").fetchall()
    # schema: cid, name, type, notnull, dflt_value, pk
    return {r[1].lower(): (r[2] or "").upper() for r in rows}


def _count(con: duckdb.DuckDBPyConnection, sql: str, params: list | None = None) -> int:
    return con.execute(sql, params or []).fetchone()[0]
