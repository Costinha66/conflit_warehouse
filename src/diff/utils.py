import pandas as pd
import hashlib
from typing import List


def to_int(series):
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def stable_hash(df_subset: pd.DataFrame) -> pd.Series:
    # 1) enforce deterministic column order
    cols = list(df_subset.columns)
    s = (
        df_subset[cols]
        .astype(object)  # avoid pandas NA casting weirdness
        .where(pd.notna(df_subset), None)  # normalize NaNs/NaTs
        .applymap(lambda x: x.isoformat() if hasattr(x, "isoformat") else x)
        .astype(str)
        .agg("|".join, axis=1)
    )
    return s.apply(lambda x: hashlib.sha256(x.encode("utf-8")).hexdigest())


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
