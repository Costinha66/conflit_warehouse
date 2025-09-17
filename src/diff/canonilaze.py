from src.diff.utils import (
    _apply_normalize,
    _cast,
    read_table,
    _normalize_on,
    stable_hash,
    ensure_columns,
    Rejects,
)
import pandas as pd
from typing import Dict, Any, Tuple, List


def apply_harmonization(
    df_raw: pd.DataFrame,
    input_spec: Dict[str, Any],
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    """
    input_spec (from YAML) example:
      columns:
        iso3:         {sources: [ISO3, iso3], type: string, required: true, normalize: [strip, upper]}
        year:         {sources: [Year, year], type: int, required: true}
      lineage: [_source_id, _bronze_file, _ingest_run_id, _transform_version]
      defaults:
        _source_id: "unhcr"     # optional constants to fill if missing
    """
    cols_spec: Dict[str, Any] = input_spec["columns"]
    lineage_cols: List[str] = input_spec.get("lineage", [])
    defaults: Dict[str, Any] = input_spec.get("defaults", {})

    # 1) Build a selection for each canonical column from the first available source column
    df = pd.DataFrame(index=df_raw.index)
    source_used = {}  # canonical -> chosen source name

    for canonical, spec in cols_spec.items():
        sources = spec.get("sources", [canonical])
        # Prefer the first present source column
        chosen = next((c for c in sources if c in df_raw.columns), None)
        if chosen is None:
            # Create empty column now; will be validated later
            df[canonical] = pd.NA
            source_used[canonical] = None
            continue

        s = df_raw[chosen]
        # 2) normalize
        s = _apply_normalize(s, spec.get("normalize"))
        # 3) cast
        s = _cast(s, spec.get("type", "string"))

        df[canonical] = s
        source_used[canonical] = chosen

    # 4) add lineage placeholders/constants (don’t cast here; types are light)
    for col in lineage_cols:
        if col in df_raw.columns:
            df[col] = df_raw[col]
        else:
            df[col] = defaults.get(col, pd.NA)

    # 5) required checks → collect rejects (structure-level, not business semantics)
    required_missing = []
    for canonical, spec in cols_spec.items():
        if spec.get("required", False):
            missing_mask = df[canonical].isna()
            if missing_mask.any():
                required_missing.append((canonical, missing_mask))

    # assemble rejects frame (if any)
    rejects_rows = pd.Series(False, index=df.index)
    reasons = pd.Series("", index=df.index, dtype="string")
    for canonical, mask in required_missing:
        rejects_rows = rejects_rows | mask
        reasons = reasons.where(
            ~mask,
            other=(
                reasons.where(~mask, "")
                + (";" if reasons[mask].ne("").any() else "")
                + f"missing_required:{canonical}"
            ),
        )

    rejects = pd.DataFrame()
    if rejects_rows.any():
        rejects = df.loc[rejects_rows, [*cols_spec.keys(), *lineage_cols]].copy()
        rejects["reject_reasons"] = reasons.loc[rejects_rows].fillna("missing_required")

    # 6) keep only non-rejected rows
    df_h = df.loc[~rejects_rows].copy()

    # 7) reorder: canonical columns first, then lineage
    df_h = df_h[[*cols_spec.keys(), *lineage_cols]]

    # 8) simple diagnostics you can log
    dq = {
        "raw_cols": list(df_raw.columns),
        "source_used": source_used,
        "rows_in": len(df_raw),
        "rows_out": len(df_h),
        "rows_rejected": int(rejects_rows.sum()),
    }

    return df_h, rejects, dq


def join_dimensions(
    df: pd.DataFrame,
    dim: pd.DataFrame,
    *,
    how: str = "left",
    on,
    select: list | None = None,
    required: bool = False,
    reason: str = "",
    expect_one_to_one: bool = True,
    alias: str | None = None,  # NEW
    select_rename: dict | None = None,  # NEW {dim_col: out_col}
):
    left_keys, right_keys = _normalize_on(on, df.columns, dim.columns)
    if select is None:
        select = []
    if alias and select_rename:
        raise ValueError("Use either alias or select_rename, not both.")

    # project
    need = list(dict.fromkeys(right_keys + select))
    dim_proj = dim[need].copy()

    # uniqueness guard
    dup_mask = dim_proj.duplicated(subset=right_keys, keep=False)
    if dup_mask.any():
        if expect_one_to_one:
            ex = dim_proj.loc[dup_mask, right_keys].drop_duplicates().head(5)
            raise ValueError(
                f"Dimension not unique on keys {right_keys}. Examples:\n{ex}"
            )
        dim_proj = dim_proj.drop_duplicates(subset=right_keys, keep="first")

    # rename selected columns to output names
    rename_map = {}
    if select_rename:
        # validate
        for c in select:
            out = select_rename.get(c)
            if not out:
                raise ValueError(f"select_rename missing target for '{c}'")
            rename_map[c] = out
    elif alias:
        rename_map = {c: f"{alias}{c}" for c in select}

    # collision check
    collisions = [new for old, new in rename_map.items() if new in df.columns]
    if collisions:
        raise ValueError(f"Join would overwrite existing columns: {collisions}")

    dim_proj = dim_proj.rename(columns=rename_map)

    df_before = len(df)
    df_joined = df.merge(
        dim_proj,
        how=how,
        left_on=left_keys,
        right_on=right_keys,
        suffixes=("", "_dim"),
        indicator=True,
        copy=False,
    )

    # many-to-many/explosion guard (only meaningful if dim unique)
    if expect_one_to_one and len(df_joined) != df_before:
        raise ValueError(
            f"Join produced {len(df_joined)} rows from {df_before}. Likely many-to-many."
        )

    # no-match handling
    if how != "left":
        raise ValueError("Dimensions should be left-joined.")
    no_match = df_joined["_merge"].eq("left_only")
    rejects = pd.DataFrame()
    if required and no_match.any():
        rejects = df_joined.loc[no_match, df.columns].copy()
        rejects["reject_reasons"] = reason or "dim_not_found"
        df_joined = df_joined.loc[~no_match].copy()

    # drop join helper cols (right keys) and indicator
    df_joined = df_joined.drop(columns=["_merge"] + right_keys, errors="ignore")

    metrics = {
        "left_rows": df_before,
        "joined_rows": len(df_joined),
        "no_match": int(no_match.sum()),
        "rejected": int(no_match.sum()) if required else 0,
        "left_keys": left_keys,
        "right_keys": right_keys,
        "selected_cols": select,
        "alias": alias,
        "select_rename": select_rename or {},
    }
    return df_joined, rejects, metrics


def filter_rows(df: pd.DataFrame, rule, action: str, reason: str):
    """
    rule: bool Series aligned with df, or a pandas-query string
    action: 'keep'  -> keep rows WHERE rule is True; reject the rest
            'reject'-> reject rows WHERE rule is True; keep the rest
    """
    if isinstance(rule, str):
        mask = (
            df.query(rule)
            .index.to_series()
            .reindex(df.index, fill_value=False)
            .astype(bool)
        )
    else:
        mask = rule.astype(bool)

    if action == "keep":
        keep_mask = mask
        rej_mask = ~mask
    elif action == "reject":
        keep_mask = ~mask
        rej_mask = mask
    else:
        raise ValueError("action must be 'keep' or 'reject'")

    kept = df.loc[keep_mask].copy()
    rejected = df.loc[rej_mask].copy()
    if not rejected.empty:
        rejected = rejected.copy()
        rejected["reject_reasons"] = reason
    return kept, rejected


def canonicalize(df_raw: pd.DataFrame, spec: Dict[str, Any]):
    # 0) Harmonize
    df, rej_h, dq_h = apply_harmonization(df_raw, spec["input"])
    rc = Rejects()
    rc.add(rej_h)

    # 1) Dim joins
    for d in spec.get("dims", []):
        dim_df = read_table(d["path"])  # your helper
        df, rej, dq_join = join_dimensions(
            df,
            dim_df,
            how="left",
            on=d["on"],
            select=d["select"],
            alias=d["alias"],
            required=d.get("required", False),
            reason=f"{d['name']}_not_found",
            expect_one_to_one=True,
        )
        rc.add(rej)
    dq_h.extend(dq_join)

    # 3) Rules
    for r in spec.get("rules", []):
        df, rej = filter_rows(
            df, rule=r["when"], action=r["action"], reason=r["reject_reason"]
        )
        rc.add(rej)

    # 5) Row hash
    df["row_hash"] = stable_hash(df[spec["row_hash"]["columns"]])

    # 6) Final projection
    final_cols = (
        spec["output"]["columns"] + spec["output"].get("lineage", []) + ["row_hash"]
    )
    missing_req = [c for c in spec["output"]["columns"] if c not in df.columns]
    assert not missing_req, f"Missing required output columns: {missing_req}"

    df = ensure_columns(df, final_cols)

    return df, rc.concat(), dq_h
