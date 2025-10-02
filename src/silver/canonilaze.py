from src.core.miscelannious import (
    read_table,
    _normalize_on,
    stable_hash,
    ensure_columns,
    Rejects,
)
import pandas as pd
from typing import Dict, Any, List


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


def canonicalize(df: pd.DataFrame, spec: Dict[str, Any], logger):
    rc = Rejects()
    join_metrics: List[Dict[str, Any]] = []

    dims = spec.get("canonicalizer", {}).get("dims", [])
    for i, d in enumerate(dims, start=1):
        dim_df = read_table(d["path"])
        try:
            left_keys, right_keys = _normalize_on(
                d["on_keys"], df.columns, dim_df.columns
            )

            next_df, rej, m = join_dimensions(
                df,
                dim_df,
                how="left",
                on=d["on_keys"],
                select=d.get("select", []),
                alias=d.get("alias"),
                required=d.get("required", False),
                reason=f"{d.get('name','dim')}_not_found",
                expect_one_to_one=True,
            )

            # enrich metrics
            m.update(
                {
                    "dim_index": i,
                    "dim_name": d.get("name"),
                    "path": d.get("path"),
                    "alias": d.get("alias"),
                    "left_keys": left_keys,
                    "right_keys": right_keys,
                    "rows_before": len(df),
                    "rows_after": len(next_df),
                }
            )
            join_metrics.append(m)
            rc.add(rej)

            print(rej)

            unmatched = m.get("unmatched_left", 0)
            if unmatched and d.get("required", False):
                logger.warning("canonicalize.required_dim_unmatched")

            if len(next_df) != len(df):
                logger.error("canonicalize.rowcount_changed")
                raise ValueError(m)

            # COMMIT the step
            df = next_df
            logger.info("canonicalize.dim_committed")

        except Exception as e:
            # Build rich context for debugging
            ctx = {
                "dim_index": i,
                "dim_name": d.get("name"),
                "path": d.get("path"),
                "on_keys": d.get("on_keys"),
                "select": d.get("select"),
                "alias": d.get("alias"),
                "df_shape": tuple(df.shape),
                "df_cols_sample": list(df.columns)[:40],
                "dim_shape": tuple(dim_df.shape),
                "dim_cols_sample": list(dim_df.columns)[:40],
            }
            # Try to add resolved keys, if possible
            try:
                lk, rk = _normalize_on(d["on_keys"], df.columns, dim_df.columns)
                ctx["left_keys"] = lk
                ctx["right_keys"] = rk
            except Exception as e_keys:
                ctx["left_keys"] = ctx["right_keys"] = (
                    f"<failed to resolve keys: {e_keys}>"
                )

            # Log and raise with context
            logger.exception("join_dimensions failed", extra=ctx)
            raise RuntimeError(
                f"join_dimensions failed for dim '{d.get('name')}' "
                f"(path={d.get('path')}, on={d.get('on')}, select={d.get('select')}, alias={d.get('alias')}). "
                f"Left shape={ctx['df_shape']}, right shape={ctx['dim_shape']}. "
                f"See logs for column samples and key resolution details. Original error: {e}"
            ) from e

    # 3) Rules
    for r in spec.get("canonicalizer", {}).get("rules", []):
        df_filtered, rej = filter_rows(
            next_df, rule=r["when"], action=r["action"], reason=r["reject_reason"]
        )
        rc.add(rej)

    # 5) Row hash
    hash_cols = spec["canonicalizer"]["row_hash"]["columns"]
    df_filtered["row_hash"] = stable_hash(df_filtered[hash_cols])

    # 6) Final projection
    lineage_cols = spec["contract"].get("lineage", [])
    contract_cols = [c["name"] for c in spec["contract"]["columns"]]
    extra_cols = spec["canonicalizer"]["output"].get("extra", [])
    final_cols = contract_cols + extra_cols + lineage_cols + ["row_hash"]

    missing_req = [c for c in hash_cols if c not in df_filtered.columns]
    assert not missing_req, f"Missing required output columns: {missing_req}"

    df_final = ensure_columns(df_filtered, final_cols)

    # Return all join metrics (not just the last one)
    return df_final, rc.concat(), {"joins": join_metrics}
