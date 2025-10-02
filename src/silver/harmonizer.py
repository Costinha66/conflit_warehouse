import pandas as pd


def apply_harmonize(df_raw: pd.DataFrame, schema: dict):
    rename_map, casts = {}, {}

    for col in schema["contract"]["columns"]:
        name = col["name"]
        srcs = col.get("source", []) or []  # <-- make source optional

        found = next((s for s in srcs if s in df_raw.columns), None)
        if found:
            rename_map[found] = name
        else:
            continue

        # record the intended type (we may cast only if the column exists now)
        casts[name] = col.get("type")

    df = df_raw.rename(columns=rename_map)

    for col in (c["name"] for c in schema["contract"]["columns"] if c.get("source")):
        if col not in df.columns:
            df[col] = None

    for name, typ in casts.items():
        if name not in df.columns:
            continue
        if typ == "BIGINT":
            df[name] = pd.to_numeric(df[name], errors="coerce").astype("Int64")
        elif typ == "DOUBLE":
            df[name] = pd.to_numeric(df[name], errors="coerce")
        elif typ == "STRING":
            df[name] = df[name].astype("string")
        elif typ == "DATE":
            df[name] = pd.to_datetime(df[name], errors="coerce")

    # lineage defaults
    for ln in schema["contract"].get("lineage", []):
        if ln not in df.columns:
            df[ln] = None

    # keep the harmonizer’s output limited to raw-sourced + placeholders;
    # the canonicalizer will fill origin_country_id/dest_country_id via the dim join.
    ordered = [
        c["name"] for c in schema["contract"]["columns"] if c.get("source")
    ] + schema["contract"].get("lineage", [])
    df = df[ordered]
    return df, pd.DataFrame([]), pd.DataFrame([])
