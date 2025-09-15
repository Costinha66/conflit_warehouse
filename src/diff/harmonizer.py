import pandas as pd


def harmonize(df_raw: pd.DataFrame, schema: dict) -> pd.DataFrame:
    rename_map = {}
    casts = {}
    for col in schema["columns"]:
        canonical = col["name"]
        found = next((c for c in col["source"] if c in df_raw.columns), None)
        if found:
            rename_map[found] = canonical
        casts[canonical] = col["type"]

    df = df_raw.rename(columns=rename_map)

    for col in [c["name"] for c in schema["columns"]]:
        if col not in df.columns:
            df[col] = None

    for col, typ in casts.items():
        if typ == "BIGINT":
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        elif typ == "DOUBLE":
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif typ == "STRING":
            df[col] = df[col].astype("string")
        elif typ == "DATE":
            df[col] = pd.to_datetime(df[col], errors="coerce")

    for col in schema.get("lineage_columns", []):
        if col not in df.columns:
            df[col] = None

    df = df[[c["name"] for c in schema["columns"]] + schema.get("lineage_columns", [])]
    return df
