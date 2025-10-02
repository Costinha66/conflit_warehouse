# src/gold/sql_engine.py
from __future__ import annotations
from typing import Dict, Any, Tuple, List
import duckdb


def quote_ident(name: str) -> str:
    # Minimal identifier quoting; extend if you allow weird names
    return '"' + name.replace('"', '""') + '"'


def _get_source_columns(con: duckdb.DuckDBPyConnection, source: str) -> set:
    """
    Return a set of lowercased column names for the <schema.table> 'source'.
    """
    if "." not in source:
        schema, table = "main", source
    else:
        schema, table = source.split(".", 1)
    rows = con.execute(
        """
        SELECT lower(column_name)
        FROM information_schema.columns
        WHERE lower(table_schema) = lower(?)
          AND lower(table_name)   = lower(?)
        """,
        [schema, table],
    ).fetchall()
    return {r[0] for r in rows}


def generate_transform_sql(
    con: duckdb.DuckDBPyConnection,
    spec: Dict[str, Any],
    partition: str | int,
) -> Tuple[str, List[Any]]:
    """
    Build SQL from a Gold YAML 'transform' block.
    Assumes yearly grain (filters on 'year = ?')
    """
    tf = spec["transform"]
    source = tf["source"]
    select_map: Dict[str, str] = tf.get("select_map", {})
    derivations: Dict[str, str] = tf.get("derivations", {})
    defaults: Dict[str, Any] = tf.get("defaults", {})
    types: Dict[str, str] = tf.get("types", {})
    order: List[str] = tf.get("order", [])

    # Inspect source schema to decide if defaults are needed
    src_cols = _get_source_columns(con, source)

    # 1) CTE: src (slice the source by partition)
    # NOTE: adjust filter if your grain is not 'year'
    src_cte = f"""
    src AS (
      SELECT *
      FROM {source}
      WHERE year = ?
    )
    """

    # 2) CTE: base (apply select_map + defaults)
    # - Emit deterministic alias list for downstream derivations to reference
    base_cols_sql = []

    # select_map: target = source_col
    for tgt, src in select_map.items():
        base_cols_sql.append(f"{quote_ident(src)} AS {quote_ident(tgt)}")

    # defaults: only synthesize if column isn't present in source
    # If present, passthrough the real column as is (so derivations can use it)
    for tgt, defval in defaults.items():
        if tgt.lower() in src_cols or tgt in select_map.values():
            # Already exists from source or mapped; just pass it through if not mapped
            if tgt not in select_map:
                base_cols_sql.append(f"{quote_ident(tgt)} AS {quote_ident(tgt)}")
        else:
            # synthesize default literal, untyped (we cast in the final step)
            lit = f"{defval}" if isinstance(defval, (int, float)) else f"'{defval}'"
            base_cols_sql.append(f"{lit} AS {quote_ident(tgt)}")

    base_cte = f"""
    base AS (
      SELECT
        {",\n        ".join(base_cols_sql)}
      FROM src
    )
    """

    # 3) Final SELECT: add derivations, cast types, and enforce column order
    # We can reference base aliases in this SELECT.
    final_select_cols = []

    # Build a name->expr dict the final SELECT will use
    # Start with pass-through of everything we promised in 'order'
    passthrough_set = set(select_map.keys()) | set(defaults.keys())

    # derivations: expr AS name
    for name, expr in derivations.items():
        # keep expr as provided (must be valid DuckDB SQL)
        final_select_cols.append(f"{expr} AS {quote_ident(name)}")
        passthrough_set.add(name)

    # Ensure passthrough columns are selected too (if in order)
    for name in select_map.keys():
        if name not in derivations:
            final_select_cols.append(f"{quote_ident(name)} AS {quote_ident(name)}")
    for name in defaults.keys():
        if name not in derivations and name not in select_map:
            final_select_cols.append(f"{quote_ident(name)} AS {quote_ident(name)}")

    # Wrap with types (cast) and ordering
    # Build a small wrapper SELECT to cast+order deterministically
    # First, create a subselect with everything
    inner_select = f"SELECT {', '.join(final_select_cols)} FROM base"

    # Now apply types + order
    if not order:
        # If no explicit order, keep insertion order
        order = list(
            dict.fromkeys(
                list(select_map.keys())
                + list(defaults.keys())
                + list(derivations.keys())
            )
        )

    casted_cols = []
    for col in order:
        ident = quote_ident(col)
        if col in types:
            casted_cols.append(f"CAST({ident} AS {types[col]}) AS {ident}")
        else:
            casted_cols.append(f"{ident} AS {ident}")

    final_sql = f"""
    WITH
    {src_cte},
    {base_cte}
    SELECT
      {",\n      ".join(casted_cols)}
    FROM (
      {inner_select}
    ) x
    """

    params = [int(partition)]
    return final_sql, params
