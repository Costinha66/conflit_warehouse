from __future__ import annotations
from typing import List, Dict, Any
import duckdb
from src.core.miscelannious import _table_cols, _count, now_utc
from src.core.dataclasses import TestResult, DQReport

# ---- type normalization (engine-agnostic) ----
TYPE_SYNONYMS = {
    # strings
    "STRING": "VARCHAR",
    "TEXT": "VARCHAR",
    "CHAR": "VARCHAR",
    "CHARACTER VARYING": "VARCHAR",
    # integers
    "INT": "INTEGER",
    "INT4": "INTEGER",
    "SMALLINT": "INTEGER",  # optional: if you want to accept smaller ints
    "TINYINT": "INTEGER",
    # big integers
    "INT8": "BIGINT",
    "LONG": "BIGINT",
    # floats
    "FLOAT": "DOUBLE",
    "REAL": "DOUBLE",
    "FLOAT4": "DOUBLE",
    "FLOAT8": "DOUBLE",
    # boolean
    "BOOL": "BOOLEAN",
    # timestamps/dates
    "DATETIME": "TIMESTAMP",
    "TIMESTAMP WITHOUT TIME ZONE": "TIMESTAMP",
    "TIMESTAMP WITH TIME ZONE": "TIMESTAMPTZ",  # only map if you actually use tz-aware elsewhere
    # duckdb specifics passthrough
    "UBIGINT": "BIGINT",  # if you don’t want unsigned as “different”
    "UINTEGER": "INTEGER",
}


def normalize_type(t: str | None) -> str:
    if not t:
        return ""
    t_up = t.upper().strip()
    return TYPE_SYNONYMS.get(t_up, t_up)


def check_schema(con, tmp, expected_cols: List[str]) -> TestResult:
    cols = list(_table_cols(con, tmp).keys())
    missing = [c for c in expected_cols if c.lower() not in cols]
    extra = [c for c in cols if c not in [e.lower() for e in expected_cols]]
    status = "passed" if not missing else "failed"
    return TestResult(
        "schema", status, {"missing": missing, "extra": extra, "found": cols}
    )


def check_pk(con, tmp, pk: List[str]) -> List[TestResult]:
    results = []
    # nulls in PK
    null_pred = " OR ".join([f"{k} IS NULL" for k in pk])
    null_cnt = (
        _count(con, f"SELECT COUNT(*) FROM {tmp} WHERE {null_pred};") if pk else 0
    )
    results.append(
        TestResult(
            "pk_not_null",
            "passed" if null_cnt == 0 else "failed",
            {"null_rows": null_cnt},
        )
    )
    # duplicates
    if pk:
        grp = ", ".join(pk)
        dup_cnt = _count(
            con,
            f"SELECT COUNT(*) FROM (SELECT {grp}, COUNT(*) c FROM {tmp} GROUP BY {grp} HAVING COUNT(*)>1);",
        )
    else:
        dup_cnt = 0
    results.append(
        TestResult(
            "pk_unique",
            "passed" if dup_cnt == 0 else "failed",
            {"duplicate_keys": dup_cnt},
        )
    )
    return results


def check_partition(con, tmp, grain: str, partition_key: str) -> List[TestResult]:
    if grain == "year":
        dist = _count(con, f"SELECT COUNT(DISTINCT year) FROM {tmp};")
        res1 = TestResult(
            "partition_single_value",
            "passed" if dist in (0, 1) else "failed",
            {"distinct": dist},
        )
        res2 = TestResult("partition_matches_key", "passed", {})
        if dist == 1:
            min_year = con.execute(f"SELECT MIN(year) FROM {tmp};").fetchone()[0]
            res2 = TestResult(
                "partition_matches_key",
                "passed" if str(min_year) == str(partition_key) else "failed",
                {"found": min_year, "expected": partition_key},
            )
        return [res1, res2]
    # extend for month grain here
    return [
        TestResult("partition_check", "passed", {"note": "no grain check implemented"})
    ]


def check_not_null(con, tmp, cols: List[str]) -> TestResult:
    if not cols:
        return TestResult("not_null", "passed", {"checked": []})
    expr = " + ".join([f"SUM(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END)" for c in cols])
    cnt = _count(con, f"SELECT {expr} FROM {tmp};")
    return TestResult(
        "not_null",
        "passed" if cnt == 0 else "failed",
        {"violations": cnt, "cols": cols},
    )


def check_non_negative(con, tmp, cols: List[str]) -> TestResult:
    if not cols:
        return TestResult("non_negative", "passed", {"checked": []})
    exprs = [f"{c} < 0" for c in cols]
    cnt = _count(con, f"SELECT COUNT(*) FROM {tmp} WHERE {' OR '.join(exprs)};")
    return TestResult(
        "non_negative",
        "passed" if cnt == 0 else "failed",
        {"violations": cnt, "cols": cols},
    )


def check_reconcile(con, tmp, expression: str, name: str) -> TestResult:
    """Fail rows where NOT (expression)."""
    cnt = _count(con, f"SELECT COUNT(*) FROM {tmp} WHERE NOT ({expression});")
    return TestResult(
        f"reconcile:{name}",
        "passed" if cnt == 0 else "failed",
        {"violations": cnt, "expr": expression},
    )


def check_foreign_key(
    con, tmp, fk_col: str, ref_table: str, ref_key: str
) -> TestResult:
    sql = f"""
    SELECT COUNT(*)
    FROM {tmp} t
    LEFT JOIN {ref_table} d ON t.{fk_col} = d.{ref_key}
    WHERE d.{ref_key} IS NULL
    """
    cnt = _count(con, sql)
    return TestResult(
        f"ri:{fk_col}->{ref_table}.{ref_key}",
        "passed" if cnt == 0 else "failed",
        {"violations": cnt},
    )


def check_type_alignment(con, tmp, expected_types: Dict[str, str]) -> TestResult:
    actual = _table_cols(con, tmp)  # {col_lower: TYPE}
    mismatches = {}

    for col, exp in expected_types.items():
        exp_norm = normalize_type(exp)
        act_raw = actual.get(col.lower(), "")
        act_norm = normalize_type(act_raw)

        # only flag when normalized types differ
        if exp_norm and act_norm and exp_norm != act_norm:
            mismatches[col] = {"expected": exp_norm, "found": act_norm}

    status = "passed" if not mismatches else "failed"
    return TestResult("types_match", status, {"mismatches": mismatches})


def run_dq_tests(
    con: duckdb.DuckDBPyConnection,
    tmp_table: str,
    spec: Dict[str, Any],
    partition_key: str,
    layer: str = "gold",
) -> DQReport:
    """
    Execute DQ based on the entity YAML.
    - For Gold: expects spec["columns"], spec["contract"]["tests"], spec["contract"]["foreign_keys"], etc.
    - For Silver: adapt by passing layer="silver" and reading appropriate spec shape if you reuse this function.
    """
    results: List[TestResult] = []

    # 1) Schema expectations
    if layer == "gold":
        expected_cols = [c["name"] for c in spec["columns"]]
        lineage_cols = [
            lc["name"] for lc in spec["contract"].get("lineage_columns", [])
        ]
        expected_cols = expected_cols  # lineage columns should already be in tmp if you stamp them before validate
        grain = spec.get("grain", "year")
        types = {c["name"]: c.get("type") for c in spec["columns"]}
    else:
        contract_cols = [c["name"] for c in spec["contract"]["columns"]]
        extra_cols = spec.get("canonicalizer", {}).get("output", {}).get("extra", [])
        lineage_cols = spec["contract"].get("lineage", [])
        expected_cols = contract_cols + extra_cols + lineage_cols + ["row_hash"]
        grain = spec.get("grain", "year")
        types = {c["name"]: c.get("type") for c in spec["contract"]["columns"]}

    results.append(check_schema(con, tmp_table, expected_cols))

    # 2) Primary key checks
    pk = spec["contract"]["primary_key"]
    results.extend(check_pk(con, tmp_table, pk))

    # 3) Partition consistency
    results.extend(check_partition(con, tmp_table, grain, partition_key))

    # 4) NOT NULL checks (business + lineage)
    not_null_cols = []
    if layer == "gold":
        # gather business not-null
        not_null_cols.extend(
            [c["name"] for c in spec["columns"] if not c.get("nullable", True)]
        )
        # lineage not-null
        not_null_cols.extend(
            [lc["name"] for lc in spec["contract"].get("lineage_columns", [])]
        )
    else:
        not_null_cols.extend(
            [
                c["name"]
                for c in spec["contract"]["columns"]
                if not c.get("nullable", True)
            ]
        )
    results.append(check_not_null(con, tmp_table, list(dict.fromkeys(not_null_cols))))

    # 5) Non-negative checks (from contract.tests in Gold)
    nonneg_cols: List[str] = []
    for t in spec["contract"].get("tests", []):
        if isinstance(t, dict) and "non_negative" in t:
            nonneg_cols.extend(t["non_negative"])
    results.append(check_non_negative(con, tmp_table, nonneg_cols))

    # 6) Reconcile checks
    for t in spec["contract"].get("tests", []):
        if isinstance(t, dict) and "reconcile" in t:
            name = t["reconcile"].get("name", "reconcile")
            expr = t["reconcile"]["expression"]
            results.append(check_reconcile(con, tmp_table, expr, name))

    # 7) FK / RI checks
    for fk in spec["contract"].get("foreign_keys", []):
        results.append(
            check_foreign_key(
                con,
                tmp_table,
                fk["column"],
                fk["references"].split("(")[0],
                fk["references"].split("(")[1].rstrip(")"),
            )
        )

    # 8) Type alignment (optional but useful)
    results.append(check_type_alignment(con, tmp_table, types))

    # Finalize status
    overall = "passed" if all(r.status == "passed" for r in results) else "failed"
    summary = {
        "layer": layer,
        "tmp_table": tmp_table,
        "partition_key": partition_key,
        "tests_total": len(results),
        "tests_failed": sum(1 for r in results if r.status == "failed"),
        "at": now_utc().isoformat(),
    }
    return DQReport(status=overall, results=results, summary=summary)
