import pytest
import duckdb
import pandas as pd

from src.core.dq.dq import (
    normalize_type,
    check_schema,
    check_pk,
    check_partition,
    check_not_null,
    check_non_negative,
    check_type_alignment,
)
from src.silver.processor import diff_counts_and_hash


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def con():
    c = duckdb.connect()
    yield c
    c.close()


def _make_tmp(con, df: pd.DataFrame, name: str = "test_tmp") -> str:
    con.register("_df", df)
    con.execute(f"CREATE OR REPLACE TEMP TABLE {name} AS SELECT * FROM _df;")
    con.unregister("_df")
    return name


# ---------------------------------------------------------------------------
# normalize_type
# ---------------------------------------------------------------------------


def test_normalize_type_synonyms():
    assert normalize_type("STRING") == "VARCHAR"
    assert normalize_type("INT") == "INTEGER"
    assert normalize_type("FLOAT") == "DOUBLE"
    assert normalize_type("BOOL") == "BOOLEAN"
    assert normalize_type("DATETIME") == "TIMESTAMP"


def test_normalize_type_passthrough():
    assert normalize_type("VARCHAR") == "VARCHAR"
    assert normalize_type("BIGINT") == "BIGINT"
    assert normalize_type("DOUBLE") == "DOUBLE"


def test_normalize_type_empty():
    assert normalize_type(None) == ""
    assert normalize_type("") == ""


# ---------------------------------------------------------------------------
# check_schema
# ---------------------------------------------------------------------------


def test_check_schema_pass(con):
    t = _make_tmp(con, pd.DataFrame({"a": [1], "b": ["x"]}))
    r = check_schema(con, t, ["a", "b"])
    assert r.status == "passed"
    assert r.details["missing"] == []


def test_check_schema_missing_column(con):
    t = _make_tmp(con, pd.DataFrame({"a": [1]}))
    r = check_schema(con, t, ["a", "b"])
    assert r.status == "failed"
    assert "b" in r.details["missing"]


def test_check_schema_extra_columns_not_fail(con):
    t = _make_tmp(con, pd.DataFrame({"a": [1], "b": ["x"], "c": [True]}))
    r = check_schema(con, t, ["a", "b"])
    assert r.status == "passed"
    assert "c" in r.details["extra"]


# ---------------------------------------------------------------------------
# check_pk
# ---------------------------------------------------------------------------


def test_check_pk_clean(con):
    t = _make_tmp(con, pd.DataFrame({"id": [1, 2, 3], "v": ["a", "b", "c"]}))
    statuses = {r.name: r.status for r in check_pk(con, t, ["id"])}
    assert statuses["pk_not_null"] == "passed"
    assert statuses["pk_unique"] == "passed"


def test_check_pk_duplicate(con):
    t = _make_tmp(con, pd.DataFrame({"id": [1, 1, 3]}))
    statuses = {r.name: r.status for r in check_pk(con, t, ["id"])}
    assert statuses["pk_unique"] == "failed"
    assert statuses["pk_not_null"] == "passed"


def test_check_pk_null(con):
    t = _make_tmp(con, pd.DataFrame({"id": pd.array([1, None, 3], dtype="Int64")}))
    statuses = {r.name: r.status for r in check_pk(con, t, ["id"])}
    assert statuses["pk_not_null"] == "failed"


def test_check_pk_empty_pk_list(con):
    t = _make_tmp(con, pd.DataFrame({"id": [1, 2]}))
    results = check_pk(con, t, [])
    assert all(r.status == "passed" for r in results)


# ---------------------------------------------------------------------------
# check_partition
# ---------------------------------------------------------------------------


def test_check_partition_year_pass(con):
    t = _make_tmp(con, pd.DataFrame({"year": [2020, 2020], "v": [1, 2]}))
    statuses = {r.name: r.status for r in check_partition(con, t, "year", "2020")}
    assert statuses["partition_single_value"] == "passed"
    assert statuses["partition_matches_key"] == "passed"


def test_check_partition_wrong_key(con):
    t = _make_tmp(con, pd.DataFrame({"year": [2020, 2020]}))
    statuses = {r.name: r.status for r in check_partition(con, t, "year", "2019")}
    assert statuses["partition_matches_key"] == "failed"


def test_check_partition_multiple_years(con):
    t = _make_tmp(con, pd.DataFrame({"year": [2020, 2021]}))
    statuses = {r.name: r.status for r in check_partition(con, t, "year", "2020")}
    assert statuses["partition_single_value"] == "failed"


# ---------------------------------------------------------------------------
# check_not_null
# ---------------------------------------------------------------------------


def test_check_not_null_pass(con):
    t = _make_tmp(con, pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}))
    assert check_not_null(con, t, ["a", "b"]).status == "passed"


def test_check_not_null_fail(con):
    t = _make_tmp(con, pd.DataFrame({"a": pd.array([1, None], dtype="Int64")}))
    r = check_not_null(con, t, ["a"])
    assert r.status == "failed"
    assert r.details["violations"] == 1


def test_check_not_null_empty_cols(con):
    t = _make_tmp(con, pd.DataFrame({"a": [1]}))
    assert check_not_null(con, t, []).status == "passed"


# ---------------------------------------------------------------------------
# check_non_negative
# ---------------------------------------------------------------------------


def test_check_non_negative_pass(con):
    t = _make_tmp(con, pd.DataFrame({"refugees": [0, 100, 200]}))
    assert check_non_negative(con, t, ["refugees"]).status == "passed"


def test_check_non_negative_fail(con):
    t = _make_tmp(con, pd.DataFrame({"refugees": [100, -1, 50]}))
    r = check_non_negative(con, t, ["refugees"])
    assert r.status == "failed"
    assert r.details["violations"] == 1


def test_check_non_negative_empty_cols(con):
    t = _make_tmp(con, pd.DataFrame({"refugees": [1]}))
    assert check_non_negative(con, t, []).status == "passed"


# ---------------------------------------------------------------------------
# check_type_alignment
# ---------------------------------------------------------------------------


def test_check_type_alignment_pass(con):
    t = _make_tmp(con, pd.DataFrame({"n": pd.array([1], dtype="Int32")}))
    r = check_type_alignment(con, t, {"n": "INTEGER"})
    assert r.status == "passed"


def test_check_type_alignment_mismatch(con):
    t = _make_tmp(con, pd.DataFrame({"n": [1.5]}))  # DOUBLE
    r = check_type_alignment(con, t, {"n": "INTEGER"})
    assert r.status == "failed"
    assert "n" in r.details["mismatches"]


# ---------------------------------------------------------------------------
# diff_counts_and_hash
# ---------------------------------------------------------------------------

_SPEC = {"grain": "year", "contract": {"primary_key": ["country"]}}


def _seed_silver(con, rows: list[dict]):
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    con.execute("DROP TABLE IF EXISTS silver.test_entity;")
    if rows:
        df = pd.DataFrame(rows)
        con.register("_seed", df)
        con.execute("CREATE TABLE silver.test_entity AS SELECT * FROM _seed;")
        con.unregister("_seed")
    else:
        con.execute(
            "CREATE TABLE silver.test_entity"
            " (country VARCHAR, year INTEGER, row_hash VARCHAR);"
        )


def test_diff_first_load_all_inserts(con):
    _seed_silver(con, [])
    df_new = pd.DataFrame(
        {"country": ["AFG", "SYR"], "year": [2020, 2020], "row_hash": ["aaa", "bbb"]}
    )
    ins, upd, dlt, h = diff_counts_and_hash(con, df_new, "test_entity", "2020", _SPEC)
    assert ins == 2
    assert upd == 0
    assert dlt == 0
    assert h is not None


def test_diff_partition_removed_all_deletes(con):
    _seed_silver(
        con,
        [
            {"country": "AFG", "year": 2020, "row_hash": "aaa"},
            {"country": "SYR", "year": 2020, "row_hash": "bbb"},
        ],
    )
    df_new = pd.DataFrame(
        {
            "country": pd.Series([], dtype=str),
            "year": pd.Series([], dtype="Int64"),
            "row_hash": pd.Series([], dtype=str),
        }
    )
    ins, upd, dlt, h = diff_counts_and_hash(con, df_new, "test_entity", "2020", _SPEC)
    assert ins == 0
    assert upd == 0
    assert dlt == 2
    assert h is None


def test_diff_row_hash_changed_counts_as_update(con):
    _seed_silver(con, [{"country": "AFG", "year": 2020, "row_hash": "old_hash"}])
    df_new = pd.DataFrame(
        {"country": ["AFG"], "year": [2020], "row_hash": ["new_hash"]}
    )
    ins, upd, dlt, h = diff_counts_and_hash(con, df_new, "test_entity", "2020", _SPEC)
    assert ins == 0
    assert upd == 1
    assert dlt == 0


def test_diff_identical_rows_no_change(con):
    _seed_silver(con, [{"country": "AFG", "year": 2020, "row_hash": "same"}])
    df_new = pd.DataFrame({"country": ["AFG"], "year": [2020], "row_hash": ["same"]})
    ins, upd, dlt, h = diff_counts_and_hash(con, df_new, "test_entity", "2020", _SPEC)
    assert ins == 0
    assert upd == 0
    assert dlt == 0


def test_diff_mixed_insert_update_delete(con):
    _seed_silver(
        con,
        [
            {"country": "AFG", "year": 2020, "row_hash": "old"},  # will update
            {"country": "IRQ", "year": 2020, "row_hash": "gone"},  # will delete
        ],
    )
    df_new = pd.DataFrame(
        {
            "country": ["AFG", "SYR"],  # AFG updated, SYR new
            "year": [2020, 2020],
            "row_hash": ["new", "fresh"],
        }
    )
    ins, upd, dlt, h = diff_counts_and_hash(con, df_new, "test_entity", "2020", _SPEC)
    assert ins == 1
    assert upd == 1
    assert dlt == 1


def test_diff_partition_hash_stable(con):
    _seed_silver(con, [])
    df = pd.DataFrame({"country": ["AFG"], "year": [2020], "row_hash": ["abc"]})
    _, _, _, h1 = diff_counts_and_hash(con, df, "test_entity", "2020", _SPEC)
    _, _, _, h2 = diff_counts_and_hash(con, df, "test_entity", "2020", _SPEC)
    assert h1 == h2
