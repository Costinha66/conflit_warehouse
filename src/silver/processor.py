import hashlib
from pathlib import Path
from typing import List, Dict, Any
import duckdb
import pandas as pd
from src.silver.harmonizer import apply_harmonize
from src.silver.canonilaze import canonicalize
from src.core.json import load_schema
from src.diff.planner import planner
from src.others.ddls import create_silver
from src.core.dataclasses import RunRef, CommitEvent
from src.core.miscelannious import now_utc
from src.core.dataclasses import DatasetRef, DQSummary
from src.core.dq import run_dq_tests
from src.core.write_partition import write_partition, silver_finalize_hook
from src.core.log import configure_logging


def diff_counts_and_hash(
    con,
    df_new: pd.DataFrame,
    entity: str,  # e.g. "refugee_displacement_conformed"
    partition_key: str,  # "2020" or "2020-07"
    spec: dict,  # unified YAML (has contract/grain)
    table_fqn: str | None = None,  # override table name if needed
):
    """
    Computes I/U/D counts for the new partition vs current snapshot, and a partition hash.
    Works with per-entity tables (default) and can be pointed to another table via table_fqn.
    """
    # resolve table and grain
    table_fqn = table_fqn or f"silver.{entity}"
    grain = spec.get("grain", "year")
    pk = spec["contract"][
        "primary_key"
    ]  # e.g. ['country_origin','country_destination','year']

    # build WHERE clause for partition
    if grain == "year":
        where = "year = ?"
        where_args = [int(partition_key)]
    elif grain == "month":
        # adapt if your monthly column is named differently (e.g., month_start as DATE)
        where = "month_start = ?"
        where_args = [partition_key]
    else:
        raise ValueError(f"Unsupported grain: {grain}")

    # load old snapshot safely (table might not exist on first run)
    try:
        old = con.execute(
            f"SELECT {', '.join(pk)}, row_hash FROM {table_fqn} WHERE {where};",
            where_args,
        ).df()
    except duckdb.CatalogException:
        old = pd.DataFrame(columns=[*pk, "row_hash"])

    # make sure df_new has exactly the PK + row_hash we need
    cols_needed = [*pk, "row_hash"]
    missing = [c for c in cols_needed if c not in df_new.columns]
    if missing:
        raise RuntimeError(
            f"diff_counts_and_hash: new dataframe missing columns {missing}"
        )

    new_small = df_new[cols_needed].copy()

    # compute I/U/D
    if old.empty and new_small.empty:
        inserts = updates = deletes = 0
    elif old.empty:
        inserts, updates, deletes = len(new_small), 0, 0
    elif new_small.empty:
        inserts, updates, deletes = 0, 0, len(old)
    else:
        merged = new_small.merge(
            old, on=pk, how="outer", suffixes=("_new", "_old"), indicator=True
        )
        inserts = int((merged["_merge"] == "left_only").sum())
        deletes = int((merged["_merge"] == "right_only").sum())
        updates = int(
            (
                (merged["_merge"] == "both")
                & (merged["row_hash_new"] != merged["row_hash_old"])
            ).sum()
        )

    # stable partition hash over new rows (None if empty)
    if new_small.empty:
        partition_hash = None
    else:
        joined = "|".join(sorted(new_small["row_hash"].tolist()))
        partition_hash = hashlib.sha256(joined.encode("utf-8")).hexdigest()

    return inserts, updates, deletes, partition_hash


class SilverProcessor:
    def __init__(self, log, con: duckdb.DuckDBPyConnection, run: RunRef):
        self.con = con
        self.run = run
        self.log = log

    def process_plan_item(
        self,
        plan_item: Dict[
            str, Any
        ],  # {entity, partition_key, inputs:[{file_path, content_hash, ...}], ...}
        spec: Dict[str, Any],  # loaded YAML for this Silver entity
        emit_stdout: bool = True,
    ) -> Dict[str, Any]:
        """
        Orchestrates a single Silver partition build (idempotent).
        Steps: read -> harmonize -> canonicalize -> DQ -> write -> lineage.
        """
        entity = plan_item["entity"]
        part = str(plan_item["partition_key"])
        inputs = plan_item.get("inputs", [])
        t0 = now_utc()
        slog = self.log.bind(entity=entity, partition=part, run_id=self.run.run_id)

        slog.info("silver.start")

        # 1) Read Bronze inputs
        df_raw = self._read_inputs(inputs)

        # 2) Harmonize
        df_h, rej_h, dq_h = apply_harmonize(df_raw, spec)

        # 3) Canonicalize
        df_c, rej_c, dq_c = canonicalize(df_h, spec, slog)
        print(df_c.columns)

        # 4) Temp table + DQ (generic runner)
        self.con.register("silver_tmp_df", df_c)
        self.con.execute(
            "CREATE TEMP TABLE writing_temp AS SELECT * FROM silver_tmp_df;"
        )
        dq_report = run_dq_tests(
            self.con,
            tmp_table="writing_temp",
            spec=spec,
            partition_key=part,
            layer="silver",
        )
        for r in dq_report.results:
            if r.status != "passed":
                slog.error(
                    "dq_failure_detail", extra={"name": r.name, "details": r.details}
                )
                raise RuntimeError(f"Silver DQ failed for {entity} {part}")

        # 5) Stamp lineage columns on rows (uniform across tiers)
        df_c["_ingest_run_id"] = self.run.run_id
        df_c["_transform_version"] = self.run.transform_version
        df_c["_build_at"] = now_utc()

        # 6) Diff/hash (optional if your writer doesn’t need it)
        partition_hash = diff_counts_and_hash(self.con, df_c, entity, part, spec)
        print(inputs)
        # 7) Build CommitEvent once
        ev = CommitEvent(
            run=RunRef(
                run_id=self.run.run_id,
                transform_version=self.run.transform_version,
                started_at=self.run.started_at,
            ),
            part=DatasetRef(
                layer="silver",
                entity=entity,
                grain=spec.get("grain", "year"),
                partition_key=part,
            ),
            commit_entity="silver_commits",
            row_count=len(df_c),
            insert_cnt=0,
            update_cnt=0,
            delete_cnt=0,  # optional: fill if you compute I/U/D
            partition_hash=partition_hash,
            inputs=inputs,
            dq_summary=DQSummary(
                raw_rows=len(df_raw),
                harmonized_rows=len(df_h),
                valid_rows=len(df_c),
                rejects_harmonize=len(rej_h),
                rejects_canonicalize=len(rej_c),
            ),
            spec_version=str(spec.get("version", "1")),
            finished_at=now_utc(),
        )

        # 8) Write (transactional swap + commit record inside)
        write_partition(
            con=self.con,
            df_input=df_c,
            spec=spec,
            log=slog,
            ev=ev,
            post_commit_hooks=[silver_finalize_hook],
        )

        # 9) (Optional) Emit an additional lineage event for “transform_run”

        slog.info(
            "silver.done",
            rows_out=len(df_c),
            ms_total=int((now_utc() - t0).total_seconds() * 1000),
        )
        return {"entity": entity, "partition": part, "rows_out": len(df_c)}

    # ---- internals ----------------------------------------------------------

    def _read_inputs(self, inputs: List[Dict[str, Any]]) -> pd.DataFrame:
        frames = []
        for inp in inputs:
            df = pd.read_parquet(inp["file_path"])
            df["_bronze_file"] = inp["file_path"]
            df["_content_hash"] = inp.get("content_hash")
            df["_ingest_run_id"] = self.run.run_id
            frames.append(df)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# -------------------------
# Entrypoint
# -------------------------
def main(
    bronze_path: str = "/home/faacosta0245695/conflit/conflit_warehouse/data/bronze",
    warehouse_path: str = "warehouse/database.db",
    spec_path: str = "/home/faacosta0245695/conflit/conflit_warehouse/schemas/silver/refugees_stack.yaml",
    log_level: str = "INFO",
):
    base_logger = configure_logging(log_level)

    con = duckdb.connect(warehouse_path)
    spec = load_schema(Path(spec_path))
    run_id = now_utc().strftime("%Y%m%dT%H%M%S")
    proc = SilverProcessor(
        base_logger,
        con,
        RunRef(
            run_id=run_id,
            transform_version=int(spec.get("transform_version", 1)),
            started_at=now_utc(),
        ),
    )

    create_silver(con)

    results = []
    for idx, plan_item in enumerate(planner(con)):
        results.append(proc.process_plan_item(plan_item, spec))
    con.close()
    return results


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--db", default="warehouse/database.db")
    p.add_argument(
        "--spec",
        default="/home/faacosta0245695/conflit/conflit_warehouse/schemas/silver/refugees_stack.yaml",
    )
    p.add_argument("--log-level", default="INFO")
    args = p.parse_args()
    main(warehouse_path=args.db, spec_path=args.spec, log_level=args.log_level)
