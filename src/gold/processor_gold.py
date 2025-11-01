# src/gold/processor.py
from __future__ import annotations
from typing import Dict, Any
from datetime import datetime, timezone
import duckdb
import pandas as pd
from pathlib import Path
import logging

from src.core.json import load_schema
from src.core.dq.dq import run_dq_tests
from src.core.logging import configure_logging
from src.infra.duckdb.io import write_partition
from src.infra.yaml_sql.sql_yaml_reader import generate_transform_sql
from src.core.types import RunRef, CommitEvent, DatasetRef, DQSummary
from src.core.time import now_utc
from src.others.ddls import create_dims, create_gold, create_lineage


# ------------------------------
# Gold Processor
# ------------------------------
class GoldProcessor:
    def __init__(
        self,
        log: logging.Logger,
        con: duckdb.DuckDBPyConnection,
        spec: Dict[Any],
        run: RunRef,
    ):
        self.con = con
        self.run = run
        self.spec = spec
        self.log = log

    def build_year(
        self,
        gold_entity: str,  # e.g., 'refugee_stack_yearly'
        silver_source: str,  # e.g., 'silver.refugee_displacement_conformed'
        year: int,
    ) -> Dict[str, Any]:
        """
        Single-partition (year) build for a Gold entity.
        Steps: load contract -> select inputs -> transform -> DQ -> write -> lineage.
        """
        t0 = datetime.now(timezone.utc)
        self.log.info("gold.build.start", entity=gold_entity, year=year)

        assert self.spec["grain"] == "year", "This skeleton assumes yearly grain."

        # 2) Read slice from Silver (deterministic)
        df_in = self._select_inputs_from_silver(silver_source, year)

        # 3) Transform / derive Gold columns (projection + derivations)
        sql, params = generate_transform_sql(self.con, self.spec, year)
        df_gold = self.con.execute(sql, params).df()
        df_gold["_ingest_run_id"] = self.run.run_id
        df_gold["_transform_version"] = self.run.transform_version
        df_gold["_built_at"] = now_utc()

        # 4) DQ gate (fail-fast). Reuse your YAML-driven tests where possible.
        self.con.register("gold_tmp_df", df_gold)
        self.con.execute("CREATE TEMP TABLE writing_temp AS SELECT * FROM gold_tmp_df;")
        print(df_gold.head())
        dq_report = run_dq_tests(
            con=self.con,
            tmp_table="writing_temp",
            spec=self.spec,
            partition_key=str(year),
            layer="gold",
        )
        for r in dq_report.results:
            if r.status != "passed":
                self.log.error(
                    "dq_failure_detail", extra={"name": r.name, "details": r.details}
                )
                raise RuntimeError("Silver DQ")

        # Prepare lineage inputs (point to the Silver partition(s) you read)
        inputs = [
            {
                "dataset": {
                    "layer": "silver",
                    "entity": silver_source.split(".")[1],
                    "partition_key": str(year),
                },
                # add more fields if you want (e.g., manifest hashes) — not mandatory here
            }
        ]

        ev = CommitEvent(
            run=RunRef(
                run_id=self.run.run_id,
                transform_version=self.run.transform_version,
                started_at=self.run.started_at,
            ),
            part=DatasetRef(
                layer="gold",
                entity=gold_entity,
                grain=self.spec.get("grain", "year"),
                partition_key=str(year),
            ),
            commit_entity="gold_commits",
            row_count=len(df_gold),
            insert_cnt=0,
            update_cnt=0,
            delete_cnt=0,  # optional: fill if you compute I/U/D
            inputs=inputs,
            partition_hash=None,
            dq_summary=DQSummary(
                raw_rows=len(df_in),
                harmonized_rows=0,
                valid_rows=0,
                rejects_harmonize=0,
                rejects_canonicalize=0,
            ),
            spec_version=str(self.spec.get("version", "1")),
            finished_at=now_utc(),
        )

        # Write (your write_partition already does validate_temp + insert_commit)
        write_partition(
            con=self.con,
            df_input=df_gold,
            spec=self.spec,
            log=self.log.bind(entity=gold_entity, year=year),
            ev=ev,
        )

        self.log.info(
            "gold.build.done",
            entity=gold_entity,
            year=year,
            rows_out=len(df_gold),
            ms_total=self._elapsed_ms(t0),
        )
        return {"entity": gold_entity, "year": year, "rows_out": len(df_gold)}

    # ------------------------------
    # Internals (keep small & pure)
    # ------------------------------
    def _select_inputs_from_silver(self, silver_source: str, year: int) -> pd.DataFrame:
        """
        Select exactly one year slice from Silver.
        Assumes Silver is yearly; if it were monthly, you’d pick terminal month here.
        """
        sql = f"""
            SELECT *
            FROM {silver_source}
            WHERE year = ?
        """
        return self.con.execute(sql, [year]).df()

    @staticmethod
    def _elapsed_ms(t0: datetime) -> int:
        return int((datetime.now(timezone.utc) - t0).total_seconds() * 1000)


def main(
    warehouse_path: str = "warehouse/database.db",
    spec_path: str = "/home/faacosta0245695/conflit/conflit_warehouse/schemas/silver/refugees_stack.yaml",
    log_level: str = "INFO",
):
    configure_logging(log_level)
    base_logger = logging.getLogger("processor")

    con = duckdb.connect(warehouse_path)
    spec = load_schema(Path(spec_path))
    run_id = now_utc().strftime("%Y%m%dT%H%M%S")
    create_dims(con)
    create_lineage(con)
    create_gold(con)
    GoldProcessor(
        base_logger,
        con,
        spec,
        RunRef(
            run_id=run_id,
            transform_version=int(spec.get("transform_version", 1)),
            started_at=now_utc(),
        ),
    ).build_year("refugee_stack_yearly", "silver.refugee_displacement_conformed", 2020)


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--db", default="warehouse/database.db")
    p.add_argument(
        "--spec",
        default="/home/faacosta0245695/conflit/conflit_warehouse/schemas/gold/refugees_stack_yearly.yaml",
    )
    p.add_argument("--log-level", default="INFO")
    args = p.parse_args()
    main(warehouse_path=args.db, spec_path=args.spec, log_level=args.log_level)
