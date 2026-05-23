from __future__ import annotations

from pathlib import Path

import structlog
from prefect import flow, task

from src.bronze.snapshot_maker import SnapshotService
from src.core.dq.bronze_policy import DQBuilderBronze
from src.core.logging import configure_logging
from src.core.types import DiscoveryConfig, SnapshotConfig
from src.diff.discovery import DiscoveryService
from src.gold.processor_gold import main as gold_main
from src.silver.processor import main as silver_main

PROJECT_ROOT = Path(__file__).parent.parent.parent


@task(name="ingest-bronze", retries=3, retry_delay_seconds=30)
def ingest_bronze(
    csv_path: str,
    out_root: str = "warehouse/bronze",
    db_path: str = "warehouse/database.db",
    start_year: int = 2020,
    cutoff_year: int = 2020,
) -> None:
    log = structlog.get_logger().bind(task="ingest_bronze")
    log.info("bronze.start", csv_path=csv_path)
    cfg = SnapshotConfig(
        db_path=PROJECT_ROOT / db_path,
        csv_path=PROJECT_ROOT / csv_path,
        out_root=PROJECT_ROOT / out_root,
        start_year=start_year,
        cutoff_year=cutoff_year,
    )
    SnapshotService(cfg, DQBuilderBronze).run()
    log.info("bronze.done")


@task(name="run-diff")
def run_diff(
    bronze_root: str = "warehouse/bronze",
    router_path: str = "src/diff/router.yaml",
    db_path: str = "warehouse/database.db",
) -> dict:
    log = structlog.get_logger().bind(task="run_diff")
    log.info("diff.start")
    cfg = DiscoveryConfig(
        bronze_root=PROJECT_ROOT / bronze_root,
        router_path=PROJECT_ROOT / router_path,
        db_path=PROJECT_ROOT / db_path,
    )
    result = DiscoveryService(cfg).run()
    log.info("diff.done", **result)
    return result


@task(name="process-silver")
def process_silver(
    warehouse_path: str = "warehouse/database.db",
    spec_path: str = "schemas/silver/refugees_stack.yaml",
) -> list:
    log = structlog.get_logger().bind(task="process_silver")
    log.info("silver.start")
    results = silver_main(
        warehouse_path=str(PROJECT_ROOT / warehouse_path),
        spec_path=str(PROJECT_ROOT / spec_path),
    )
    log.info("silver.done", partitions=len(results) if results else 0)
    return results or []


@task(name="process-gold")
def process_gold(
    warehouse_path: str = "warehouse/database.db",
    spec_path: str = "schemas/gold/refugees_stack_yearly.yaml",
) -> None:
    log = structlog.get_logger().bind(task="process_gold")
    log.info("gold.start")
    gold_main(
        warehouse_path=str(PROJECT_ROOT / warehouse_path),
        spec_path=str(PROJECT_ROOT / spec_path),
    )
    log.info("gold.done")


@flow(name="conflit-warehouse-pipeline", log_prints=True)
def pipeline_flow(
    csv_path: str = "data/raw/unhcr_population.csv",
    bronze_root: str = "warehouse/bronze",
    db_path: str = "warehouse/database.db",
    router_path: str = "src/diff/router.yaml",
    silver_spec: str = "schemas/silver/refugees_stack.yaml",
    gold_spec: str = "schemas/gold/refugees_stack_yearly.yaml",
    start_year: int = 2020,
    cutoff_year: int = 2020,
) -> None:
    configure_logging(json_logs=True)
    log = structlog.get_logger().bind(flow="pipeline")
    log.info("pipeline.start", start_year=start_year, cutoff_year=cutoff_year)

    ingest_bronze(csv_path, bronze_root, db_path, start_year, cutoff_year)
    run_diff(bronze_root, router_path, db_path)
    process_silver(db_path, silver_spec)
    process_gold(db_path, gold_spec)

    log.info("pipeline.done")


if __name__ == "__main__":
    pipeline_flow()
