import structlog
import duckdb
from pathlib import Path
from datetime import datetime
from src.core.log import configure_logging
from src.core.metrics import collect_file_metrics, StageTimer
from src.bronze.dq import DQBuilderBronze
from datetime import timezone
from src.core.json import write_json


def main(
    snapshot_version: str | None = None, cutoff_year: int = 2020, start_year: int = 2020
):
    # Logging
    configure_logging(json_logs=True)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    log = structlog.get_logger().bind(
        stage="bronze_snapshot",
        source="unhcr",
        run_id=run_id,
        snapshot_version=snapshot_version,
        start_year=start_year,
        cutoff_year=cutoff_year,
        mode="cumulative",
    )
    snapshot_version = snapshot_version or datetime.now().date().isoformat()
    log.info(
        "snapshot_start",
        source="unhcr",
        snapshot_version=snapshot_version,
        mode="cumulative",
    )

    # Init
    con = None
    m = {"records": None, "bytes": None, "hash": None}
    dq = type("DQ", (), {"passed": False, "level": "CRITICAL", "metrics": {}})()
    timer = type("T", (), {"duration_sec": None})()

    out_path = Path(f"data/bronze/unhcr/date={snapshot_version}")
    out_path_parquet = out_path / f"{start_year}-{cutoff_year}-part-000.parquet"
    out_path_parquet.parent.mkdir(parents=True, exist_ok=True)

    csv_path = "/home/faacosta0245695/conflit/conflit_warehouse/data/raw/unhcr/persons_of_concern.csv"
    # DuckDB

    try:
        with StageTimer() as timer:
            cutoff_year = int(cutoff_year)
            start_year = int(start_year)
            assert start_year <= cutoff_year
            # Inputs
            con = duckdb.connect("warehouse/database.db")

            # 1) Parameterized SELECT into a temp view
            con.execute(f"""
            CREATE OR REPLACE TEMP VIEW snapshot_view AS
            SELECT
                TRY_CAST(Year AS INT) AS year,
                CAST("Country of Asylum ISO" AS TEXT)  AS country_of_asylum,
                CAST("Country of Origin ISO"  AS TEXT) AS country_of_origin,
                CAST("Refugees"              AS BIGINT) AS refugees,
                CAST("Returned Refugees"     AS BIGINT) AS returned_refugees,
                CAST("Asylum Seekers"        AS BIGINT) AS asylum_seekers,
                CAST("IDPs"                  AS BIGINT) AS idps,
                CAST("Returned IDPs"         AS BIGINT) AS returned_idps,
                CAST("Stateless"             AS BIGINT) AS stateless,
                CAST("HST"                   AS BIGINT) AS hst,
                CAST("OOC"                   AS BIGINT) AS ooc
            FROM read_csv_auto('{csv_path.replace("'", "''")}', HEADER=TRUE)
            WHERE TRY_CAST(Year AS INT) BETWEEN {start_year} AND {cutoff_year}
            """)

            # 2) COPY the view to a literal path
            con.execute(
                f"COPY snapshot_view TO '{str(out_path_parquet).replace("'", "''")}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
        # Output
        m = collect_file_metrics(out_path_parquet)

        dq = DQBuilderBronze(m, cutoff_year, start_year)

        log.info(
            "snapshot_done",
            duration_sec=timer.duration_sec,
            dq_passed=dq.passed,
            dq_level=dq.level,
            dq_metrics=dq.metrics,
        )

        write_json(
            out_path / "_dq_summary.json",
            {
                "source": "unhcr",
                "snapshot_version": snapshot_version,
                "cutoff_year": cutoff_year,
                "start_year": start_year,
                "records": m["records"],
                "bytes": m["bytes"],
                "hash": m["hash"],
                "duration_sec": timer.duration_sec,
                "dq_passed": dq.passed,
                "dq_level": dq.level,
                "dq_metrics": dq.metrics,
            },
        )
    except Exception:
        log.error(
            "snapshot_failed",
            duration_sec=timer.duration_sec,
            dq_passed=dq.passed,
            dq_level=dq.level,
            dq_metrics=dq.metrics,
        )

        write_json(
            out_path / "_dq_summary.json",
            {
                "source": "unhcr",
                "snapshot_version": snapshot_version,
                "cutoff_year": cutoff_year,
                "start_year": start_year,
                "records": m["records"],
                "bytes": m["bytes"],
                "hash": m["hash"],
                "duration_sec": timer.duration_sec,
                "dq_passed": dq.passed,
                "dq_level": dq.level,
                "dq_metrics": dq.metrics,
            },
        )
        con.close()
        raise
    finally:
        con.close()


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--snapshot_version")
    p.add_argument("--cutoff_year", type=int, default=2020)
    p.add_argument("--start_year", type=int, default=2020)
    a = p.parse_args()
    main(a.snapshot_version, a.cutoff_year, a.start_year)
