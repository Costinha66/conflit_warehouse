import os
import hashlib
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import duckdb
import re
import structlog
from src.others.ddls import create_diff

# ---- configure logging ----
structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(min_level=20),  # INFO+
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.dev.ConsoleRenderer(),
    ],
)
log = structlog.get_logger()


def compute_file_hash(path: Path, chunk_size=65536) -> str:
    log.debug("computing_hash.start", file=str(path))
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(chunk_size):
            h.update(chunk)
    digest = h.hexdigest()
    log.debug("computing_hash.done", file=str(path), hash=digest)
    return h.hexdigest()


def _extract_source_id(file_path: Path) -> str | None:
    parts = file_path.parts
    if "bronze" in parts:
        i = parts.index("bronze")
        source = parts[i + 1] if i + 1 < len(parts) else None
        log.debug("source_id.extracted", file=str(file_path), source_id=source)
        return source
    return None


YEAR_RE = re.compile(r"^(?P<start>\d{4})$")
YEAR_RANGE_RE = re.compile(r"^(?P<start>\d{4})-(?P<end>\d{4})$")
MONTH_RE = re.compile(r"^(?P<start>\d{4}-\d{2})$")
MONTH_RANGE_RE = re.compile(r"^(?P<start>\d{4}-\d{2})-(?P<end>\d{4}-\d{2})$")


def parse_partition_from_filename(file_path: Path):
    """
    Parse coverage info (start, end, grain) from a parquet filename.
    Returns (coverage_start, coverage_end, grain) or raises ValueError.
    """
    # Get the base filename without extension, e.g. "2020-2020-part-000"
    stem = file_path.stem

    # Take only the coverage part (before "-part-")
    coverage_token = stem.split("-part-")[0]

    if m := YEAR_RANGE_RE.match(coverage_token):
        return m.group("start"), m.group("end"), "year"
    elif m := YEAR_RE.match(coverage_token):
        return m.group("start"), m.group("start"), "year"
    elif m := MONTH_RANGE_RE.match(coverage_token):
        return m.group("start"), m.group("end"), "month"
    elif m := MONTH_RE.match(coverage_token):
        return m.group("start"), m.group("start"), "month"
    else:
        raise ValueError(f"Unrecognized partition format in filename: {file_path.name}")


def discovery(path_warehouse: Path, chunk_size=65536):
    log.info("discovery.start", path=str(path_warehouse))
    records = []
    for root, _, files in os.walk(path_warehouse):
        for file in files:
            if file.endswith(".parquet"):
                file_path = Path(root) / file
                try:
                    coverage_start, coverage_end, grain = parse_partition_from_filename(
                        file_path
                    )
                except ValueError as e:
                    coverage_start, coverage_end, grain = None, None, None
                    status = "failed"
                    notes = str(e)
                else:
                    status = "pending"
                    notes = None
                records.append(
                    {
                        "source_id": _extract_source_id(file_path) or "unknown",
                        "file_path": str(file_path),
                        "content_hash": compute_file_hash(file_path, chunk_size),
                        "file_size": file_path.stat().st_size,
                        "discovered_at": datetime.now(timezone.utc),
                        "coverage_start": coverage_start,
                        "coverage_end": coverage_end,
                        "grain": grain,
                        "processed_at": None,
                        "status": status,
                        "run_id": None,
                        "notes": notes,
                    }
                )
    log.info("discovery.done", count=len(records))
    return records


def populate_manifest(con, df):
    log.info("manifest.populate.start", rows=len(df))
    con.execute("BEGIN")
    con.register("staging_manifest", df)

    con.execute("""
        CREATE TEMP TABLE _new_versions AS
        SELECT s.*
        FROM staging_manifest s
        LEFT JOIN ingest_manifest m
        ON m.file_path = s.file_path
        AND m.content_hash = s.content_hash
        WHERE m.file_path IS NULL
        AND s.status = 'pending';
    """)
    con.execute("""
    INSERT INTO ingest_manifest (
        source_id, file_path, content_hash, file_size,
        discovered_at, processed_at, status, run_id, notes,
        coverage_start, coverage_end, grain
        )
        SELECT
        s.source_id, s.file_path, s.content_hash, s.file_size,
        s.discovered_at, s.processed_at, s.status, s.run_id, s.notes,
        s.coverage_start, s.coverage_end, s.grain
        FROM _new_versions s;
    """)
    con.execute("""
    UPDATE ingest_manifest AS old
       SET status = 'superseded'
     FROM (SELECT file_path, MAX(discovered_at) AS max_disc
             FROM ingest_manifest GROUP BY file_path) latest
    WHERE old.file_path = latest.file_path
      AND old.discovered_at < latest.max_disc
      AND old.status IN ('pending','processed');
    """)
    con.execute("COMMIT;")
    con.unregister("staging_manifest")
    log.info("manifest.populate.done")


def dirty_partitions(con):
    log.info("dirty_partitions.start")
    con.execute("BEGIN")
    con.execute("""CREATE TEMP TABLE _pending_latest AS
        SELECT m.*
        FROM ingest_manifest m
        JOIN (
        SELECT file_path, MAX(discovered_at) AS max_disc
        FROM ingest_manifest
        WHERE status = 'pending'
        GROUP BY file_path
        ) x
        ON m.file_path = x.file_path AND m.discovered_at = x.max_disc
        WHERE m.coverage_start IS NOT NULL
        AND m.coverage_end   IS NOT NULL
        AND m.grain IN ('year','month');
    """)
    con.execute("""
    CREATE TEMP TABLE _routed AS
        SELECT
        p.file_path,
        p.content_hash,
        p.coverage_start,
        p.coverage_end,
        p.grain,
        r.entity,
        r.grain,
        r.route_id
        FROM _pending_latest p
        JOIN entity_router r
        ON r.enabled
        AND r.source_id = p.source_id
        AND regexp_matches(p.file_path, r.path_regex)""")
    con.execute("""
        CREATE TEMP TABLE _expanded AS
            WITH monthly AS (
            SELECT
                r.file_path,
                r.content_hash,
                r.entity,
                r.route_id,
                -- alias the single column from range() as month_dt
                strftime(m.month_dt, '%Y-%m') AS partition_key
            FROM _routed r
            , LATERAL (
                SELECT *
                FROM range(
                    strptime(r.coverage_start || '-01', '%Y-%m'),
                    strptime(r.coverage_end   || '-01', '%Y-%m') + INTERVAL 1 MONTH,
                    INTERVAL 1 MONTH
                )
            ) AS m(month_dt)
            WHERE r.grain = 'month'
            ),
            yearly AS (
            SELECT
                r.file_path,
                r.content_hash,
                r.entity,
                r.route_id,
                CAST(y.y AS TEXT) AS partition_key    -- 'YYYY'
            FROM _routed r
            , LATERAL (
                -- range(start, end) is end-exclusive, so add +1
                SELECT *
                FROM range(CAST(r.coverage_start AS INTEGER), CAST(r.coverage_end AS INTEGER) + 1)
            ) AS y(y)
            WHERE r.grain = 'year'
            )
            SELECT * FROM monthly
            UNION ALL
            SELECT * FROM yearly; """)
    con.execute("""
    INSERT INTO manifest_partition_link (file_path, content_hash, entity, partition_key, route_id)
        SELECT e.file_path, e.content_hash, e.entity, e.partition_key, e.route_id
        FROM _expanded e
        LEFT JOIN manifest_partition_link l
        ON l.file_path=e.file_path AND l.content_hash=e.content_hash
        AND l.entity=e.entity AND l.partition_key=e.partition_key
        WHERE l.file_path IS NULL;
    """)
    con.execute("""
    INSERT INTO dirty_partitions (entity, partition_key, reason, first_seen_at, last_seen_at, status)
        SELECT e.entity, e.partition_key, 'pending_manifest', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'dirty'
        FROM _expanded e
        LEFT JOIN dirty_partitions d
        ON d.entity=e.entity AND d.partition_key=e.partition_key AND d.status='dirty'
        WHERE d.entity IS NULL;
    """)
    con.execute("COMMIT;")
    log.info("dirty_partitions.end")


def main(
    bronze_path: str = "/home/faacosta0245695/conflit/conflit_warehouse/data/bronze",
    warehouse_path="warehouse/database.db",
):
    run_id = datetime.now(timezone.utc).isoformat()
    log.info("pipeline.run.start", run_id=run_id)
    records = discovery(bronze_path)

    df = pd.DataFrame.from_records(records)
    con = duckdb.connect(warehouse_path)
    create_diff(con)
    populate_manifest(con, df)
    dirty_partitions(con)
    con.close()
    log.info("pipeline.run.done", run_id=run_id)


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser()
    a = p.parse_args()
    main()
