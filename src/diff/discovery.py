# src/diff/discovery_service.py
from __future__ import annotations
from pathlib import Path
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, List
import duckdb
import pandas as pd
import structlog

from src.core.types import DiscoveryConfig
from src.core.miscelannious import _sha256, _iter_parquet, _extract_source_id
from src.diff.parser import _parse_cov
from src.diff.router import load_router_yaml, seed_entity_router
from src.others.ddls import create_diff


class DiscoveryService:
    def __init__(
        self, cfg: DiscoveryConfig, con: duckdb.DuckDBPyConnection | None = None
    ):
        self.cfg = cfg
        self.log = structlog.get_logger().bind(mod="discovery", db=str(cfg.db_path))
        self._external_con = con  # allow DI for tests

    # --------- public entrypoint ---------
    def run(self) -> dict:
        run_id = datetime.now(timezone.utc).isoformat()
        self.log.info("run.begin", run_id=run_id, root=str(self.cfg.bronze_root))

        with self._connect() as con:
            self._ensure_ddls(con, self.cfg.router_path)
            df = self._discover_dataframe()
            self._populate_manifest(con, df)
            self._route_and_mark_dirty(con)
        self.log.info("run.done", run_id=run_id, rows=df.shape[0])
        return {"run_id": run_id, "rows": int(df.shape[0])}

    def _discover_dataframe(self) -> pd.DataFrame:
        root = self.cfg.bronze_root
        files = list(_iter_parquet(root))
        self.log.info("scan.start", files=len(files), root=str(root))
        recs: List[Dict[str, Any]] = []

        def worker(fp: Path) -> Dict[str, Any]:
            st = fp.stat()
            s_id = _extract_source_id(fp) or "unknown"
            start, end, grain, err = _parse_cov(fp)
            status = "pending" if err is None else "failed"
            notes = None if err is None else err
            ch = _sha256(fp)
            return {
                "source_id": s_id,
                "file_path": str(fp),
                "file_size": st.st_size,
                "content_hash": ch,
                "discovered_at": datetime.now(timezone.utc),
                "coverage_start": start,
                "coverage_end": end,
                "grain": grain,
                "processed_at": None,
                "status": status,
                "run_id": None,
                "notes": notes,
            }

        with ThreadPoolExecutor(max_workers=self.cfg.max_workers) as ex:
            futs = [ex.submit(worker, f) for f in files]
            for fut in as_completed(futs):
                try:
                    recs.append(fut.result())
                except Exception:
                    self.log.exception("hash_failed")

        df = pd.DataFrame.from_records(recs)
        self.log.info("scan.done", rows=int(df.shape[0]))
        return df

    def _populate_manifest(
        self, con: duckdb.DuckDBPyConnection, df: pd.DataFrame
    ) -> None:
        self.log.info("manifest.populate.begin", rows=int(df.shape[0]))
        con.execute("BEGIN;")
        try:
            con.register("staging_manifest", df)
            con.execute("""
                CREATE TEMP TABLE _new_versions AS
                SELECT s.*
                FROM staging_manifest s
                LEFT JOIN ingest_manifest m
                  ON m.file_path=s.file_path AND m.content_hash=s.content_hash
                WHERE m.file_path IS NULL AND s.status='pending';
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
                   SET status='superseded'
                 FROM (SELECT file_path, MAX(discovered_at) max_disc
                         FROM ingest_manifest GROUP BY file_path) latest
                WHERE old.file_path=latest.file_path
                  AND old.discovered_at < latest.max_disc
                  AND old.status IN ('pending','processed');
            """)
            con.execute("COMMIT;")
        except Exception:
            con.execute("ROLLBACK;")
            self.log.exception("manifest.populate.failed")
            raise
        finally:
            try:
                con.unregister("staging_manifest")
            except Exception:
                pass
        self.log.info("manifest.populate.done")

    def _route_and_mark_dirty(self, con: duckdb.DuckDBPyConnection) -> None:
        self.log.info("routing.begin")
        con.execute("BEGIN;")
        try:
            con.execute("""
                CREATE TEMP TABLE _pending_latest AS
                SELECT m.*
                FROM ingest_manifest m
                JOIN (
                  SELECT file_path, MAX(discovered_at) AS max_disc
                  FROM ingest_manifest WHERE status='pending'
                  GROUP BY file_path
                ) x
                ON m.file_path=x.file_path AND m.discovered_at=x.max_disc
                WHERE m.coverage_start IS NOT NULL AND m.coverage_end IS NOT NULL
                  AND m.grain IN ('year','month');
            """)
            con.execute("""
                CREATE TEMP TABLE _routed AS
                SELECT
                  p.file_path, p.content_hash,
                  p.coverage_start, p.coverage_end,
                  p.grain AS p_grain,
                  r.entity, r.grain AS r_grain, r.route_id
                FROM _pending_latest p
                JOIN entity_router r
                  ON r.enabled
                 AND r.source_id = p.source_id
            """)
            con.execute("""
                CREATE TEMP TABLE _expanded AS
                WITH monthly AS (
                  SELECT r.file_path, r.content_hash, r.entity, r.route_id,
                         strftime(m.month_dt,'%Y-%m') AS partition_key
                  FROM _routed r,
                  LATERAL (
                    SELECT * FROM range(
                      strptime(r.coverage_start||'-01','%Y-%m'),
                      strptime(r.coverage_end||'-01','%Y-%m') + INTERVAL 1 MONTH,
                      INTERVAL 1 MONTH
                    )
                  ) AS m(month_dt)
                  WHERE r.r_grain='month'
                ),
                yearly AS (
                  SELECT r.file_path, r.content_hash, r.entity, r.route_id,
                         CAST(y.y AS TEXT) AS partition_key
                  FROM _routed r,
                  LATERAL (SELECT * FROM range(CAST(r.coverage_start AS INT),
                                               CAST(r.coverage_end AS INT) + 1)) AS y(y)
                  WHERE r.r_grain='year'
                )
                SELECT * FROM monthly
                UNION ALL
                SELECT * FROM yearly;
            """)
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
        except Exception:
            con.execute("ROLLBACK;")
            self.log.exception("routing.failed")
            raise
        self.log.info("routing.done")

    def _connect(self):
        if self._external_con:
            # simple wrapper to match context manager protocol
            class _Ctx:
                def __init__(self, con):
                    self.con = con

                def __enter__(self):
                    return self.con

                def __exit__(self, exc_type, exc, tb):
                    return False

            return _Ctx(self._external_con)
        return duckdb.connect(str(self.cfg.db_path))

    def _ensure_ddls(self, con: duckdb.DuckDBPyConnection, path: str) -> None:
        rules = load_router_yaml(path)
        seed_entity_router(con, rules)
        create_diff(con)


if __name__ == "__main__":
    cfg = DiscoveryConfig(
        bronze_root=Path("/home/faacosta0245695/conflit/conflit_warehouse/data/bronze"),
        router_path=Path(
            "/home/faacosta0245695/conflit/conflit_warehouse/src/diff/router.yaml"
        ),
        db_path=Path("warehouse/database.db"),
        max_workers=8,
    )
    DiscoveryService(cfg).run()
