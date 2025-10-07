from dataclasses import replace
from pathlib import Path
from datetime import datetime, timezone
from typing import Mapping, Any, Optional

import duckdb
import structlog

from src.core.logging import configure_logging
from src.core.metrics import collect_file_metrics
from src.core.time import StageTimer
from src.core.json import write_json
from src.core.dq.bronze_policy import DQBuilderBronze
from src.core.types import DQResult, SnapshotConfig


# ---------- Service ----------
class SnapshotService:
    def __init__(
        self, cfg: SnapshotConfig, dq_builder: DQBuilderBronze, log_json: bool = True
    ) -> None:
        configure_logging(json_logs=log_json)
        self.run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        self.cfg = self._resolve_cfg(cfg)
        self.log = structlog.get_logger().bind(
            stage="bronze_snapshot",
            source=self.cfg.source,
            run_id=self.run_id,
            snapshot_version=self.cfg.snapshot_version,
            start_year=self.cfg.start_year,
            cutoff_year=self.cfg.cutoff_year,
            mode=self.cfg.mode,
        )
        self.dq_builder = dq_builder

    def run(self) -> None:
        con: Optional[duckdb.DuckDBPyConnection] = None
        metrics: Mapping[str, Any] | None = None
        dq: DQResult | None = None
        out_dir: Path | None = None
        out_parquet: Path | None = None

        try:
            self.log.info("snapshot_start")
            with StageTimer() as t:
                con = self._connect()
                out_dir, out_parquet = self._prepare_paths()

                if out_parquet.exists() and not self.cfg.overwrite:
                    self.log.warning("snapshot_skip_exists", file=str(out_parquet))
                    summary = self._make_summary_dict(
                        out_parquet,
                        metrics=None,
                        dq=None,
                        duration_sec=0.0,
                        note="skipped (exists, overwrite=False)",
                    )
                    write_json(out_dir / "_dq_summary.json", summary)
                    return

                self._create_view(con)  # TODO: implement
                self._copy_view(con, out_parquet)  # TODO: implement

                metrics = self._collect_metrics(out_parquet)
                dq = self._build_dq(metrics)

            summary = self._make_summary_dict(
                out_parquet, metrics, dq, duration_sec=t.duration_sec
            )
            self._write_summary(out_dir, summary)
            self.log.info(
                "snapshot_done",
                duration_sec=t.duration_sec,
                dq_passed=dq.passed if dq else None,
                dq_level=dq.level if dq else None,
                dq_metrics=(dq.metrics if dq else None),
            )
        except Exception as e:
            self.log.error("snapshot_failed", exc_info=True)
            # Best-effort failure summary
            try:
                if out_dir is None:
                    # Make a reasonable default if failure happened early
                    out_dir = self._default_out_dir()
                    out_dir.mkdir(parents=True, exist_ok=True)
                summary = self._make_summary_dict(
                    out_parquet or (out_dir / "UNKNOWN.parquet"),
                    metrics=metrics,
                    dq=dq,
                    duration_sec=None,
                    error=repr(e),
                )
                self._write_summary(out_dir, summary)
            finally:
                # re-raise original error
                raise
        finally:
            if con:
                con.close()

    def _resolve_cfg(self, cfg: SnapshotConfig) -> SnapshotConfig:
        """Freeze snapshot_version once; validate years."""
        snap = cfg.snapshot_version or datetime.now().date().isoformat()
        if cfg.start_year > cfg.cutoff_year:
            raise ValueError(
                f"start_year ({cfg.start_year}) > cutoff_year ({cfg.cutoff_year})"
            )
        return replace(cfg, snapshot_version=snap)

    def _connect(self) -> duckdb.DuckDBPyConnection:
        """Open DuckDB connection."""
        db = str(self.cfg.db_path)
        self.log.debug("duckdb_connect", db_path=db)
        return duckdb.connect(db)

    def _prepare_paths(self) -> tuple[Path, Path]:
        """Create output directory and return (out_dir, out_parquet)."""
        out_dir = self.cfg.out_root / f"date={self.cfg.snapshot_version}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_parquet = (
            out_dir / f"{self.cfg.start_year}-{self.cfg.cutoff_year}-part-000.parquet"
        )
        self.log.debug(
            "paths_prepared", out_dir=str(out_dir), out_parquet=str(out_parquet)
        )
        return out_dir, out_parquet

    def _default_out_dir(self) -> Path:
        return self.cfg.out_root / f"date={self.cfg.snapshot_version}"

    def _create_view(self, con: duckdb.DuckDBPyConnection) -> None:
        """Create/replace a temp view with filtered & typed columns."""
        sql = """
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
        FROM read_csv_auto(?, HEADER=TRUE)
        WHERE TRY_CAST(Year AS INT) BETWEEN ? AND ?
        """
        params = [str(self.cfg.csv_path), self.cfg.start_year, self.cfg.cutoff_year]
        self.log.debug(
            "sql_create_view", sql="CREATE VIEW snapshot_view ...", params=params
        )
        con.execute(sql, params)

    def _copy_view(self, con: duckdb.DuckDBPyConnection, out_parquet: Path) -> None:
        """Persist the temp view to Parquet with compression."""
        try:
            con.execute(
                "COPY snapshot_view TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",
                [str(out_parquet)],
            )
        except duckdb.Error:
            safe = str(out_parquet).replace("'", "''")
            con.execute(
                f"COPY snapshot_view TO '{safe}' (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
        self.log.debug("copied_snapshot", file=str(out_parquet))

    def _collect_metrics(self, out_parquet: Path) -> Mapping[str, Any]:
        """Compute file metrics (records/bytes/hash)."""
        m = collect_file_metrics(out_parquet)
        self.log.debug(
            "file_metrics", **{k: m.get(k) for k in ("records", "bytes", "hash")}
        )
        return m

    def _build_dq(self, metrics: Mapping[str, Any]) -> DQResult:
        """Run DQ checks for Bronze; keep it fast and deterministic."""
        dq = self.dq_builder(metrics, self.cfg.cutoff_year, self.cfg.start_year)
        self.log.debug("dq_result", dq_passed=dq.passed, dq_level=dq.level)
        return dq

    def _make_summary_dict(
        self,
        out_parquet: Path,
        metrics: Mapping[str, Any] | None,
        dq: DQResult | None,
        duration_sec: float | None,
        error: str | None = None,
        note: str | None = None,
    ) -> dict[str, Any]:
        """Single builder for success/failure summaries."""
        return {
            "source": self.cfg.source,
            "snapshot_version": self.cfg.snapshot_version,
            "cutoff_year": self.cfg.cutoff_year,
            "start_year": self.cfg.start_year,
            "file": str(out_parquet),
            "records": None if not metrics else metrics.get("records"),
            "bytes": None if not metrics else metrics.get("bytes"),
            "hash": None if not metrics else metrics.get("hash"),
            "duration_sec": duration_sec,
            "dq_passed": None if not dq else dq.passed,
            "dq_level": None if not dq else dq.level,
            "dq_metrics": {} if not dq else dict(dq.metrics),
            "run_id": self.run_id,
            "error": error,
            "note": note,
        }

    def _write_summary(self, out_dir: Path, summary: Mapping[str, Any]) -> None:
        """Persist summary JSON alongside the snapshot file."""
        write_json(out_dir / "_dq_summary.json", summary)
        self.log.debug("summary_written", path=str(out_dir / "_dq_summary.json"))


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--db_path", required=True)
    p.add_argument("--csv_path", required=True)
    p.add_argument("--out_root", required=True)
    p.add_argument("--snapshot_version")
    p.add_argument("--cutoff_year", type=int, default=2020)
    p.add_argument("--start_year", type=int, default=2020)
    p.add_argument("--overwrite", action="store_true")
    args = p.parse_args()

    cfg = SnapshotConfig(
        db_path=Path(args.db_path),
        csv_path=Path(args.csv_path),
        out_root=Path(args.out_root),
        snapshot_version=args.snapshot_version,
        cutoff_year=args.cutoff_year,
        start_year=args.start_year,
        overwrite=args.overwrite,
    )
    SnapshotService(cfg, dq_builder=DQBuilderBronze).run()
