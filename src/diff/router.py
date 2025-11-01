from pathlib import Path
from typing import List, Dict, Any
import yaml
import duckdb
import pandas as pd
from src.core.miscelannious import _glob_to_regex


def load_router_yaml(path: Path) -> List[Dict[str, Any]]:
    cfg = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    rules = []
    for r in cfg.get("routes", []):
        path_regex = r.get("path_regex") or _glob_to_regex(r["pattern"])
        rules.append(
            {
                "route_id": r["route_id"],
                "source_id": r.get("source_id"),  # can be None
                "path_regex": path_regex,
                "entity": r["entity"],
                "grain": r["grain"],  # "year" | "month"
                "enabled": bool(r.get("enabled", True)),
                "notes": r.get("notes"),
            }
        )
    return rules


def seed_entity_router(
    con: duckdb.DuckDBPyConnection, rules: List[Dict[str, Any]]
) -> None:
    """
    Upsert rules into entity_router from a list of dicts.
    Uses INSERT ... ON CONFLICT if available; else DELETE+INSERT.
    """
    con.execute("BEGIN;")
    try:
        # Create schema/table if missing (idempotent)
        con.execute("""
        CREATE TABLE IF NOT EXISTS entity_router (
            route_id   TEXT PRIMARY KEY,
            source_id  TEXT,
            entity     TEXT,
            grain      TEXT CHECK (grain IN ('month','year')),
            enabled    BOOLEAN DEFAULT TRUE,
            notes      TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        df = pd.DataFrame.from_records(
            rules,
            columns=["route_id", "source_id", "entity", "grain", "enabled", "notes"],
        )
        con.register("router_stage", df)
        # DuckDB supports ON CONFLICT; if yours doesn’t, fallback to DELETE+INSERT (uncomment below)
        con.execute("""
        INSERT INTO entity_router(route_id, source_id, entity, grain, enabled, notes)
        SELECT route_id, source_id, entity, grain, enabled, notes
        FROM router_stage
        ON CONFLICT (route_id) DO UPDATE SET
          source_id=excluded.source_id,
          entity=excluded.entity,
          grain=excluded.grain,
          enabled=excluded.enabled,
          notes=excluded.notes;
        """)
        con.execute("COMMIT;")
    except Exception:
        con.execute("ROLLBACK;")
        raise
    finally:
        try:
            con.unregister("router_stage")
        except Exception:
            pass
