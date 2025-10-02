# core/lineage.py
from __future__ import annotations
from typing import List, Optional, Dict, Any
from uuid import uuid4
import json

from src.core.dataclasses import CoreLineageEvent, DatasetRef, RunRef, InputRef
from src.core.miscelannious import now_utc, json_safe


# ---------- translators (one per capture point) ----------
def make_discover_event(
    *,
    layer: str,
    entity: str,
    run_id: str,
    transform_version: str,
    inputs: List[Dict[str, Any]],
    metrics: Dict[str, Any],
    extra: Dict[str, Any],
) -> CoreLineageEvent:
    return CoreLineageEvent(
        event_id=str(uuid4()),
        event_schema_version=1,
        event_type="discover",
        dataset=DatasetRef(layer=layer, entity=entity),
        run=RunRef(
            run_id=run_id,
            transform_version=transform_version,
            started_at=now_utc(),
            finished_at=now_utc(),
        ),
        inputs=[
            InputRef(
                dataset=DatasetRef(
                    layer=i.get("layer", "bronze"),
                    entity=i.get("entity", "_files"),
                    partition_key=i.get("partition_key"),
                ),
                file_path=i.get("file_path"),
                content_hash=i.get("content_hash"),
                route_id=i.get("route_id"),
            )
            for i in inputs
        ],
        outputs=[DatasetRef(layer=layer, entity=entity)],
        metrics=metrics,
        dq={"status": "n/a"},
        extra=extra,
    )


def make_transform_run_event(
    *,
    layer: str,
    entity: str,
    partition_key: str,
    run_id: str,
    transform_version: str,
    inputs: List[Dict[str, Any]],
    metrics: Dict[str, Any],
    dq: Dict[str, Any],
    extra: Dict[str, Any],
) -> CoreLineageEvent:
    return CoreLineageEvent(
        event_id=str(uuid4()),
        event_schema_version=1,
        event_type="transform_run",
        dataset=DatasetRef(layer=layer, entity=entity, partition_key=partition_key),
        run=RunRef(
            run_id=run_id,
            transform_version=transform_version,
            started_at=now_utc(),
            finished_at=now_utc(),
        ),
        inputs=[
            InputRef(
                dataset=DatasetRef(**i["dataset"]),
                file_path=i.get("file_path"),
                content_hash=i.get("content_hash"),
                route_id=i.get("route_id"),
            )
            for i in inputs
        ],
        outputs=[DatasetRef(layer=layer, entity=entity, partition_key=partition_key)],
        metrics=metrics,
        dq=dq,
        extra=extra,
    )


def make_partition_published_event(
    *,
    layer: str,
    entity: str,
    grain: str,
    partition_key: str,
    run_id: str,
    transform_version: str,
    rows_in: int,
    rows_out: int,
    inputs: List[Dict[str, Any]],
    spec_version: str,
    dq_status: str = "passed",
    extra: Optional[Dict[str, Any]] = None,
) -> CoreLineageEvent:
    dataset = DatasetRef(
        layer=layer, entity=entity, grain=grain, partition_key=partition_key
    )
    return CoreLineageEvent(
        event_id=str(uuid4()),
        event_schema_version=1,
        event_type="partition_published",
        dataset=dataset,
        run=RunRef(
            run_id=run_id,
            transform_version=transform_version,
            started_at=now_utc(),
            finished_at=now_utc(),
        ),
        inputs=[
            InputRef(
                dataset=dataset,
                file_path=i.get("file_path"),
                content_hash=i.get("content_hash"),
                route_id=i.get("route_id"),
            )
            for i in inputs
        ],
        outputs=[dataset],
        metrics={"rows_in": rows_in, "rows_out": rows_out},
        dq={"status": dq_status},
        extra={"spec_version": spec_version, **(extra or {})},
    )


# ---------- emitter interface + two adapters ----------
class LineageEmitter:
    def emit(self, event: CoreLineageEvent) -> None:
        raise NotImplementedError


class StdoutJsonEmitter(LineageEmitter):
    def emit(self, event: CoreLineageEvent) -> None:
        print(json.dumps(event.to_payload(), ensure_ascii=False, default=json_safe))


class DuckDBAuditEmitter(LineageEmitter):
    """
    Expects two tables:
      lineage_events(event_id, event_time, event_type, layer, entity, grain, partition_key,
                     run_id, transform_version, spec_version, rows_in, rows_out, dq_status, payload_json)
      lineage_event_inputs(event_id, idx, layer, entity, partition_key, file_path, content_hash, route_id)
    """

    def __init__(self, con):
        self.con = con

    def emit(self, event: CoreLineageEvent) -> None:
        p = event.to_payload()
        ev_row = [
            p["event_id"],
            p["run"]["finished_at"],  # event_time
            p["event_type"],
            p["dataset"]["layer"],
            p["dataset"]["entity"],
            p["dataset"].get("grain"),
            p["dataset"].get("partition_key"),
            p["run"]["run_id"],
            p["run"]["transform_version"],
            p["extra"].get("spec_version"),
            p["metrics"].get("rows_in"),
            p["metrics"].get("rows_out"),
            p["dq"].get("status"),
            json.dumps(p, ensure_ascii=False, default=json_safe),
        ]
        self.con.execute(
            """
            INSERT INTO lineage_events
            (event_id, event_time, event_type, layer, entity, grain, partition_key,
             run_id, transform_version, spec_version, rows_in, rows_out, dq_status, payload_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
            ev_row,
        )

        # inputs
        for idx, inp in enumerate(p.get("inputs", [])):
            self.con.execute(
                """
                INSERT INTO lineage_event_inputs
                (event_id, idx, layer, entity, partition_key, file_path, content_hash, route_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """,
                [
                    p["event_id"],
                    idx,
                    (inp["dataset"].get("layer")),
                    (inp["dataset"].get("entity")),
                    (inp["dataset"].get("partition_key")),
                    inp.get("file_path"),
                    inp.get("content_hash"),
                    inp.get("route_id"),
                ],
            )


# ---------- one-liner helper ----------
def emit_lineage(event: CoreLineageEvent, emitters: List[LineageEmitter]) -> None:
    for e in emitters:
        e.emit(event)
