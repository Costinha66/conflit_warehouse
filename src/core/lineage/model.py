# core/lineage.py
from __future__ import annotations
from typing import List, Optional, Dict, Any
from uuid import uuid4

from src.core.types import CoreLineageEvent, DatasetRef, RunRef, InputRef
from src.core.time import now_utc


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
