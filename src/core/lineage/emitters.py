from src.core.types import CoreLineageEvent
from src.core.json import json_safe
from typing import List
import json


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


def emit_lineage(event: CoreLineageEvent, emitters: List[LineageEmitter]) -> None:
    for e in emitters:
        e.emit(event)
