import json
from dataclasses import asdict
from src.core.json import _json_default
from src.core.lineage import (
    make_partition_published_event,
    emit_lineage,
    DuckDBAuditEmitter,
    StdoutJsonEmitter,
)
from src.core.dataclasses import CommitEvent
from src.core.miscelannious import _quote_ident


def insert_commit(
    con,
    ev: CommitEvent,
    on_conflict: str = "ignore",
):
    layer = ev.part.layer  # e.g., "silver"
    commit_entity = ev.commit_entity  # e.g., "silver_commits"
    entity = ev.part.entity
    partition_key = str(ev.part.partition_key)
    run_id = ev.run.run_id

    # table identifiers must be inlined (DuckDB can't bind them as parameters)
    table_fqn = f"{_quote_ident(layer)}.{_quote_ident(commit_entity)}"

    # payloads
    inputs_json = json.dumps(ev.inputs, ensure_ascii=False, default=_json_default)
    dq_json = json.dumps(
        asdict(ev.dq_summary), ensure_ascii=False, default=_json_default
    )

    # timestamps: pass as datetimes if your column type is TIMESTAMP
    started_at = ev.run.started_at
    finished_at = ev.finished_at

    if on_conflict == "ignore":
        sql = f"""
            INSERT INTO {table_fqn} AS c
            SELECT ?,?,?,?,?,?,?,?,?,?,?,?,?
            WHERE NOT EXISTS (
              SELECT 1 FROM {table_fqn}
              WHERE entity = ? AND partition_key = ? AND run_id = ?
            );
        """
        params = [
            entity,
            partition_key,
            run_id,
            started_at,
            finished_at,
            ev.row_count,
            ev.insert_cnt,
            ev.update_cnt,
            ev.delete_cnt,
            ev.partition_hash,
            inputs_json,
            dq_json,
            ev.spec_version,
            entity,
            partition_key,
            run_id,
        ]
        con.execute(sql, params)

    elif on_conflict == "replace":
        con.execute(
            f"DELETE FROM {table_fqn} WHERE entity=? AND partition_key=? AND run_id=?;",
            [entity, partition_key, run_id],
        )
        sql = f"""
            INSERT INTO {table_fqn}
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?);
        """
        params = [
            entity,
            partition_key,
            run_id,
            started_at,
            finished_at,
            ev.row_count,
            ev.insert_cnt,
            ev.update_cnt,
            ev.delete_cnt,
            ev.partition_hash,
            inputs_json,
            dq_json,
            ev.spec_version,
        ]
        con.execute(sql, params)
    else:
        raise ValueError("on_conflict must be 'ignore' or 'replace'")

    return con.execute(
        f"SELECT * FROM {table_fqn} WHERE entity=? AND partition_key=? AND run_id=?;",
        [entity, partition_key, run_id],
    ).df()


def write_partition(
    con,
    df_input,
    spec,
    log,
    ev: CommitEvent,
    post_commit_hooks=None,
):
    post_commit_hooks = post_commit_hooks or []

    layer = ev.part.layer
    entity = ev.part.entity
    grain = spec.get("grain", "year")
    partition = str(ev.part.partition_key)

    con.execute("BEGIN;")
    try:
        # Stage temp
        con.register("temp_df", df_input)
        con.execute("DROP TABLE IF EXISTS writing_temp;")
        con.execute("CREATE TEMP TABLE writing_temp AS SELECT * FROM temp_df;")

        # Swap into target
        table_fqn = f"{layer}.{entity}"
        if grain == "year":
            where = "year = ?"
            where_args = [int(partition)]
        elif grain == "month":
            where = "month_start = ?"
            where_args = [partition]
        else:
            raise ValueError(f"Unsupported grain: {grain}")

        con.execute(f"DELETE FROM {table_fqn} WHERE {where};", where_args)
        con.execute(f"INSERT INTO {table_fqn} SELECT * FROM writing_temp;")
        log.info("swap done", extra={"rows_written": len(df_input)})

        # Commit/audit row (serialization handled inside insert_commit)
        insert_commit(con, ev)
        log.info(
            "commit row inserted",
            extra={
                "inserts": ev.insert_cnt,
                "updates": ev.update_cnt,
                "deletes": ev.delete_cnt,
            },
        )

        # Lineage event (same transaction)
        lineage_event = make_partition_published_event(
            layer=layer,
            entity=entity,
            grain=grain,
            partition_key=partition,
            run_id=ev.run.run_id,
            transform_version=ev.run.transform_version,
            rows_in=ev.dq_summary.raw_rows,
            rows_out=ev.row_count,
            inputs=ev.inputs,
            spec_version=ev.spec_version,
            dq_status="passed",  # or derive from your DQ result
        )
        emit_lineage(lineage_event, [DuckDBAuditEmitter(con), StdoutJsonEmitter()])
        log.info("lineage event emitted", extra={"event_id": lineage_event.event_id})

        # Layer-specific finalize hooks (e.g., Silver: mark_manifest_processed, clean_partition)
        for hook in post_commit_hooks:
            hook(
                con=con,
                layer=layer,
                entity=entity,
                partition=partition,
                ev=ev,
                spec=spec,
                log=log,
            )

        con.execute("COMMIT;")
        return {
            "entity": entity,
            "partition": partition,
            "action": "swap",
            "ins": ev.insert_cnt,
            "upd": ev.update_cnt,
            "del": ev.delete_cnt,
        }
    except Exception:
        con.execute("ROLLBACK;")
        log.exception("swap failed; rolled back")
        raise


def silver_finalize_hook(con, layer, entity, partition, ev, spec, log, **_):
    def mark_manifest_processed(con, entity, part, run_id):
        con.execute(
            """
        UPDATE ingest_manifest m
        SET processed_at = CURRENT_TIMESTAMP,
            status       = 'processed',
            run_id       = ?
        FROM manifest_partition_link mpl
        WHERE mpl.entity = ?
            AND mpl.partition_key = ?
            AND m.file_path = mpl.file_path
            AND m.content_hash = mpl.content_hash
            AND m.status = 'pending';
        """,
            [run_id, entity, part],
        )

    def clean_partition(con, entity, part):
        con.execute(
            """
        UPDATE dirty_partitions
        SET status = 'clean', last_seen_at = CURRENT_TIMESTAMP
        WHERE entity = ? AND partition_key = ? AND status IN ('dirty','rebuilding');
        """,
            [entity, part],
        )

    # Only act for silver
    if layer != "silver":
        return
    mark_manifest_processed(con, entity, partition, ev.run.run_id)
    clean_partition(con, entity, partition)
