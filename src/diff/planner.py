def planner(con):
    dirty = con.execute(""" SELECT entity, partition_key
                                FROM dirty_partitions
                                WHERE status = 'dirty'
                                ORDER BY last_seen_at, entity, partition_key; """).df()
    for entity, partition_key in zip(dirty["entity"], dirty["partition_key"]):
        plan_df = con.execute(
            """
            WITH candidates AS (
                SELECT mpl.file_path, mpl.content_hash, m.file_size, m.discovered_at, m.status
                FROM manifest_partition_link mpl
                JOIN ingest_manifest m
                    ON m.file_path = mpl.file_path
                AND m.content_hash = mpl.content_hash
                WHERE mpl.entity = ?
                    AND mpl.partition_key = ?
                ),
                latest_per_path AS (
                SELECT * EXCLUDE rn
                FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY file_path ORDER BY discovered_at DESC) rn
                    FROM candidates
                    WHERE status IN ('pending','processed')
                ) WHERE rn = 1
                )
                SELECT * FROM latest_per_path
                ORDER BY discovered_at ASC, file_path ASC;

            """,
            [entity, partition_key],
        ).df()
        if plan_df.empty:
            pass

        yield {
            "entity": entity,
            "partition_key": partition_key,
            "inputs": plan_df.to_dict("records"),
        }
