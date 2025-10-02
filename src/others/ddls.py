def create_silver(con):
    con.execute("""-- ============ SCHEMAS ============
CREATE SCHEMA IF NOT EXISTS silver;

-- ============ ENTITY TABLE ============
-- YAML: entity: refugee_displacement_conformed, grain: year

CREATE TABLE IF NOT EXISTS silver.refugee_displacement_conformed (
  -- business columns (contract)
  origin_country_id     BIGINT    NOT NULL,
  dest_country_id       BIGINT    NOT NULL,
  country_origin        TEXT      NOT NULL,
  country_destination   TEXT      NOT NULL,
  refugees              BIGINT    NOT NULL CHECK (refugees        >= 0),
  asylum_seekers        BIGINT    NOT NULL CHECK (asylum_seekers  >= 0),
  idps                  BIGINT    NOT NULL CHECK (idps            >= 0),
  stateless             BIGINT    NOT NULL CHECK (stateless       >= 0),
  host_community        BIGINT    NOT NULL CHECK (host_community  >= 0),
  others_of_concern     BIGINT    NOT NULL CHECK (others_of_concern>= 0),
  year                  INTEGER   NOT NULL,

  -- extra columns from canonicalizer.output.extra (enrichment)
  origin_region_unhcr   TEXT,
  origin_sub_region_unhcr TEXT,
  dest_region_unhcr     TEXT,
  dest_sub_region_unhcr   TEXT,

  -- lineage (contract.lineage)
  _build_at          TEXT,
  _ingest_run_id        TEXT,
  _transform_version    TEXT,

  -- technical hash over canonical business columns
  row_hash              TEXT      NOT NULL CHECK (length(row_hash) = 64),

  -- constraints (DuckDB supports UNIQUE; treat as PK)
  UNIQUE (country_origin, country_destination, year)
);

-- Helpful indexes (DuckDB creates zone maps; explicit secondary indexes can still help filters)
CREATE INDEX IF NOT EXISTS idx_ref_disp_year
  ON silver.refugee_displacement_conformed (year);

CREATE INDEX IF NOT EXISTS idx_ref_disp_origin
  ON silver.refugee_displacement_conformed (country_origin);

CREATE INDEX IF NOT EXISTS idx_ref_disp_dest
  ON silver.refugee_displacement_conformed (country_destination);

-- ============ EXPOSURE VIEW (hide lineage, keep enrichments) ============
CREATE OR REPLACE VIEW silver.refugee_displacement_conformed_exposed AS
SELECT
  origin_country_id, dest_country_id, country_origin, country_destination, year,
  refugees, asylum_seekers, idps, stateless, host_community, others_of_concern,
  origin_region_unhcr, origin_sub_region_unhcr,
  dest_region_unhcr,   dest_sub_region_unhcr
FROM silver.refugee_displacement_conformed;

-- ============ COMMITS & REJECTS (ops lineage) ============
CREATE TABLE IF NOT EXISTS silver.silver_commits (
  entity          TEXT        NOT NULL,                 -- e.g., 'refugee_displacement_conformed'
  partition_key   TEXT        NOT NULL,                 -- 'YYYY' (grain = year)
  run_id          TEXT        NOT NULL,
  started_at      TIMESTAMP   NOT NULL,
  finished_at     TIMESTAMP   NOT NULL,

  row_count       BIGINT      NOT NULL,
  insert_cnt      BIGINT      NOT NULL,
  update_cnt      BIGINT      NOT NULL,
  delete_cnt      BIGINT      NOT NULL,

  partition_hash  TEXT,                                  -- sha256 over sorted row_hash
  inputs_json     JSON,                                  -- list of file versions used
  dq_summary_json JSON,                                  -- small DQ counters
  spec_version    TEXT,

  PRIMARY KEY (entity, partition_key, run_id)
);

CREATE INDEX IF NOT EXISTS idx_commits_entity_part_time
  ON silver.silver_commits (entity, partition_key, finished_at);

CREATE TABLE IF NOT EXISTS silver.silver_rejects (
  entity         TEXT        NOT NULL,
  partition_key  TEXT        NOT NULL,
  run_id         TEXT        NOT NULL,
  stage          TEXT        NOT NULL,                  -- 'harmonize' | 'canonicalize' | 'dq'
  reject_reasons TEXT        NOT NULL,

  -- minimal lineage for traceability
  _bronze_file   TEXT,
  _ingest_run_id TEXT,

  -- small JSON payload with key fields for debugging
  payload        JSON,

  ts             TIMESTAMP   DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_rejects_entity_part_time
  ON silver.silver_rejects (entity, partition_key, ts);
 """)


def create_lineage(con):
    con.execute("""-- High-level lineage events (one per commit/publish/discover/transform)
CREATE TABLE IF NOT EXISTS lineage_events (
    event_id         TEXT PRIMARY KEY,  -- UUID
    event_time       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, -- when event finished
    event_type       TEXT NOT NULL,     -- 'discover' | 'transform_run' | 'partition_published'
    layer            TEXT NOT NULL,     -- bronze/silver/gold
    entity           TEXT NOT NULL,     -- dataset/entity name
    grain            TEXT,              -- 'year' | 'month' | null if not partitioned
    partition_key    TEXT,              -- e.g. '2020' or '2020-05'
    run_id           TEXT NOT NULL,
    transform_version TEXT NOT NULL,
    spec_version     TEXT,
    rows_in          BIGINT,
    rows_out         BIGINT,
    dq_status        TEXT,              -- 'passed' | 'failed' | 'n/a'
    payload_json     JSON               -- full serialized CoreLineageEvent for audit
);

-- Inputs provenance (zero..many per lineage_event)
CREATE TABLE IF NOT EXISTS lineage_event_inputs (
    event_id      TEXT NOT NULL REFERENCES lineage_events(event_id),
    idx           INTEGER NOT NULL,       -- order of input
    layer         TEXT,
    entity        TEXT,
    partition_key TEXT,
    file_path     TEXT,
    content_hash  TEXT,
    route_id      TEXT,
    PRIMARY KEY (event_id, idx)
); """)


def create_gold(con):
    con.execute("""
  CREATE SCHEMA IF NOT EXISTS gold;

  CREATE TABLE IF NOT EXISTS gold.refugee_stack_yearly (
      -- Keys
      origin_country_id   INT     NOT NULL,
      dest_country_id     INT     NOT NULL,

      -- Convenience ISO3 codes
      country_origin      STRING  NOT NULL,
      country_destination STRING  NOT NULL,

      -- Core populations
      refugees            BIGINT  NOT NULL CHECK (refugees >= 0),
      asylum_seekers      BIGINT  NOT NULL CHECK (asylum_seekers >= 0),
      idps                BIGINT  NOT NULL CHECK (idps >= 0),
      stateless           BIGINT  NOT NULL CHECK (stateless >= 0),
      others_of_concern   BIGINT  NOT NULL CHECK (others_of_concern >= 0),

      -- Impact
      host_community      BIGINT  NOT NULL CHECK (host_community >= 0),

      -- Derived stacks
      cross_border_total  BIGINT  NOT NULL CHECK (cross_border_total >= 0),
      internal_total      BIGINT  NOT NULL CHECK (internal_total >= 0),
      population_of_concern BIGINT NOT NULL CHECK (population_of_concern >= 0),

      -- Year
      year                INT     NOT NULL,

      -- Lineage
      _ingest_run_id      STRING  NOT NULL,
      _transform_version  STRING  NOT NULL,
      _built_at           TIMESTAMP NOT NULL,

      -- Constraints
      CONSTRAINT refugee_stack_yearly_pk PRIMARY KEY (origin_country_id, dest_country_id, year),
      CONSTRAINT refugee_stack_yearly_reconcile CHECK (
          population_of_concern =
              cross_border_total + internal_total + stateless + others_of_concern
      )
  );

  -- 1) Partition commits (audit trail for Gold writes)
CREATE TABLE IF NOT EXISTS gold.gold_commits (
  entity           VARCHAR NOT NULL,
  partition_key    VARCHAR NOT NULL,
  run_id           VARCHAR NOT NULL,
  started_at       TIMESTAMPTZ NOT NULL,
  finished_at      TIMESTAMPTZ NOT NULL,
  row_count        BIGINT NOT NULL,
  insert_cnt       BIGINT NOT NULL,
  update_cnt       BIGINT NOT NULL,
  delete_cnt       BIGINT NOT NULL,
  partition_hash   VARCHAR,                 -- 64-char sha256 or NULL when empty
  inputs           JSON,                    -- array of inputs
  dq_summary       JSON,                    -- DQ report summary
  spec_version     VARCHAR NOT NULL,
  PRIMARY KEY (entity, partition_key, run_id)
);
  """)


def create_dims(con):
    con.execute("""
  CREATE SCHEMA IF NOT EXISTS dims;

  CREATE TABLE IF NOT EXISTS dims.dim_country (
      country_id        INT      NOT NULL,  -- surrogate PK
      iso_country       STRING   NOT NULL,  -- ISO3 (A-Z, length 3)
      country_unhcr     STRING,             -- UNHCR country code
      country_unsd      STRING,             -- UNSD official name
      region_unhcr      STRING,             -- UNHCR region name
      sub_region_unhcr  STRING,             -- UNHCR sub-region name

      -- Constraints
      CONSTRAINT dim_country_pk PRIMARY KEY (country_id),
      CONSTRAINT dim_country_iso_unique UNIQUE (iso_country)
  );
  """)


def create_diff(con):
    con.execute("""
    CREATE TABLE IF NOT EXISTS ingest_manifest (
        source_id        TEXT,
        file_path        TEXT,
        content_hash     TEXT,
        file_size        BIGINT,
        coverage_start   TEXT,
        coverage_end     TEXT,
        grain            TEXT,
        discovered_at    TIMESTAMP,
        processed_at     TIMESTAMP,
        status           TEXT,   -- 'pending' | 'processed' | 'failed' | 'superseded'
        run_id           TEXT,
        notes            TEXT,
        PRIMARY KEY (file_path, content_hash)
        );
    CREATE TABLE IF NOT EXISTS dirty_partitions (
        entity         TEXT,
        partition_key  TEXT,
        reason         TEXT,
        first_seen_at  TIMESTAMP,
        last_seen_at   TIMESTAMP,
        status         TEXT
            );
    CREATE TABLE IF NOT EXISTS entity_router (
        route_id        TEXT,                         -- stable id, e.g., 'unhcr_refugees'
        source_id       TEXT,                         -- e.g., 'unhcr'
        path_regex      TEXT,                         -- regex against full file_path
        entity          TEXT,                         -- e.g., 'refugee_displacement_conformed'
        grain    TEXT CHECK (grain IN ('month','year')),
        enabled         BOOLEAN DEFAULT TRUE,
        notes           TEXT,
        created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (route_id)
        );
    CREATE TABLE IF NOT EXISTS manifest_partition_link (
        file_path     TEXT,
        content_hash  TEXT,
        entity        TEXT,
        partition_key TEXT,               -- canonical (e.g., 'YYYY-MM' or 'YYYY')
        route_id      TEXT,               -- provenance: which router rule matched
        linked_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (file_path, content_hash, entity, partition_key)
);
  """)
