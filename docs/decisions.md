# 📄 Decisions & Assumptions – Conflict & Crisis Data Warehouse POC

## Assumptions
- **Static Sources**
  Current data sources (e.g. UNHCR Persons of Concern CSV) are static extracts. No true “live” feed is available, so change over time is simulated.
- **Layered Architecture**
  The warehouse follows a 3-layer model:
  - **Bronze** – immutable raw snapshots, typed but not transformed.
  - **Silver** – conformed state, deduplicated and harmonized (ISO codes, unique keys).
  - **Gold** – analytical marts for dashboards and policy analysis.

---

## Decisions

### Snapshot Semantics
- **Cumulative Snapshots**
  Each Bronze snapshot represents the *entire dataset up to a cutoff year*.
  Example: cutoff = 2021 → includes all rows with `year ≤ 2021`.
- **Change Simulation**
  To mimic real-world data movement, synthetic revisions are introduced:
  - **INSERTs**: new years or corridors appear.
  - **UPDATEs**: value changes for existing rows (e.g. +10%).
  - **DELETEs**: occasional removals to test delete policies.
- **Rationale**
  This mirrors how humanitarian datasets evolve (new data, revisions, late backfills).

### Data Quality Policy
- **Layered DQ Checks**
  - **Bronze**
    - Goal: freeze source state.
    - Checks: schema conformity, column presence, basic type casting, non-negativity, year bounds.
    - Policy: always write snapshot (even if failing), log `dq_passed=false`. No fixes in Bronze.
  - **Silver**
    - Goal: trustworthy conformed state.
    - Checks: ISO mapping coverage, unique keys, non-negative counts, null thresholds, freshness.
    - Policy: promotion blocked if critical checks fail (`promoted=false`). Failed rows quarantined to `_rejects`.
  - **Gold**
    - Goal: analysis-ready marts.
    - Built only from latest promoted Silver. Skipped if upstream is red.

- **Promotion Gates**
  A table only advances to the next layer if DQ passes (hard stop on CRITICAL issues).

### Logging & Observability
- **Structured Logging (structlog)**
  Every stage logs `snapshot_start` and `snapshot_done/failed` with fields:
  - `run_id`, `stage`, `source`, `snapshot_version`, `start_year`, `cutoff_year`, `records`, `bytes`, `hash`, `duration_sec`, `dq_passed`, `dq_level`, `dq_metrics`.
- **File Artifacts**
  Each snapshot folder contains:
  - `part-000.parquet` – the data.
  - `_dq_summary.json` – metrics + DQ outcome.

---

### AI-Assisted Architecture Verification (Claude Code)

- **Tool Used**
  [Claude Code](https://claude.ai/code) (Anthropic) was used interactively throughout the POC to verify architectural decisions, review code structure, and cross-check implementation against documented intent.

- **Scope of Use**
  - Verified that the Bronze → Silver → Gold layering is consistently applied across `src/bronze/`, `src/silver/`, `src/gold/`, and `src/orchestration/`.
  - Confirmed that DQ policy (gate-at-promotion, quarantine-to-`_rejects`, `dq_passed` flag) is reflected in `src/core/dq/` and `src/bronze/snapshot_maker.py`.
  - Cross-checked that structured logging fields (`run_id`, `snapshot_version`, `hash`, `dq_metrics`, etc.) documented here match the implementation in `src/core/logging.py` and `src/core/lineage/`.
  - Reviewed schema contracts in `schemas/silver/` and `schemas/gold/` for alignment with documented promotion gates.
  - Assessed CI/CD setup (`.github/workflows/ci.yml`, `.pre-commit-config.yaml`, `pyproject.toml`) for consistency with the stated toolchain (ruff, pytest, DVC, Prefect).

- **Nature of Verification**
  Claude Code performed static analysis — reading source files, tracing data flow across modules, and comparing code to this document. It did not execute the pipeline or generate test data. All findings were reviewed and accepted by the author.
