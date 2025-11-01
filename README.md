# Conflict & Crisis Data Warehouse

A reproducible **data engineering and analytics pipeline** for humanitarian and conflict-related datasets.  
This proof-of-concept demonstrates how to build a **bronze–silver–gold lakehouse architecture** with robust **data quality validation**, **manifest-based lineage**, and **snapshot reproducibility** — using **DuckDB** as a database and **Python** for data handeling.

---

## Project Overview

The **Conflict & Crisis Data Warehouse (CCDW)** integrates open sources such as **UNHCR refugee flows**, **ACLED conflict events**, and other humanitarian datasets into a unified, auditable warehouse.  

It is designed to:
- Support **policy analysis and forecasting** in conflict and displacement research;
- Enforce **data quality (DQ)** and schema contracts at every layer;
- Enable **incremental ingestion** and **reproducible snapshots** for longitudinal studies.

---

## Architecture (Medallion )
Sources (UNHCR, ACLED, etc.)
│
Bronze → immutable raw snapshots, manifests, hashes
│
Silver → harmonized & validated tables
│
Gold → analytics-ready & ML feature datasets


---

## Tech Stack

| Area | Tools |
|------|-------|
| Data Engine | **DuckDB**, Parquet |
| Orchestration | `make`, `structlog`, Python CLI |
| Quality & Validation | Custom `DQBuilder`, manifest registry |
| Schema & Metadata | `schemas/` folder, YAML/JSON contracts |
| Testing | `pytest`, synthetic datasets |
| Environment | `uv` |

---

## Quickstart

```bash
# 1. Clone
git clone https://github.com/Costinha66/conflit_warehouse
cd conflit_warehouse

# 2. Install dependencies
uv sync     # or: poetry install

# 3. Run sample pipeline
make run-sample

# 4. Explore results
duckdb results/warehouse.duckdb
``` 

## Repository Structure
```bash
conflit_warehouse/
│
├─ src/
│  ├─ bronze/
│  │  └─ snapshot_maker.py        # snapshot creation + metrics + dq
│  ├─ diff/
│  │  ├─ discovery.py             # scan bronze, populate ingest_manifest, routing
│  │  ├─ parser.py, planner.py, router.py, router.yaml
│  ├─ silver/
│  │  ├─ processor.py             # canonicalize + harmonize → silver tables
│  │  ├─ canonilaze.py, harmonizer.py
│  ├─ gold/
│  │  └─ processor_gold.py        # build analytics marts from silver
│  ├─ core/
│  │  ├─ types.py, config.py, logging.py, metrics.py, time.py, json.py
│  │  ├─ dq/                      # bronze policy + dq helpers
│  │  ├─ lineage/                 # lineage models & emitters
│  │  └─ sql/, infra/duckdb/, infra/yaml_sql/
│  └─ others/
│     ├─ ddls.py                  # DDL helpers (ingest_manifest, etc.)
│     └─ load_dim_country.py
│
├─ schemas/
│  ├─ silver/                     # e.g., refugees_stack.yaml, refugees_internal.yaml
│  └─ gold/                       # e.g., refugees_stack_yearly.yaml
│
├─ tests/
│  └─ test_diff.py                # discovery/manifest tests
│
├─ docs/
│  └─ decisions.md                # decisions & assumptions (SLA, promotion gates)
│
├─ notebooks/
│  └─ exploration.ipynb
│
├─ makefile
├─ pyproject.toml
└─ .pre-commit-config.yaml
```


Author

Filipe Costa
Data Science @ JADS

License

MIT License © 2025 Filipe Costa

