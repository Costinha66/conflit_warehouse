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
├── src/
│   ├── diff/      # Ingestion, discovery  routing logic
│   ├── dq/        # Data quality checks & builders
│   └── snapshot/  # Snapshot creation and management
│
├── schemas/       # Table contracts and DQ definitions
├── notebooks/     # Exploratory data analysis
├── tests/         # Unit and integration tests
├── docs/          # documentation
├── makefile       # Operational entrypoints
└── pyproject.toml # Project configuration
```


Author

Filipe Costa
Data Science @ JADS

License

MIT License © 2025 Filipe Costa

