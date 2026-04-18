# conflit_warehouse — Portfolio Upgrade Plan

## Context: Who This Is For

**Filipe Costa** — Data Scientist / EngD candidate, ~3 years industry experience, strong ML modeling background.
Targeting **mid-senior DS/MLE roles** at high-stack companies. Has an **8-month job search runway**.

This project (`conflit_warehouse`) is a key portfolio piece that directly addresses two of the identified career gaps:
- **Gap 2** — Distributed systems & data infrastructure (no demonstrated ownership of large-scale data systems)
- **Gap 5** — Public technical footprint (no visible GitHub activity or writing)

It also has extension potential into **Gap 1** (Production ML / MLOps) in later phases.

---

## Project Summary (Current State)

A reproducible **data engineering and analytics pipeline** for humanitarian and conflict-related datasets (UNHCR, ACLED).

**Architecture:** Bronze → Silver → Gold medallion lakehouse  
**Stack:** DuckDB, Python, Parquet, pytest, pre-commit, uv, makefile  
**Key features:** Custom DQBuilder, manifest-based lineage, SHA256 snapshot hashing, YAML schema contracts  
**Structure:**
```
conflit_warehouse/
├─ src/
│  ├─ bronze/        # snapshot creation + DQ
│  ├─ diff/          # discovery, manifest, routing
│  ├─ silver/        # canonicalize + harmonize
│  ├─ gold/          # analytics marts
│  └─ core/          # types, config, logging, metrics, lineage, DQ
├─ schemas/          # silver + gold YAML contracts
├─ tests/            # pytest
├─ docs/             # decisions.md
├─ notebooks/        # exploration.ipynb
├─ makefile
├─ pyproject.toml
└─ .pre-commit-config.yaml
```

**Current weaknesses:**
- No README visuals or output examples
- No GitHub description or topics
- No orchestration (makefile only)
- No CI/CD
- Notebook has no visible outputs
- No versioned release

---

## Upgrade Plan

### Phase 1 — Polish & Publish
*Target: 1-2 weekends | Aligns with Month 1-2 GitHub cleanup goal*

- [ ] Add architecture diagram to README (bronze→silver→gold flow)
- [ ] Add a concrete output example in README (synthetic data table or screenshot)
- [ ] Add "why this matters" intro — frame around humanitarian impact
- [ ] Fill in GitHub repo description and add topics: `data-engineering`, `duckdb`, `medallion-architecture`, `humanitarian-data`, `python`
- [ ] Run pipeline end-to-end and commit notebook with visible outputs (gold-layer query, DQ report, lineage trace)
- [ ] Add `CONTRIBUTING.md`
- [ ] Tag a `v0.1.0` release

---

### Phase 2 — Upgrade the Stack
*Target: Month 2-3 | Bridges Gap 2 — data infrastructure depth*

- [ ] **Add Airflow or Prefect orchestration** — replace makefile with a proper DAG
  - This is the highest-ROI upgrade; every data engineering interview asks about orchestration
- [ ] **Add DVC** for data versioning of bronze snapshots
  - Ties into Gap 1 (MLOps) since DVC is already needed for another project in the career plan
- [ ] **Add CI/CD pipeline** via GitHub Actions
  - On every push: lint → pytest → (optional) run sample pipeline
  - Should take ~2 hours, looks very professional

---

### Phase 3 — Write About It
*Target: Month 2 | Directly hits Gap 5 — first blog post*

Write a technical blog post using this project as the subject.

**Suggested title:** *"Building a Humanitarian Data Warehouse with DuckDB: Bronze, Silver, Gold on a Laptop"*

**Angle:** Production-grade data engineering patterns applied to a meaningful domain, explained clearly.
- Why medallion architecture?
- Why DuckDB over Postgres/Spark for this scale?
- How does the DQ + lineage system work?
- What does the manifest registry give you?

This hits the sweet spot between technical depth and accessibility — the kind of post that gets shared in data engineering communities.

---

### Phase 4 — Extend to ML
*Target: Month 4-5 | Bridges data engineering → ML/MLOps track*

- [ ] Build a gold-layer **feature table for displacement forecasting**
  - Features: refugee flows + conflict intensity → displacement pressure on host countries
- [ ] Train a baseline forecasting model (even a simple one)
- [ ] Track experiments with **MLflow** (model registry, artifact versioning)
- [ ] Document the feature engineering decisions in `docs/`

This transforms the project from a pure data engineering demo into a **full DS pipeline showcase**:
`raw data → warehouse → features → model → tracked experiment`

---

## What This Project Signals to Hiring Managers

| Signal | Evidence |
|---|---|
| Data infrastructure ownership | Medallion architecture, DQ, lineage, manifests |
| Real engineering practices | Pre-commit, pytest, schema contracts, CI/CD |
| Domain judgment | Meaningful, non-trivial humanitarian data |
| End-to-end thinking | Bronze → gold → ML features |
| Communication | Blog post explaining design decisions |

---

## How to Use This File with Claude Code

When starting a Claude Code session in this repo, reference this file for full context:

```
I'm working on upgrading this repo into a portfolio project. Read @PLAN.md for full context on my background, career goals, and the 4-phase upgrade plan. Let's start with Phase 1 — help me build the architecture diagram and improve the README.
```

Adjust the final sentence depending on which phase you're working on.
