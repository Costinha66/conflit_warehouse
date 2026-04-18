# Contributing to conflit_warehouse

## Getting Started

```bash
# Clone and install
git clone https://github.com/Costinha66/conflit_warehouse
cd conflit_warehouse
uv sync

# Install pre-commit hooks (runs ruff on every commit)
make setup
# or: pre-commit install
```

Python 3.12+ is required. The project uses `uv` for dependency management.

---

## Development Workflow

- Work on a feature branch: `git checkout -b feat/your-feature`
- Keep commits focused and descriptive: `feat: add ACLED silver schema`, `fix: handle null country codes in harmonizer`
- Run lint before pushing: `make lint` (or `pre-commit run --all-files`)
- Open a PR against `main`

---

## Running Tests

```bash
pytest
```

Tests live in `tests/`

---

## Code Style

`ruff` is enforced via pre-commit. It runs automatically on `git commit`. To run manually:

```bash
pre-commit run --all-files
```

---

## Adding a New Data Source

Adding a new source (e.g., IDMC internal displacement data) involves four steps:

**1. Bronze** — Place source files in `data/raw/<source>/`. Bronze snapshots are created automatically by `snapshot_maker.py` given a `SnapshotConfig`.

**2. Router** — Register the source in `src/diff/router.yaml`:
```yaml
- pattern: "data/raw/idmc/**/*.csv"
  entity: idmc_displacement
  grain: year
  source_id: idmc
  route_id: idmc_displacement_year
```

**3. Silver schema** — Create `schemas/silver/<entity>.yaml` defining:
- `contract.primary_key`, `contract.partitions`
- Column types and DQ rules (`non_negative`, `not_null`, etc.)
- `canonicalizer` section (dim joins, dedup key, row hash columns)

**4. Gold schema** — If the source contributes to an analytics mart, create `schemas/gold/<mart>.yaml` with:
- `transform.source` pointing to the silver entity
- `transform.select_map` and `transform.derivations`
- Reconciliation assertions under `contract.tests`

---

## Opening a PR

Before marking a PR ready for review, confirm:

- [ ] `pytest` passes
- [ ] `pre-commit run --all-files` is clean (no ruff errors)
- [ ] If you added/changed a schema, the corresponding YAML in `schemas/` is updated
- [ ] If you changed the pipeline flow, `docs/decisions.md` reflects the rationale
- [ ] The PR description explains *why* the change is needed, not just what it does
