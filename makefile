setup:
	pre-commit install

run.bronze:
	uv run  -m src.bronze.ingest_cli --cutoff_year 2020 > warehouse/run_log/bronze.jsonl

run.bronze.snapshot:
	uv run -m src.bronze.snapshot_maker --cutoff_year 2020 --start_year 2020 > warehouse/run_log/bronze_snapshot.jsonl

run.diff:
	uv run  -m src.diff.discovery

run.silver:
	uv run  -m src.silver.processor

run.gold:
	uv run  -m src.gold.processor_gold

replay:
	rm -rf warehouse/silver/* warehouse/gold/*
	$(MAKE) run.diff run.silver run.gold

lint:
	pre-commit run --all-files
