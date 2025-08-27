setup:
	pre-commit install

run.diff:
	python -m src.diff.diff_cli --t0 2025-08-01 --t1 2025-08-27 > warehouse/run_log/bronze.jsonl

run.silver:
	python -m src.silver.upsert_cli --version 2025-08-27 > warehouse/run_log/silver.jsonl

run.gold:
	python -m src.gold.build_cli > warehouse/run_log/gold.jsonl

replay:
	rm -rf warehouse/silver/* warehouse/gold/*
	$(MAKE) run.diff run.silver run.gold

lint:
	pre-commit run --all-files
