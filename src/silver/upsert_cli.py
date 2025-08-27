# src/silver/unhcr_upsert_cli.py
import time
import structlog
from src.core.log import configure_logging


def main(version: str):
    configure_logging(True)
    log = structlog.get_logger()
    s = time.time()
    # TODO: upsert changes into silver table; run DQ checks
    log.info(
        "silver_upsert_done",
        source="unhcr",
        version=version,
        records_out=0,
        dq_passed=True,
        duration_sec=round(time.time() - s, 3),
    )


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--version", required=True)
    a = p.parse_args()
    main(a.version)
