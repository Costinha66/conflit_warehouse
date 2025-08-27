# src/gold/build_mart_cli.py
import time
import structlog
from src.core.log import configure_logging


def main():
    configure_logging(True)
    log = structlog.get_logger()
    s = time.time()
    # TODO: build country_year_mart
    log.info("gold_build_done", rows=0, duration_sec=round(time.time() - s, 3))


if __name__ == "__main__":
    main()
