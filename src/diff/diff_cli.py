# src/diff/unhcr_diff_cli.py
import time
import structlog
from src.core.log import configure_logging


def main(t0: str, t1: str):
    configure_logging(True)
    log = structlog.get_logger()
    s = time.time()
    # TODO: compute INSERT/UPDATE/DELETE between bronze t0 and t1
    log.info(
        "diff_done",
        source="unhcr",
        t0=t0,
        t1=t1,
        inserts=0,
        updates=0,
        deletes=0,
        duration_sec=round(time.time() - s, 3),
    )


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--t0", required=True)
    p.add_argument("--t1", required=True)
    a = p.parse_args()
    main(a.t0, a.t1)
