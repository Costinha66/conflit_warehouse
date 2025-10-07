from datetime import datetime, timezone
import time


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


class StageTimer:
    def __enter__(self):
        self.t0 = time.time()
        self.duration_sec = None
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.duration_sec = round(time.time() - self.t0, 3)
