from pathlib import Path
import re
from typing import Optional

YEAR_RE = re.compile(r"^(?P<start>\d{4})$")
YEAR_RANGE_RE = re.compile(r"^(?P<start>\d{4})-(?P<end>\d{4})$")
MONTH_RE = re.compile(r"^(?P<start>\d{4}-\d{2})$")
MONTH_RANGE_RE = re.compile(r"^(?P<start>\d{4}-\d{2})-(?P<end>\d{4}-\d{2})$")


def _parse_cov(
    fp: Path,
) -> tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """returns (start, end, grain, error_reason)"""
    tok = fp.stem.split("-part-")[0]
    if m := YEAR_RANGE_RE.match(tok):
        s, e = m["start"], m["end"]
        return (s, e, "year", None if int(s) <= int(e) else "year_range_invalid")
    if m := YEAR_RE.match(tok):
        s = m["start"]
        return (s, s, "year", None)
    if m := MONTH_RANGE_RE.match(tok):
        s, e = m["start"], m["end"]
        return (s, e, "month", None if s <= e else "month_range_invalid")
    if m := MONTH_RE.match(tok):
        s = m["start"]
        return (s, s, "month", None)
    return (None, None, None, f"unrecognized:{fp.name}")
