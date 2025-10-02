import yaml
import json
import math
import pathlib
import uuid
import decimal
from pathlib import Path
from datetime import datetime, date
import numpy as np
import pandas as pd


def load_schema(path: Path):
    with open(path, "r") as f:
        schema = yaml.safe_load(f)
    return schema


def write_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def _json_default(o):
    # datetimes / dates (incl. pandas.Timestamp)
    if isinstance(o, (pd.Timestamp, datetime, date)):
        # keep timezone info if present
        return o.isoformat()
    # numpy scalars
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, (np.floating,)):
        # handle NaN/Inf safely
        return (
            None
            if (isinstance(o, float) and (math.isnan(o) or math.isinf(o)))
            else float(o)
        )
    if isinstance(o, (np.bool_)):
        return bool(o)
    # Decimals
    if isinstance(o, decimal.Decimal):
        return float(o)
    # pathlib paths, UUIDs
    if isinstance(o, (pathlib.Path, uuid.UUID)):
        return str(o)
    # anything else: fallback to str
    return str(o)
