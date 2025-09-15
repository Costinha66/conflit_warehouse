import json
import yaml
from pathlib import Path


def load_schema(path: Path):
    with open(path, "r") as f:
        schema = yaml.safe_load(f)
    return schema


def write_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
