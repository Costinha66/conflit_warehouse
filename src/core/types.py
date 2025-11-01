from dataclasses import dataclass, asdict, field
from typing import List, Optional, Dict, Any
from datetime import datetime
import json
from pathlib import Path
from src.core.miscelannious import iso


@dataclass
class DatasetRef:
    layer: str
    entity: str
    grain: Optional[str] = None
    partition_key: Optional[str] = None


@dataclass
class RunRef:
    run_id: str
    transform_version: str
    started_at: datetime
    finished_at: Optional[datetime] = None


@dataclass
class InputRef:
    dataset: DatasetRef
    file_path: Optional[str] = None
    content_hash: Optional[str] = None
    route_id: Optional[str] = None


@dataclass
class CoreLineageEvent:
    event_id: str
    event_schema_version: int
    event_type: str
    dataset: DatasetRef
    run: RunRef
    inputs: List[InputRef] = field(default_factory=list)
    outputs: List[DatasetRef] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)
    dq: Dict[str, Any] = field(default_factory=dict)
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> Dict[str, Any]:
        d = asdict(self)
        d["run"]["started_at"] = iso(self.run.started_at)
        if self.run.finished_at:
            d["run"]["finished_at"] = iso(self.run.finished_at)
        return d


@dataclass
class TestResult:
    name: str
    status: str  # "passed" | "failed" | "warn"
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DQReport:
    status: str  # "passed" if all pass, else "failed"
    results: List[TestResult]
    summary: Dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)


@dataclass
class DQSummary:
    raw_rows: int
    harmonized_rows: int
    valid_rows: int
    rejects_harmonize: int
    rejects_canonicalize: int


@dataclass
class CommitEvent:
    run: RunRef
    part: DatasetRef
    commit_entity: str
    row_count: int
    insert_cnt: int
    update_cnt: int
    delete_cnt: int
    partition_hash: str
    inputs: List[Dict[str, Any]]
    dq_summary: DQSummary
    spec_version: str
    finished_at: datetime


@dataclass
class DiscoveryConfig:
    bronze_root: Path
    router_path: Path
    db_path: Path
    max_workers: int = 8
    log_level: str = "INFO"


@dataclass
class RouteRule:
    pattern: str
    entity: str
    grain: str
    source_id: Optional[str] = None
    route_id: Optional[str] = None


@dataclass
class DQResult:
    passed: bool
    level: str = "MINOR"  # "MINOR"|"MAJOR"|"CRITICAL"
    metrics: dict = None  # e.g., {"negatives": 0, "iso_unmapped": 12}


@dataclass(frozen=True)
class SnapshotConfig:
    db_path: Path
    csv_path: Path
    out_root: Path
    source: str = "unhcr"
    mode: str = "cumulative"
    start_year: int = 2020
    cutoff_year: int = 2020
    snapshot_version: Optional[str] = None
    overwrite: bool = True
