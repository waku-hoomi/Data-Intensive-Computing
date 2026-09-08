"""Ingestion metadata log: one JSON-lines file per run, appended to.

Task 3 asks for "ingestion metadata such as the number of processed
records, rejected records, execution time, and schema version." We keep
this as its own module (rather than inline print statements) so Week 3's
incremental-update story can later read this same log to decide what
changed between runs.
"""
import json
import os
import time
from dataclasses import asdict, dataclass, field
from typing import Dict, Optional


@dataclass
class IngestionRunMetadata:
    dataset_name: str
    run_started_at: str
    run_ended_at: str
    execution_seconds: float
    schema_version: int
    input_rows: int
    output_rows: int
    rejected_rows: int
    rejected_by_reason: Dict[str, int]
    output_path: str
    status: str
    error: Optional[str] = None


def write_metadata(metadata_dir: str, record: IngestionRunMetadata) -> str:
    os.makedirs(metadata_dir, exist_ok=True)
    log_path = os.path.join(metadata_dir, "ingestion_log.jsonl")
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(asdict(record)) + "\n")
    return log_path


class Timer:
    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *exc):
        self.elapsed = time.time() - self.start