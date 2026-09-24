"""Append one durable Delta event for each pipeline stage and dataset."""
from __future__ import annotations

import datetime as dt
import json
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import (
    DoubleType, LongType, StringType, StructField, StructType,
)

from urban_platform.utils.config import PlatformConfig


RUN_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("dataset_name", StringType(), True),
    StructField("stage", StringType(), False),
    StructField("status", StringType(), False),
    StructField("started_at_utc", StringType(), False),
    StructField("ended_at_utc", StringType(), False),
    StructField("duration_seconds", DoubleType(), False),
    StructField("processed", LongType(), False),
    StructField("inserted", LongType(), False),
    StructField("updated", LongType(), False),
    StructField("duplicates", LongType(), False),
    StructField("rejected", LongType(), False),
    StructField("schema_version", LongType(), False),
    StructField("validation_failures", LongType(), False),
    StructField("before_version", LongType(), True),
    StructField("after_version", LongType(), True),
    StructField("error", StringType(), True),
    StructField("details_json", StringType(), True),
])


def jvm_single_row(
    spark: SparkSession, record: dict, schema: StructType,
) -> DataFrame:
    """Build a one-row DataFrame without starting a Python worker.

    Windows local Spark can intermittently time out while spawning a fresh
    Python worker late in a long run. Monitoring and manifest rows contain
    only driver-side scalar values, so JVM literals are both cheaper and more
    reliable than ``createDataFrame([record])``.
    """
    return spark.range(1).select(*[
        F.lit(record.get(field.name)).cast(field.dataType).alias(field.name)
        for field in schema
    ])


def run_table_path(cfg: PlatformConfig) -> str:
    return str(Path(cfg.metadata_dir).parent / "monitoring" / "pipeline_runs")


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def append_run_event(
    spark: SparkSession, cfg: PlatformConfig, run_id: str, stage: str,
    started_at_utc: str, duration_seconds: float, *, dataset_name: str | None = None,
    status: str = "success", metrics: dict | None = None, error: str | None = None,
) -> float:
    """Return the Delta monitoring write time so its overhead is measurable."""
    metrics = metrics or {}
    failures = metrics.get("validation_failures", 0)
    if isinstance(failures, dict):
        failure_count = sum(int(value) for value in failures.values())
    elif isinstance(failures, list):
        failure_count = len(failures)
    else:
        failure_count = int(failures or 0)
    record = {
        "run_id": run_id, "dataset_name": dataset_name, "stage": stage,
        "status": status, "started_at_utc": started_at_utc,
        "ended_at_utc": utc_now(), "duration_seconds": float(duration_seconds),
        "processed": int(metrics.get("processed", 0) or 0),
        "inserted": int(metrics.get("inserted", 0) or 0),
        "updated": int(metrics.get("updated", 0) or 0),
        "duplicates": int(metrics.get("duplicates", 0) or 0),
        "rejected": int(metrics.get("rejected", 0) or 0),
        "schema_version": int(metrics.get("schema_version", cfg.schema_version) or cfg.schema_version),
        "validation_failures": failure_count,
        "before_version": metrics.get("before_version"),
        "after_version": metrics.get("after_version"),
        "error": error or metrics.get("error"),
        "details_json": json.dumps(metrics, sort_keys=True, default=str),
    }
    start = time.perf_counter()
    jvm_single_row(spark, record, RUN_SCHEMA).write.format("delta").mode("append").save(
        run_table_path(cfg)
    )
    return time.perf_counter() - start
