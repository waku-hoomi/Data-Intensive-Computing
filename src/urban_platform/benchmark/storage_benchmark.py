"""Task 6: Benchmark two storage strategies for the taxi_trips dataset.

Strategy A ("by_month"): partitioned by (pickup_year, pickup_month) -- the
same scheme used for the platform's bronze/gold tables. Low cardinality
(3 partitions for our 3-month sample), matches how the fact table grows
over time (Task 2: new files arrive month by month).

Strategy B ("by_borough"): partitioned by pickup_borough -- a business
dimension instead of a temporal one. Cardinality is similar (a handful of
boroughs) but the join key used by two of the three benchmark queries
(borough-level aggregates) aligns with the partition key, so this tests
whether partitioning on the query's own GROUP BY column pays off compared
to partitioning on ingestion time.

Both strategies read from bronze/taxi_trips (already cleaned) so the
benchmark isolates the effect of storage layout, not of re-running
validation logic.
"""
import glob
import os
import time
from dataclasses import asdict, dataclass
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession, functions as F

from urban_platform.utils.config import PlatformConfig


@dataclass
class StorageBenchmarkResult:
    strategy: str
    partition_by: List[str]
    ingestion_seconds: float
    storage_bytes: int
    file_count: int
    query_latencies_seconds: Dict[str, float]


def _dir_size_and_file_count(path: str) -> "tuple[int, int]":
    total_size = 0
    file_count = 0
    for root, _, files in os.walk(path):
        for fname in files:
            if fname.startswith("_") or fname.startswith("."):
                continue  # skip Delta log / checkpoint bookkeeping files
            fpath = os.path.join(root, fname)
            if fname.endswith(".parquet"):
                total_size += os.path.getsize(fpath)
                file_count += 1
    return total_size, file_count


def _write_strategy(source_df: DataFrame, output_path: str, partition_by: List[str]) -> float:
    start = time.time()
    writer = source_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(output_path)
    return time.time() - start


def _run_queries(spark: SparkSession, table_path: str) -> Dict[str, float]:
    """The three queries Task 6 requires, run against a given table layout."""
    df = spark.read.format("delta").load(table_path)
    latencies = {}

    start = time.time()
    df.groupBy("pickup_borough").count().collect()
    latencies["trips_per_borough"] = time.time() - start

    start = time.time()
    (
        df.withColumn("trip_date", F.to_date("pickup_ts"))
        .withColumn(
            "duration_min",
            (F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")) / 60.0,
        )
        .groupBy("trip_date")
        .agg(F.avg("duration_min").alias("avg_duration_min"))
        .collect()
    )
    latencies["avg_duration_per_day"] = time.time() - start

    start = time.time()
    df.groupBy("pickup_borough").agg(F.avg("fare_amount").alias("avg_fare")).collect()
    latencies["avg_fare_per_borough"] = time.time() - start

    return latencies


def benchmark_strategy(
    spark: SparkSession, platform_cfg: PlatformConfig, strategy_name: str, partition_by: List[str]
) -> StorageBenchmarkResult:
    # Source: bronze taxi_trips enriched with pickup_borough, since the
    # borough-based queries and the by_borough partitioning both need it,
    # and the raw bronze table only has location IDs.
    trips = spark.read.format("delta").load(os.path.join(platform_cfg.bronze_dir, "taxi_trips"))
    zones = spark.read.format("delta").load(os.path.join(platform_cfg.bronze_dir, "taxi_zone_lookup"))
    pu_zones = zones.select(
        F.col("location_id").alias("pu_location_id"), F.col("borough").alias("pickup_borough")
    )
    trips_with_borough = trips.join(pu_zones, on="pu_location_id", how="left")

    output_path = os.path.join(platform_cfg.metadata_dir, "benchmark", f"taxi_trips_{strategy_name}")
    ingestion_seconds = _write_strategy(trips_with_borough, output_path, partition_by)

    storage_bytes, file_count = _dir_size_and_file_count(output_path)
    query_latencies = _run_queries(spark, output_path)

    return StorageBenchmarkResult(
        strategy=strategy_name,
        partition_by=partition_by,
        ingestion_seconds=round(ingestion_seconds, 3),
        storage_bytes=storage_bytes,
        file_count=file_count,
        query_latencies_seconds={k: round(v, 3) for k, v in query_latencies.items()},
    )