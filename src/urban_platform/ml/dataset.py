"""Task 1: build the hourly taxi-demand training dataset from the platform.

The dataset is a zone x hour grid (one row per pickup zone per clock hour,
including hours with zero pickups) assembled from the Gold table the platform
already produced. This module deliberately owns nothing that Weeks 1-3 already
own: ingestion, validation, timezone normalisation and the environmental joins
all happen upstream in `urban_platform.integration` and
`urban_platform.analytics.context`. Here we only turn the integrated view into
a labelled, split, model-ready table.

Reading Gold goes through `read_integrated`, so the dataset is always built
from the same published version that the analytical queries and products use,
and an in-flight Week 3 update cannot expose a half-written Gold state.
"""
from __future__ import annotations

from pathlib import Path

import yaml
from pyspark.sql import DataFrame, SparkSession, functions as F

from urban_platform.analytics.context import register_analysis_views, sql_text
from urban_platform.utils.config import PlatformConfig

ROOT = Path(__file__).resolve().parents[3]


def ml_config() -> dict:
    return yaml.safe_load((ROOT / "config/ml.yaml").read_text())["ml"]


def dataset_path() -> Path:
    return ROOT / "data/ml/training_dataset"


def add_split(df: DataFrame, cfg: dict) -> DataFrame:
    """Label each row by the chronological split its pickup hour falls into.

    Cutoffs come from config, not from the data's own range, so rerunning on a
    longer table extends the test window forward instead of quietly moving it.
    """
    boundaries = cfg["split_boundaries"]
    validation_start = F.to_timestamp(F.lit(boundaries["validation_start"] + " 00:00:00"))
    test_start = F.to_timestamp(F.lit(boundaries["test_start"] + " 00:00:00"))
    return df.withColumn(
        "split",
        F.when(F.col("pickup_hour") >= test_start, F.lit("test"))
        .when(F.col("pickup_hour") >= validation_start, F.lit("validation"))
        .otherwise(F.lit("train")),
    )


def build_training_dataset(
    spark: SparkSession, platform_cfg: PlatformConfig, cfg: dict | None = None
) -> DataFrame:
    from urban_platform.analytics.context import read_integrated

    cfg = cfg or ml_config()
    integrated = read_integrated(spark, platform_cfg)
    register_analysis_views(spark, integrated)
    rows = spark.sql(sql_text("zone_hour_demand", "ml"))
    return add_split(rows, cfg)


def persist_training_dataset(
    spark: SparkSession, platform_cfg: PlatformConfig, cfg: dict | None = None
) -> dict:
    cfg = cfg or ml_config()
    output = dataset_path()
    df = build_training_dataset(spark, platform_cfg, cfg)
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("split")
        .save(str(output))
    )
    counts = {row["split"]: row["count"] for row in df.groupBy("split").count().collect()}
    total = df.count()
    zero_hours = df.filter(F.col("trip_count") == 0).count()
    aq_missing = df.filter(F.col("pm25_ug_m3").isNull()).count()
    return {
        "path": str(output),
        "rows": total,
        "rows_by_split": counts,
        "zero_demand_hours": zero_hours,
        "zero_demand_share": (zero_hours / total) if total else 0.0,
        "air_quality_missing_rows": aq_missing,
        "air_quality_missing_share": (aq_missing / total) if total else 0.0,
    }
