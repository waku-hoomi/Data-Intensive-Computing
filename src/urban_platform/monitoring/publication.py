"""Publish a cross-table version manifest after all stages have succeeded."""
from __future__ import annotations

import json
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType

from urban_platform.analytics.products import PRODUCTS, product_path
from urban_platform.incremental.gold import current_version, gold_path
from urban_platform.monitoring.events import jvm_single_row, utc_now
from urban_platform.utils.config import PlatformConfig


PUBLICATION_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("published_at_utc", StringType(), False),
    StructField("gold_version", LongType(), False),
    StructField("product_versions_json", StringType(), False),
    StructField("bronze_versions_json", StringType(), False),
    StructField("analysis_config_json", StringType(), False),
])


def publication_path(cfg: PlatformConfig) -> str:
    return str(Path(cfg.metadata_dir).parent / "monitoring" / "published_snapshots")


def latest_publication(spark: SparkSession, cfg: PlatformConfig) -> dict | None:
    path = publication_path(cfg)
    if not DeltaTable.isDeltaTable(spark, path):
        return None
    row = (spark.read.format("delta").load(path)
           .orderBy("published_at_utc", ascending=False).first())
    if row is None:
        return None
    result = row.asDict()
    result["product_versions"] = json.loads(result["product_versions_json"])
    result["bronze_versions"] = json.loads(result["bronze_versions_json"])
    result["analysis_config"] = json.loads(result["analysis_config_json"])
    return result


def publish_snapshot(
    spark: SparkSession, cfg: PlatformConfig, run_id: str, analysis_config: dict
) -> dict:
    """Advance the reader-visible pointer only after every table is complete."""
    products = {name: current_version(spark, str(product_path(name))) for name in PRODUCTS}
    bronze = {ds.name: current_version(spark, str(Path(cfg.bronze_dir) / ds.name))
              for ds in cfg.datasets}
    record = {
        "run_id": run_id, "published_at_utc": utc_now(),
        "gold_version": current_version(spark, gold_path(cfg)),
        "product_versions_json": json.dumps(products, sort_keys=True),
        "bronze_versions_json": json.dumps(bronze, sort_keys=True),
        "analysis_config_json": json.dumps(analysis_config, sort_keys=True),
    }
    jvm_single_row(spark, record, PUBLICATION_SCHEMA).write.format("delta").mode("append").save(
        publication_path(cfg)
    )
    return {**record, "product_versions": products, "bronze_versions": bronze,
            "analysis_config": analysis_config}
