"""Materialize four small analytical Delta products plus an append-only registry."""
import datetime as dt
import json
import time
from pathlib import Path
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType
from urban_platform.analytics.context import (
    ROOT, analytics_config, effective_analysis_config, register_analysis_views,
    require_weather_confirmation, sql_text,
)
from urban_platform.monitoring.events import jvm_single_row

PRODUCTS = ["daily_mobility_summary", "taxi_zone_statistics", "weather_impact_summary", "air_quality_impact_summary"]

PRODUCT_REGISTRY_SCHEMA = StructType([
    StructField("product_name", StringType(), False),
    StructField("data_source", StringType(), False),
    StructField("source_delta_version", LongType(), False),
    StructField("created_at_utc", StringType(), False),
    StructField("refreshed_at_utc", StringType(), False),
    StructField("schema_version", LongType(), False),
    StructField("schema_json", StringType(), False),
    StructField("analysis_config_json", StringType(), False),
    StructField("storage_bytes", LongType(), False),
    StructField("active_files", LongType(), False),
    StructField("row_count", LongType(), False),
    StructField("build_seconds", DoubleType(), False),
])


def product_path(name):
    if name not in PRODUCTS:
        raise ValueError(name)
    return ROOT / "data/products" / name


def record_product_refresh(spark, name, source, version, cfg, elapsed):
    """Append one registry event only after a product write has succeeded."""
    registry_path = str(ROOT / "data/lab2_metadata/product_registry")
    refreshed = dt.datetime.now(dt.timezone.utc).isoformat()
    created = refreshed
    if DeltaTable.isDeltaTable(spark, registry_path):
        prior = (spark.read.format("delta").load(registry_path)
                 .filter(F.col("product_name") == name)
                 .agg(F.min("created_at_utc").alias("created")).first().created)
        created = prior or refreshed
    output = product_path(name)
    df = spark.read.format("delta").load(str(output))
    detail = DeltaTable.forPath(spark, str(output)).detail().first()
    record = {
        "product_name": name, "data_source": str(Path(source).resolve()),
        "source_delta_version": int(version), "created_at_utc": created,
        "refreshed_at_utc": refreshed, "schema_version": int(cfg["schema_version"]),
        "schema_json": df.schema.json(), "analysis_config_json": json.dumps(cfg, sort_keys=True),
        "storage_bytes": int(detail.sizeInBytes), "active_files": int(detail.numFiles),
        "row_count": df.count(), "build_seconds": elapsed,
    }
    jvm_single_row(spark, record, PRODUCT_REGISTRY_SCHEMA).write.format("delta").mode("append").save(
        registry_path
    )
    return record


def build_products(spark, platform_cfg, names=None):
    names = names or PRODUCTS
    if "weather_impact_summary" in names:
        require_weather_confirmation()
    source = str(Path(platform_cfg.gold_dir) / "integrated_taxi_trips")
    version = int(DeltaTable.forPath(spark, source).history(1).first().version)
    integrated = spark.read.format("delta").option("versionAsOf", version).load(source)
    cfg = effective_analysis_config(integrated, analytics_config()["analysis"])
    register_analysis_views(spark, integrated, cfg)
    results = []
    for name in names:
        start = time.perf_counter()
        output = product_path(name)
        df = spark.sql(sql_text(name, "products"))
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(output))
        elapsed = time.perf_counter() - start
        results.append(record_product_refresh(spark, name, source, version, cfg, elapsed))
    return results
