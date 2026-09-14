"""Materialize four small analytical Delta products plus an append-only registry."""
import datetime as dt
import json
import time
from pathlib import Path
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from urban_platform.analytics.context import (
    ROOT, analytics_config, register_analysis_views, require_weather_confirmation, sql_text,
)

PRODUCTS = ["daily_mobility_summary", "taxi_zone_statistics", "weather_impact_summary", "air_quality_impact_summary"]


def product_path(name):
    if name not in PRODUCTS:
        raise ValueError(name)
    return ROOT / "data/products" / name


def build_products(spark, platform_cfg, names=None):
    names = names or PRODUCTS
    if "weather_impact_summary" in names:
        require_weather_confirmation()
    source = str(Path(platform_cfg.gold_dir) / "integrated_taxi_trips")
    version = int(DeltaTable.forPath(spark, source).history(1).first().version)
    integrated = spark.read.format("delta").option("versionAsOf", version).load(source)
    cfg = analytics_config()["analysis"]
    register_analysis_views(spark, integrated, cfg)
    registry_path = str(ROOT / "data/lab2_metadata/product_registry")
    results = []
    for name in names:
        refreshed = dt.datetime.now(dt.timezone.utc).isoformat()
        created = refreshed
        if DeltaTable.isDeltaTable(spark, registry_path):
            prior = (spark.read.format("delta").load(registry_path).filter(F.col("product_name") == name)
                     .agg(F.min("created_at_utc").alias("created")).first().created)
            created = prior or refreshed
        start = time.perf_counter()
        output = product_path(name)
        df = spark.sql(sql_text(name, "products"))
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(output))
        elapsed = time.perf_counter() - start
        detail = DeltaTable.forPath(spark, str(output)).detail().first()
        record = {
            "product_name": name, "data_source": "data/gold/integrated_taxi_trips",
            "source_delta_version": version, "created_at_utc": created,
            "refreshed_at_utc": refreshed, "schema_version": int(cfg["schema_version"]),
            "schema_json": df.schema.json(), "analysis_config_json": json.dumps(cfg, sort_keys=True),
            "storage_bytes": int(detail.sizeInBytes), "active_files": int(detail.numFiles),
            "row_count": spark.read.format("delta").load(str(output)).count(),
            "build_seconds": elapsed,
        }
        spark.createDataFrame([record]).write.format("delta").mode("append").save(registry_path)
        results.append(record)
    return results
