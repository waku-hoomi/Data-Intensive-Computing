"""Task 4: build the same training dataset two ways and compare them.

Approach A reimplements the whole preparation path straight from the raw course
files (parquet + CSVs): read, cast, fix timezones, map counties to boroughs,
aggregate, join. Approach B calls the Week 1-3 platform via
`dataset.build_training_dataset`.

Both paths then hand their table to the *same* `train_and_evaluate`, so the
comparison isolates the data-preparation stage. What is measured is wall-clock
time to produce the dataset (and the rows that result), plus the validation/test
error each path ultimately reaches. The point is not that B is faster in
wall-clock on a laptop -- A skips validation entirely -- but that A is
re-derived code that will drift from the platform, while B is a thin query over
already-validated tables.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F

from urban_platform.ml import dataset as dataset_module

ROOT = Path(__file__).resolve().parents[3]
ARTIFACTS = ROOT / "artifacts/week4"

# Raw column names as they appear in the course files, plus the same constants
# the platform declared in config/datasets.yaml, duplicated here on purpose so
# Approach A is genuinely self-contained (that duplication is itself one of the
# findings the report discusses).
_EPA_COUNTY_TO_BOROUGH = {
    "Bronx": "Bronx",
    "Kings": "Brooklyn",
    "Queens": "Queens",
    "New York": "Manhattan",
    "Richmond": "Staten Island",
}


def build_dataset_from_raw(spark: SparkSession, platform_cfg, cfg: dict | None = None) -> DataFrame:
    """Approach A: replicate the preparation from raw files, no platform."""
    cfg = cfg or dataset_module.ml_config()
    raw = Path(platform_cfg.raw_data_dir)

    trips = (
        spark.read.parquet(
            str(raw / "yellow_tripdata_2024-01.parquet"),
            str(raw / "yellow_tripdata_2024-02.parquet"),
            str(raw / "yellow_tripdata_2024-03.parquet"),
        )
        .select(F.col("tpep_pickup_datetime"), F.col("PULocationID").try_cast("int").alias("pu_location_id"))
    )

    # The platform standardises on UTC at ingestion; Approach A has to repeat
    # that here, because the raw parquet is naive local NYC wall-clock time.
    trips = (
        trips.withColumn("pickup_ts", F.to_utc_timestamp(F.col("tpep_pickup_datetime"), "America/New_York"))
        .withColumn("pickup_hour", F.date_trunc("hour", F.col("pickup_ts")))
        .withColumn("local_hour", F.from_utc_timestamp(F.col("pickup_hour"), "America/New_York"))
    )

    # Raw CSV columns are read as strings (inferSchema off) to avoid the
    # documented mis-inference of air-quality "Time GMT"; every cast is explicit.
    zones = (
        spark.read.option("header", True).csv(str(raw / "taxi_zone_lookup.csv"))
        .select(F.col("LocationID").try_cast("int").alias("pu_location_id"),
                F.col("Zone").alias("pickup_zone"),
                F.col("Borough").alias("pickup_borough"))
    )

    weather = (
        spark.read.option("header", True).csv(str(raw / "weather.csv"))
        .withColumn("pickup_hour",
                    F.try_make_timestamp(F.col("year").try_cast("int"), F.col("month").try_cast("int"),
                                         F.col("day").try_cast("int"), F.col("hour").try_cast("int"),
                                         F.lit(0), F.lit(0.0)))
        .select("pickup_hour", F.col("temp").try_cast("double").alias("temp_c"),
                F.col("rhum").try_cast("int").alias("relative_humidity_pct"),
                F.col("prcp").try_cast("double").alias("precipitation_mm"),
                F.col("wspd").try_cast("double").alias("wind_speed_kmh"),
                F.col("coco").try_cast("int").alias("weather_condition_code"))
    )

    air = (
        spark.read.option("header", True).csv(str(raw / "air_quality/hourly_88101_2024.csv"))
        .filter(F.col("State Name") == "New York")
        .filter(F.col("Parameter Name") == "PM2.5 - Local Conditions")
    )
    borough_map = F.create_map(*[F.lit(v) for pair in _EPA_COUNTY_TO_BOROUGH.items() for v in pair])
    air = air.withColumn("pickup_borough", borough_map[F.col("County Name")]).filter(
        F.col("pickup_borough").isNotNull()
    )
    air = air.withColumn(
        "pickup_hour",
        F.try_to_timestamp(F.concat_ws(" ", F.col("Date GMT"), F.col("Time GMT")),
                           F.lit("yyyy-MM-dd HH:mm")),
    ).select("pickup_borough", "pickup_hour", F.col("Sample Measurement").try_cast("double").alias("pm25_ug_m3"))

    zone_hours = (
        zones.select("pu_location_id", "pickup_zone", "pickup_borough").distinct()
        .crossJoin(trips.select("pickup_hour", "local_hour").distinct())
    )
    counts = trips.groupBy("pu_location_id", "pickup_hour").agg(F.count("*").alias("trip_count"))
    zone_hours = zone_hours.join(counts, on=["pu_location_id", "pickup_hour"], how="left").fillna({"trip_count": 0})

    joined = (
        zone_hours
        .join(weather, on="pickup_hour", how="left")
        .join(air.groupBy("pickup_borough", "pickup_hour").agg(F.max("pm25_ug_m3").alias("pm25_ug_m3")),
              on=["pickup_borough", "pickup_hour"], how="left")
    )
    joined = joined.withColumn("humidity_pct_v2", F.lit(None).cast("double")) \
                   .withColumn("aqi_max", F.lit(None).cast("int")) \
                   .withColumn("aq_station_count", F.lit(None).cast("long")) \
                   .withColumn("weather_condition_code", F.lit(None).cast("int"))
    return dataset_module.add_split(joined, cfg)


def summarize(df: DataFrame, label_col: str) -> dict:
    aggregations = df.agg(
        F.count("*").alias("rows"),
        F.sum(label_col).alias("total_demand"),
    ).first()
    return {
        "rows": aggregations["rows"],
        "total_demand": int(aggregations["total_demand"] or 0),
        "rows_by_split": {row["split"]: row["count"] for row in df.groupBy("split").count().collect()},
    }


def compare_approaches(spark: SparkSession, platform_cfg, cfg: dict | None = None) -> dict:
    """Time both dataset builds, then train the identical pipeline on each."""
    from urban_platform.ml.train import train_and_evaluate

    cfg = cfg or dataset_module.ml_config()
    results: dict = {"label": cfg["label_col"], "approaches": {}}

    started = time.perf_counter()
    approach_a = build_dataset_from_raw(spark, platform_cfg, cfg)
    a_seconds = time.perf_counter() - started
    a_summary = summarize(approach_a, cfg["label_col"])

    started = time.perf_counter()
    approach_b = dataset_module.build_training_dataset(spark, platform_cfg, cfg)
    b_seconds = time.perf_counter() - started
    b_summary = summarize(approach_b, cfg["label_col"])

    results["approaches"]["raw"] = {
        "dataset_seconds": a_seconds,
        "dataset": a_summary,
        "model": train_and_evaluate(spark, platform_cfg, cfg, run_id="week4-approach-a", training_df=approach_a),
    }
    results["approaches"]["platform"] = {
        "dataset_seconds": b_seconds,
        "dataset": b_summary,
        "model": train_and_evaluate(spark, platform_cfg, cfg, run_id="week4-approach-b", training_df=approach_b),
    }

    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    (ARTIFACTS / "comparison.json").write_text(json.dumps(results, indent=2, default=str), encoding="utf-8")
    return results
