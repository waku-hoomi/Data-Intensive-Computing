
"""Task 5: Integration pipeline.

Builds `integrated_taxi_trips`: every taxi trip enriched with pickup-time
weather, pickup-time air quality, and pickup/dropoff zone + borough.

Design decisions (see docs/DESIGN_REPORT.md for full justification):

  - Weather is a single citywide hourly series (no location dimension in
    the source data), so it is joined purely on the pickup hour:
    date_trunc('hour', pickup_ts) == weather.observation_ts. This is an
    "align to the enclosing hour" join, not nearest-neighbour in either
    direction, chosen because it is simple, deterministic, and matches how
    the observation itself is defined (an hourly reading, valid for the
    hour it timestamps).

  - Air quality (PM2.5) is per-borough, not citywide -- the raw stations
    only cover Bronx, Brooklyn (Kings) and Queens. Multiple stations can
    exist in one borough for the same hour, so we first aggregate to
    (borough, hour) by averaging PM2.5 across stations, then join taxi
    trips on (pickup_borough, pickup_hour). Manhattan, Staten Island and
    EWR pickups get a NULL air-quality reading -- a real coverage gap in
    the source data, not a bug in the join.

  - Both weather and air-quality joins use the PICKUP time/location only
    (not dropoff) since Task 5 asks specifically for "weather conditions
    at the pickup time" / "air-quality measurements at the pickup time".

  - Missing observations (no weather row for that hour, no AQ station in
    that borough) are left as SQL NULL rather than imputed -- imputation
    would silently fabricate environmental readings, which is worse for
    an analytical dataset than an honest NULL that downstream queries can
    choose to filter or impute themselves.
"""
import os

from pyspark.sql import DataFrame, SparkSession, functions as F

from urban_platform.utils.config import PlatformConfig


def _read_bronze(spark: SparkSession, platform_cfg: PlatformConfig, name: str) -> DataFrame:
    return spark.read.format("delta").load(os.path.join(platform_cfg.bronze_dir, name))


def build_integrated_taxi_trips(spark: SparkSession, platform_cfg: PlatformConfig) -> DataFrame:
    trips = _read_bronze(spark, platform_cfg, "taxi_trips")
    weather = _read_bronze(spark, platform_cfg, "weather")
    air_quality = _read_bronze(spark, platform_cfg, "air_quality")
    zones = _read_bronze(spark, platform_cfg, "taxi_zone_lookup")

    trips = trips.withColumn("pickup_hour", F.date_trunc("hour", F.col("pickup_ts")))

    pu_zones = zones.select(
        F.col("location_id").alias("pu_location_id"),
        F.col("zone").alias("pickup_zone"),
        F.col("borough").alias("pickup_borough"),
    )
    do_zones = zones.select(
        F.col("location_id").alias("do_location_id"),
        F.col("zone").alias("dropoff_zone"),
        F.col("borough").alias("dropoff_borough"),
    )

    enriched = (
        trips.join(pu_zones, on="pu_location_id", how="left")
        .join(do_zones, on="do_location_id", how="left")
    )

    weather_hourly = weather.select(
        F.col("observation_ts").alias("pickup_hour"),
        F.col("temp_c"),
        F.col("relative_humidity_pct"),
        F.col("precipitation_mm"),
        F.col("wind_speed_kmh"),
        F.col("condition_code").alias("weather_condition_code"),
    )
    enriched = enriched.join(weather_hourly, on="pickup_hour", how="left")

    aq_by_borough_hour = (
        air_quality.groupBy(
            F.col("county_name").alias("pickup_borough"),
            F.col("observation_ts").alias("pickup_hour"),
        )
        .agg(F.avg("pm25_ug_m3").alias("pm25_ug_m3"), F.count("*").alias("aq_station_count"))
    )
    # EPA county_name "Kings" corresponds to the taxi-zone borough "Brooklyn".
    aq_by_borough_hour = aq_by_borough_hour.withColumn(
        "pickup_borough",
        F.when(F.col("pickup_borough") == "Kings", F.lit("Brooklyn")).otherwise(F.col("pickup_borough")),
    )

    enriched = enriched.join(aq_by_borough_hour, on=["pickup_borough", "pickup_hour"], how="left")

    return enriched.select(
        "trip_id",
        "vendor_id",
        "pickup_ts",
        "dropoff_ts",
        "passenger_count",
        "trip_distance",
        "rate_code_id",
        "pu_location_id",
        "pickup_zone",
        "pickup_borough",
        "do_location_id",
        "dropoff_zone",
        "dropoff_borough",
        "payment_type",
        "fare_amount",
        "total_amount",
        "temp_c",
        "relative_humidity_pct",
        "precipitation_mm",
        "wind_speed_kmh",
        "weather_condition_code",
        "pm25_ug_m3",
        "aq_station_count",
        "pickup_year",
        "pickup_month",
    )


def write_integrated_taxi_trips(spark: SparkSession, platform_cfg: PlatformConfig) -> str:
    df = build_integrated_taxi_trips(spark, platform_cfg)
    output_path = os.path.join(platform_cfg.gold_dir, "integrated_taxi_trips")
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("pickup_year", "pickup_month")
        .save(output_path)
    )
    return output_path
