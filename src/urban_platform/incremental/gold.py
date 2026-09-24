"""Re-enrich only trips affected by a committed Bronze change set."""
from __future__ import annotations

from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, functions as F

from urban_platform.integration.integrate import build_integrated_taxi_trips
from urban_platform.utils.config import PlatformConfig


NYC_COUNTIES = {
    "005": "Bronx", "047": "Brooklyn", "061": "Manhattan",
    "081": "Queens", "085": "Staten Island",
}


def gold_path(cfg: PlatformConfig) -> str:
    return str(Path(cfg.gold_dir) / "integrated_taxi_trips")


def current_version(spark: SparkSession, path: str) -> int:
    return int(DeltaTable.forPath(spark, path).history(1).first().version)


def enable_gold_change_feed(spark: SparkSession, cfg: PlatformConfig) -> None:
    """Enable CDF before the first Week 3 update, including on an old Gold table."""
    path = gold_path(cfg).replace("\\", "/")
    spark.sql(
        f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
    )


def change_feed_enabled(spark: SparkSession, path: str) -> bool:
    """Read the CDF table property without relying on runtime-specific DETAIL fields."""
    sql_path = str(path).replace("\\", "/")
    row = spark.sql(
        f"SHOW TBLPROPERTIES delta.`{sql_path}` ('delta.enableChangeDataFeed')"
    ).first()
    return row is not None and str(row["value"]).lower() == "true"


def _has_rows(df: DataFrame | None) -> bool:
    return df is not None and bool(df.limit(1).count())


def affected_trip_ids(
    spark: SparkSession, cfg: PlatformConfig, changed_keys: dict[str, DataFrame | None]
) -> DataFrame:
    """Use source keys to find trips whose context or own row may have changed."""
    trips = spark.read.format("delta").load(str(Path(cfg.bronze_dir) / "taxi_trips"))
    result = trips.select("trip_id").limit(0)
    direct = changed_keys.get("taxi_trips")
    if _has_rows(direct):
        result = result.unionByName(direct.select("trip_id"))

    weather = changed_keys.get("weather")
    if _has_rows(weather):
        hours = weather.select(F.col("observation_ts").alias("changed_hour")).distinct()
        matching = (trips.withColumn("changed_hour", F.date_trunc("hour", "pickup_ts"))
                    .join(F.broadcast(hours), "changed_hour", "left_semi")
                    .select("trip_id"))
        result = result.unionByName(matching)

    zones_changed = changed_keys.get("taxi_zone_lookup")
    if _has_rows(zones_changed):
        zone_ids = zones_changed.select("location_id").distinct()
        pu = trips.join(F.broadcast(zone_ids), trips.pu_location_id == zone_ids.location_id,
                        "left_semi").select("trip_id")
        do = trips.join(F.broadcast(zone_ids), trips.do_location_id == zone_ids.location_id,
                        "left_semi").select("trip_id")
        result = result.unionByName(pu).unionByName(do)

    air = changed_keys.get("air_quality")
    if _has_rows(air):
        county_map = F.create_map(*[
            F.lit(value) for pair in NYC_COUNTIES.items() for value in pair
        ])
        aq_hours = (air.filter(F.col("station_id").startswith("36-"))
                    .select(F.col("observation_ts").alias("pickup_hour"),
                            county_map[F.substring("station_id", 4, 3)].alias("pickup_borough"))
                    .filter(F.col("pickup_borough").isNotNull()).distinct())
        zones = (spark.read.format("delta")
                 .load(str(Path(cfg.bronze_dir) / "taxi_zone_lookup"))
                 .select(F.col("location_id").alias("pu_location_id"),
                         F.col("borough").alias("pickup_borough")))
        matching = (trips.withColumn("pickup_hour", F.date_trunc("hour", "pickup_ts"))
                    .join(F.broadcast(zones), "pu_location_id", "left")
                    .join(F.broadcast(aq_hours), ["pickup_borough", "pickup_hour"], "left_semi")
                    .select("trip_id"))
        result = result.unionByName(matching)
    return result.distinct()


def refresh_integrated_gold(
    spark: SparkSession, cfg: PlatformConfig, changed_keys: dict[str, DataFrame | None]
) -> dict:
    """Upsert newly enriched rows, preserving all unaffected Gold records."""
    path = gold_path(cfg)
    before = current_version(spark, path)
    ids = affected_trip_ids(spark, cfg, changed_keys)
    if not _has_rows(ids):
        return {"before_version": before, "after_version": before,
                "affected_trips": 0, "changed": False}
    trips = spark.read.format("delta").load(str(Path(cfg.bronze_dir) / "taxi_trips"))
    selected = trips.join(ids, "trip_id", "inner")
    selected = selected.persist()
    count = selected.count()
    try:
        enriched = build_integrated_taxi_trips(spark, cfg, selected)
        (DeltaTable.forPath(spark, path).alias("target")
         .merge(enriched.alias("source"),
                "target.trip_id = source.trip_id AND "
                "target.pickup_year = source.pickup_year AND "
                "target.pickup_month = source.pickup_month")
         .withSchemaEvolution()
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())
    finally:
        selected.unpersist()
    after = current_version(spark, path)
    return {"before_version": before, "after_version": after,
            "affected_trips": count, "changed": after > before}


def gold_changes(spark: SparkSession, cfg: PlatformConfig, before: int, after: int) -> DataFrame | None:
    if after <= before:
        return None
    return (spark.read.format("delta").option("readChangeFeed", "true")
            .option("startingVersion", before + 1).option("endingVersion", after)
            .load(gold_path(cfg)))
