"""Refresh only analytical products and grains touched by a Gold commit."""
from __future__ import annotations

import time
from functools import reduce
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, functions as F

from urban_platform.analytics.context import (
    analytics_config, effective_analysis_config, register_analysis_views,
)
from urban_platform.analytics.products import (
    PRODUCTS, build_products, product_path, record_product_refresh,
)
from urban_platform.incremental.gold import current_version, gold_path
from urban_platform.utils.config import PlatformConfig


RELEVANT = {
    "daily_mobility_summary": (
        "pickup_ts", "dropoff_ts", "pickup_borough", "trip_distance", "fare_amount"
    ),
    "taxi_zone_statistics": (
        "pickup_ts", "dropoff_ts", "pu_location_id", "pickup_zone",
        "pickup_borough", "trip_distance", "fare_amount"
    ),
    "weather_impact_summary": (
        "pickup_ts", "pu_location_id", "pickup_zone", "pickup_borough",
        "weather_condition_code", "trip_distance"
    ),
    "air_quality_impact_summary": (
        "pickup_ts", "pickup_borough", "pm25_ug_m3", "aq_station_count", "aqi_max"
    ),
}

GRAINS = {
    "daily_mobility_summary": ("local_date", "pickup_borough"),
    "taxi_zone_statistics": ("local_month", "pu_location_id"),
    "weather_impact_summary": ("pu_location_id", "weather_condition_code"),
    "air_quality_impact_summary": ("pickup_borough", "pickup_hour"),
}


def _join_condition(left: str, right: str, columns: tuple[str, ...]):
    return reduce(
        lambda a, b: a & b,
        (F.col(f"{left}.{name}").eqNullSafe(F.col(f"{right}.{name}")) for name in columns),
    )


def _paired_changes(changes: DataFrame) -> DataFrame:
    # Pair within each commit, not across the whole replay window. Retain every
    # intermediate grain: a partially committed product may contain it even if
    # a later correction restores the originally published value.
    commit_columns = ["_commit_version"] if "_commit_version" in changes.columns else []
    columns = [c for c in changes.columns if not c.startswith("_") and c != "trip_id"]
    before = changes.filter(F.col("_change_type").isin("update_preimage", "delete")).select(
        F.col("trip_id").alias("key"), *commit_columns, F.lit(1).alias("old_exists"),
        *[F.col(c).alias("old_" + c) for c in columns],
    )
    after = changes.filter(F.col("_change_type").isin("update_postimage", "insert")).select(
        F.col("trip_id").alias("key"), *commit_columns, F.lit(1).alias("new_exists"),
        *[F.col(c).alias("new_" + c) for c in columns],
    )
    return before.join(after, ["key", *commit_columns], "full_outer")


def changed_product_names(changes: DataFrame) -> set[str]:
    paired = _paired_changes(changes)
    expressions = []
    for name, columns in RELEVANT.items():
        differences = [~F.col("old_" + c).eqNullSafe(F.col("new_" + c))
                       for c in columns if "old_" + c in paired.columns]
        changed = F.col("old_exists").isNull() | F.col("new_exists").isNull()
        for difference in differences:
            changed = changed | difference
        expressions.append(F.max(changed.cast("int")).alias(name))
    flags = paired.agg(*expressions).first().asDict()
    return {name for name, flag in flags.items() if flag}


def _changed_grains(changes: DataFrame, name: str, tz: str) -> DataFrame:
    rows = changes.filter(F.col("_change_type").isin(
        "insert", "delete", "update_preimage", "update_postimage"))
    rows = (rows.withColumn("local_date", F.to_date(F.from_utc_timestamp("pickup_ts", tz)))
            .withColumn("local_month", F.date_format(F.from_utc_timestamp("pickup_ts", tz), "yyyy-MM"))
            .withColumn("pickup_hour", F.date_trunc("hour", "pickup_ts")))
    return rows.select(*GRAINS[name]).distinct()


def _expanded_air_quality_grains(
    spark: SparkSession, previous_analysis: dict | None, analysis: dict,
) -> DataFrame | None:
    """Return every borough/hour added when the live analysis window advances.

    The air-quality product has a dense borough-by-hour spine, including hours
    with zero trips. Gold CDF contains only changed trips, so it cannot by
    itself identify the zero-trip rows introduced by a later window end.
    """
    if previous_analysis is None:
        return None
    old_end = previous_analysis.get("end_date_exclusive")
    new_end = analysis.get("end_date_exclusive")
    if not old_end or not new_end or old_end == new_end:
        return None
    return (spark.table("borough_hours")
            .filter((F.col("local_hour") >= F.to_timestamp(F.lit(f"{old_end} 00:00:00"))) &
                    (F.col("local_hour") < F.to_timestamp(F.lit(f"{new_end} 00:00:00"))))
            .select(*GRAINS["air_quality_impact_summary"]).distinct())


def _weather_refresh_grains(spark: SparkSession, changes: DataFrame, keys: DataFrame) -> DataFrame:
    """Include zero-trip hours and labels not present in changed fact rows.

    A renamed zone affects every weather group for that zone, including NULL
    context hours. A citywide hourly weather change affects every zone's hour
    denominator, including zones with no trip at that hour. Old product keys
    are retained here so MERGE can delete groups that disappear.
    """
    old = spark.read.format('delta').load(str(product_path('weather_impact_summary')))
    current = spark.table('zone_weather_hours')
    zones = current.select('pu_location_id').unionByName(old.select('pu_location_id')).distinct()
    codes = current.select('weather_condition_code').unionByName(old.select('weather_condition_code')).distinct()
    zone_keys = keys.select('pu_location_id').distinct().crossJoin(codes)
    code_keys = zones.crossJoin(keys.select('weather_condition_code').distinct())
    return zone_keys.unionByName(code_keys).distinct()


def _recomputed_rows(spark: SparkSession, name: str, keys: DataFrame) -> DataFrame:
    grain = GRAINS[name]
    if name in ("daily_mobility_summary", "taxi_zone_statistics"):
        source = spark.table("analysis_trips").alias("trip").join(
            F.broadcast(keys.alias("key")), _join_condition("trip", "key", grain), "inner"
        ).select("trip.*")
        if name == "daily_mobility_summary":
            return source.groupBy(*grain).agg(
                F.count("*").alias("trip_count"),
                F.sum("trip_distance").alias("distance_sum"),
                F.count("trip_distance").alias("distance_n"),
                F.sum("duration_min").alias("duration_sum"),
                F.count("duration_min").alias("duration_n"),
                F.sum("fare_amount").alias("fare_sum"),
                F.count("fare_amount").alias("fare_n"),
            )
        return source.groupBy(*grain).agg(
            F.max("pickup_zone").alias("pickup_zone"),
            F.max("pickup_borough").alias("pickup_borough"),
            F.count("*").alias("trip_count"),
            F.avg("trip_distance").alias("avg_distance_miles"),
            F.avg("duration_min").alias("avg_duration_min"),
            F.avg("fare_amount").alias("avg_fare"),
        )
    if name == "air_quality_impact_summary":
        return (spark.table("borough_hours").alias("hour")
                .join(F.broadcast(keys.alias("key")),
                      _join_condition("hour", "key", grain), "inner")
                .select("hour.pickup_borough", "hour.pickup_hour", "hour.local_hour",
                        "hour.trip_count", "hour.pm25_ug_m3", "hour.aq_station_count",
                        "hour.aqi_max", "hour.aq_available"))
    return (spark.table("zone_weather_hours").alias("hour")
            .join(F.broadcast(keys.alias("key")), _join_condition("hour", "key", grain), "inner")
            .groupBy("hour.pu_location_id", "hour.pickup_zone", "hour.pickup_borough",
                     "hour.weather_condition_code")
            .agg(F.count("*").alias("observed_hours"),
                 F.sum("trip_count").alias("trip_count"),
                 F.sum("distance_sum").alias("distance_sum"),
                 F.sum("distance_n").alias("distance_n"),
                 F.avg("trip_count").alias("mean_hourly_trips")))


def _merge_grains(spark: SparkSession, name: str, keys: DataFrame, rows: DataFrame) -> None:
    grain = GRAINS[name]
    output = str(product_path(name))
    columns = spark.read.format("delta").load(output).columns
    source = (keys.alias("key")
              .join(rows.alias("value"), _join_condition("key", "value", grain), "left")
              .select(*[F.col("key." + c).alias(c) for c in grain],
                      *[F.col("value." + c).alias(c) for c in columns if c not in grain])
              .withColumn("_remove", F.col("trip_count").isNull()))
    values = {c: f"source.`{c}`" for c in columns}
    condition = " AND ".join(f"target.`{c}` <=> source.`{c}`" for c in grain)
    (DeltaTable.forPath(spark, output).alias("target")
     .merge(source.alias("source"), condition)
     .whenMatchedDelete(condition="source._remove")
     .whenMatchedUpdate(condition="NOT source._remove", set=values)
     .whenNotMatchedInsert(condition="NOT source._remove", values=values)
     .execute())


def refresh_affected_products(
    spark: SparkSession, cfg: PlatformConfig, changes: DataFrame | None,
    source_metrics: dict[str, dict], previous_analysis: dict | None = None,
) -> dict[str, dict]:
    """Use Gold CDF plus source semantics to skip unaffected products."""
    gold = spark.read.format("delta").load(gold_path(cfg))
    analysis = effective_analysis_config(gold, analytics_config()["analysis"])
    register_analysis_views(spark, gold, analysis)
    version = current_version(spark, gold_path(cfg))
    if changes is None:
        return {name: {"mode": "skipped", "seconds": 0.0} for name in PRODUCTS}
    affected = changed_product_names(changes)
    window_changed = (previous_analysis is not None and
                      previous_analysis.get("end_date_exclusive") != analysis["end_date_exclusive"])
    if window_changed:
        affected.add("weather_impact_summary")
    results = {}
    for name in PRODUCTS:
        if name not in affected:
            results[name] = {"mode": "skipped", "seconds": 0.0}
            continue
        start = time.perf_counter()
        full_weather = name == "weather_impact_summary" and window_changed
        if full_weather or not DeltaTable.isDeltaTable(spark, str(product_path(name))):
            build_products(spark, cfg, [name])
            results[name] = {"mode": "full", "seconds": time.perf_counter() - start}
            continue
        keys = _changed_grains(changes, name, analysis["timezone"])
        if name == "weather_impact_summary":
            keys = _weather_refresh_grains(spark, changes, keys)
        if name == "air_quality_impact_summary" and window_changed:
            expanded = _expanded_air_quality_grains(spark, previous_analysis, analysis)
            if expanded is not None:
                keys = keys.unionByName(expanded).distinct()
        keys = keys.persist()
        try:
            group_count = keys.count()
            if group_count:
                rows = _recomputed_rows(spark, name, keys)
                _merge_grains(spark, name, keys, rows)
                elapsed = time.perf_counter() - start
                record_product_refresh(spark, name, gold_path(cfg), version, analysis, elapsed)
                results[name] = {"mode": "groups", "groups": group_count,
                                 "seconds": time.perf_counter() - start}
            else:
                results[name] = {"mode": "skipped", "seconds": 0.0}
        finally:
            keys.unpersist()
    return results
