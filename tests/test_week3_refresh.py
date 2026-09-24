"""Focused checks for Change Data Feed product selection."""
from datetime import datetime, timezone

from urban_platform.analytics.refresh import changed_product_names


def _row(change_type, *, humidity=50.0, condition=1):
    stamp = datetime(2024, 2, 1, 12, tzinfo=timezone.utc)
    return (
        "trip-1", stamp, stamp, "Manhattan", 1.2, 8.0, 161,
        "Midtown", condition, 10.0, 2, 42, humidity, change_type,
    )


SCHEMA = (
    "trip_id string,pickup_ts timestamp,dropoff_ts timestamp,"
    "pickup_borough string,trip_distance double,fare_amount double,"
    "pu_location_id int,pickup_zone string,weather_condition_code int,"
    "pm25_ug_m3 double,aq_station_count long,aqi_max int,"
    "humidity_pct_v2 double,_change_type string"
)


def test_non_product_schema_change_skips_products(spark):
    changes = spark.createDataFrame([
        _row("update_preimage", humidity=50.0),
        _row("update_postimage", humidity=55.0),
    ], SCHEMA)
    assert changed_product_names(changes) == set()


def test_weather_code_change_refreshes_only_weather_product(spark):
    changes = spark.createDataFrame([
        _row("update_preimage", condition=1),
        _row("update_postimage", condition=3),
    ], SCHEMA)
    assert changed_product_names(changes) == {"weather_impact_summary"}


def test_insert_refreshes_all_products(spark):
    changes = spark.createDataFrame([_row("insert")], SCHEMA)
    assert changed_product_names(changes) == {
        "daily_mobility_summary", "taxi_zone_statistics",
        "weather_impact_summary", "air_quality_impact_summary",
    }
