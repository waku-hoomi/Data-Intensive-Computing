from datetime import datetime, timezone
from dataclasses import replace
from pathlib import Path

from urban_platform.integration.integrate import aggregate_nyc_air_quality
from urban_platform.ingestion.pipeline import _validate_raw_schema, SchemaValidationError
from urban_platform.ingestion.quality import apply_quality_checks
from urban_platform.utils.config import load_config
from urban_platform.transform.transforms import transform_weather, transform_taxi_trips
from urban_platform.analytics.context import register_analysis_views, sql_text, QUERY_IDS
import pytest

ROOT = Path(__file__).resolve().parents[1]


def test_geography_and_equal_station_weight(spark):
    hour = datetime(2024, 1, 1, 8, tzinfo=timezone.utc)
    df = spark.createDataFrame([
        ("06-031-0001", "Kings", hour, 100.0),
        ("36-047-0001", "Kings", hour, 10.0),
        ("36-047-0001", "Kings", hour, 20.0),
        ("36-047-0002", "Kings", hour, 25.0),
        ("36-001-0001", "Albany", hour, 999.0),
    ], "station_id string, county_name string, observation_ts timestamp, pm25_ug_m3 double")
    rows = aggregate_nyc_air_quality(df).collect()
    assert len(rows) == 1
    assert rows[0].pickup_borough == "Brooklyn"
    assert rows[0].pm25_ug_m3 == 20.0
    assert rows[0].aq_station_count == 2


def test_ansi_safe_invalid_weather_and_schema(spark):
    cfg = load_config(str(ROOT / "config/datasets.yaml")).get_dataset("weather")
    values = {c: "0" for c in cfg.expected_raw_columns}
    values.update(year="2024", month="13", day="1", hour="0", temp="bad")
    df = spark.createDataFrame([values])
    with pytest.raises(SchemaValidationError):
        _validate_raw_schema(df.drop("snwd"), cfg)
    row = transform_weather(df).first()
    assert row.observation_ts is None and row.temp_c is None


def test_quality_preserves_valid_duplicate_and_rejects_negative_duration(spark):
    cfg = load_config(str(ROOT / "config/datasets.yaml")).get_dataset("taxi_trips")
    cfg = replace(cfg, numeric_rules=[])
    ts = datetime(2024, 1, 1, 12, tzinfo=timezone.utc)
    later = datetime(2024, 1, 1, 13, tzinfo=timezone.utc)
    df = spark.createDataFrame([("a", later, ts), ("a", ts, later), ("b", ts, later), ("b", ts, later)],
                               "trip_id string,pickup_ts timestamp,dropoff_ts timestamp")
    clean, report = apply_quality_checks(df, cfg)
    assert {r.trip_id for r in clean.collect()} == {"a", "b"}
    assert report.rejected_rows == 2
    assert report.rejected_by_reason == {"negative_trip_duration": 1, "duplicate_primary_key": 1}


def test_trip_identity_preserves_null_positions_and_column_order(spark):
    cfg = load_config(str(ROOT / "config/datasets.yaml")).get_dataset("taxi_trips")
    row = {c: "1" for c in cfg.expected_raw_columns}
    row.update(tpep_pickup_datetime="2024-01-01 12:00:00",tpep_dropoff_datetime="2024-01-01 13:00:00")
    a, b = dict(row), dict(row)
    a.update(extra=None, mta_tax="1")
    b.update(extra="1", mta_tax=None)
    df = spark.createDataFrame([a,b])
    ids = {r.trip_id for r in transform_taxi_trips(df).select("trip_id").collect()}
    assert len(ids) == 2
    assert ids == {r.trip_id for r in transform_taxi_trips(df.select(*reversed(df.columns))).select("trip_id").collect()}


def test_six_queries_and_product_totals(spark):
    rows = []
    for index, (hour, zone, code, distance, pm) in enumerate([
        (5,1,1,2.,10.), (5,1,1,4.,10.), (6,1,2,6.,20.),
        (6,2,2,8.,20.), (7,1,1,10.,30.)]):
        rows.append((str(index), datetime(2024,1,1,hour,tzinfo=timezone.utc), datetime(2024,1,1,hour,30,tzinfo=timezone.utc),
                     zone,f"Zone {zone}","Brooklyn",distance,10.,code,pm,1,2024,1))
    df = spark.createDataFrame(rows,"trip_id string,pickup_ts timestamp,dropoff_ts timestamp,pu_location_id int,pickup_zone string,pickup_borough string,trip_distance double,fare_amount double,weather_condition_code int,pm25_ug_m3 double,aq_station_count long,pickup_year int,pickup_month int")
    register_analysis_views(spark,df,{"timezone":"America/New_York","start_date":"2024-01-01","end_date_exclusive":"2024-01-02","min_weather_hours":1})
    results = {q:spark.sql(sql_text(q)).collect() for q in QUERY_IDS}
    assert sum(r.trip_count for r in results["01_zone_monthly"]) == 5
    distance = {r.weather_condition_code:r.avg_distance_miles for r in results["02_weather_distance"]}
    assert distance == {1:pytest.approx(16/3),2:7.0}
    assert results["03_air_quality_demand"][0].observed_hours == 3
    assert results["03_air_quality_demand"][0].pearson_r == pytest.approx(-0.8660254037844386)
    variation = {r.pu_location_id:r.absolute_variation for r in results["04_weather_variation"]}
    assert variation == {1:0.5,2:1.0}  # Zero-trip zone-hours are retained.
    assert {r.local_hour_of_day for r in results["05_peak_hours"]} == {0,1}
    assert results["06_monthly_trend"][0].mean_daily_trips == 5.0
    for name in ["daily_mobility_summary","taxi_zone_statistics","weather_impact_summary","air_quality_impact_summary"]:
        assert sum(r.trip_count for r in spark.sql(sql_text(name,"products")).collect()) == 5


def test_dst_calendar_has_23_elapsed_hours(spark):
    # Test only the hour spine: no assumption about missing source observations.
    empty = spark.createDataFrame([],"trip_id string,pickup_ts timestamp,dropoff_ts timestamp,pu_location_id int,pickup_zone string,pickup_borough string,trip_distance double,fare_amount double,weather_condition_code int,pm25_ug_m3 double,aq_station_count long,pickup_year int,pickup_month int")
    register_analysis_views(spark,empty,{"timezone":"America/New_York","start_date":"2024-03-10","end_date_exclusive":"2024-03-11","min_weather_hours":1})
    assert spark.table("hour_spine").count() == 23
