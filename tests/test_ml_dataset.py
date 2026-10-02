from datetime import datetime, timezone

from pyspark.sql import functions as F

from urban_platform.ml import dataset


def _integrated(spark, rows):
    # dropoff_ts and trip_distance are needed by register_analysis_views
    # (duration_min) and the daily product views, even though the demand
    # dataset itself only uses pickup fields.
    return spark.createDataFrame(
        rows,
        "trip_id string,pickup_ts timestamp,dropoff_ts timestamp,pu_location_id int,"
        "pickup_zone string,pickup_borough string,trip_distance double,temp_c double,"
        "relative_humidity_pct double,humidity_pct_v2 double,precipitation_mm double,"
        "wind_speed_kmh double,weather_condition_code int,pm25_ug_m3 double,aqi_max int,"
        "aq_station_count long",
    )


def test_split_boundaries_are_chronological(spark):
    cfg = dataset.ml_config()
    df = spark.createDataFrame(
        [("2024-02-15 05:00:00",), ("2024-03-10 05:00:00",), ("2024-03-25 05:00:00",)],
        "pickup_hour string",
    ).withColumn("pickup_hour", F.to_timestamp("pickup_hour"))
    labelled = {row["pickup_hour"].date().isoformat(): row["split"]
                for row in dataset.add_split(df, cfg).collect()}
    assert labelled == {"2024-02-15": "train", "2024-03-10": "validation", "2024-03-25": "test"}


def test_zero_demand_hours_are_retained(spark, monkeypatch):
    # One zone, three hours in the window, but trips only in one of them.
    rows = [
        ("a", datetime(2024, 2, 1, 10, tzinfo=timezone.utc), datetime(2024, 2, 1, 10, 20, tzinfo=timezone.utc),
         1, "Zone A", "Brooklyn", 2.0, 5.0, 60.0, 55.0, 0.0, 3.0, 1, 8.0, 40, 1),
        ("b", datetime(2024, 2, 1, 11, tzinfo=timezone.utc), datetime(2024, 2, 1, 11, 20, tzinfo=timezone.utc),
         1, "Zone A", "Brooklyn", 3.0, 6.0, 61.0, 56.0, 0.0, 4.0, 1, 9.0, 41, 1),
    ]
    integrated = _integrated(spark, rows)
    monkeypatch.setattr(
        "urban_platform.analytics.context.read_integrated", lambda *a, **k: integrated
    )
    built = dataset.build_training_dataset(spark, object(), dataset.ml_config())
    counts = sorted(row["trip_count"] for row in built.collect())
    # Hours with no trips must appear with trip_count 0, not be dropped.
    assert counts[0] == 0
    assert built.agg(F.sum("trip_count")).first()[0] == 2


def test_weather_and_air_quality_attached_per_zone_hour(spark, monkeypatch):
    rows = [
        ("a", datetime(2024, 2, 1, 10, tzinfo=timezone.utc), datetime(2024, 2, 1, 10, 20, tzinfo=timezone.utc),
         1, "Zone A", "Brooklyn", 2.0, 5.0, 60.0, 55.0, 0.0, 3.0, 1, 8.0, 40, 1),
    ]
    integrated = _integrated(spark, rows)
    monkeypatch.setattr(
        "urban_platform.analytics.context.read_integrated", lambda *a, **k: integrated
    )
    built = dataset.build_training_dataset(spark, object(), dataset.ml_config())
    row = built.filter(F.col("trip_count") == 1).first()
    assert row["temp_c"] == 5.0
    assert row["pm25_ug_m3"] == 8.0
