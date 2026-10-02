from datetime import datetime, timezone
from types import SimpleNamespace

from pyspark.sql import functions as F

from urban_platform.ml import compare


def test_approach_a_and_b_agree_on_total_demand(spark, tmp_path, monkeypatch):
    """Approach A re-derives the dataset from raw files; Approach B reads the
    platform's Gold table. On the same underlying trips they must agree on the
    one number that matters: total demand per zone-hour."""
    raw = tmp_path / "dataset"
    (raw / "air_quality").mkdir(parents=True)

    zones_csv = "LocationID,Zone,Borough\n1,Zone A,Brooklyn\n2,Zone B,Manhattan\n"
    (raw / "taxi_zone_lookup.csv").write_text(zones_csv, encoding="utf-8")

    weather_csv = "year,month,day,hour,temp,rhum,prcp,wspd,coco\n"
    weather_csv += "2024,1,1,5,5.0,60,0.0,3.0,1\n2024,1,1,6,6.0,61,0.0,4.0,1\n"
    (raw / "weather.csv").write_text(weather_csv, encoding="utf-8")

    (raw / "air_quality" / "hourly_88101_2024.csv").write_text(
        "State Name,Parameter Name,County Name,Date GMT,Time GMT,Sample Measurement\n"
        "New York,PM2.5 - Local Conditions,Kings,2024-01-01,05:00,8.0\n",
        encoding="utf-8",
    )

    trips = spark.createDataFrame(
        [(datetime(2024, 1, 1, 0, 0, 0), 1),   # 00:00 NYC -> 05:00 UTC, zone 1
         (datetime(2024, 1, 1, 1, 0, 0), 1)],  # 01:00 NYC -> 06:00 UTC, zone 1
        "tpep_pickup_datetime timestamp,PULocationID int",
    )
    # Approach A reads the three named monthly files; the fixture only needs to
    # populate them, month 01 carries the actual rows.
    trips.write.parquet(str(raw / "yellow_tripdata_2024-01.parquet"))
    for month in ("02", "03"):
        trips.filter(F.lit(False)).write.parquet(str(raw / f"yellow_tripdata_2024-{month}.parquet"))

    cfg = {
        "label_col": "trip_count",
        "split_boundaries": {"validation_start": "2024-06-01", "test_start": "2024-07-01"},
    }
    platform_cfg = SimpleNamespace(raw_data_dir=str(raw))

    approach_a = compare.build_dataset_from_raw(spark, platform_cfg, cfg)
    a_total = approach_a.agg(F.sum("trip_count")).first()[0]
    assert a_total == 2

    from urban_platform.ml import dataset as dataset_module
    from urban_platform.analytics.context import register_analysis_views, sql_text

    def fake_build_training_dataset(spark_, platform_cfg_, cfg_):
        integrated = spark.createDataFrame(
            [("a", datetime(2024, 1, 1, 5, tzinfo=timezone.utc), datetime(2024, 1, 1, 5, 20, tzinfo=timezone.utc),
              1, "Zone A", "Brooklyn", 2.0, 5.0, 60.0, 55.0, 0.0, 3.0, 1, 8.0, 40, 1),
             ("b", datetime(2024, 1, 1, 6, tzinfo=timezone.utc), datetime(2024, 1, 1, 6, 20, tzinfo=timezone.utc),
              1, "Zone A", "Brooklyn", 2.5, 6.0, 61.0, 56.0, 0.0, 4.0, 1, 8.0, 40, 1)],
            "trip_id string,pickup_ts timestamp,dropoff_ts timestamp,pu_location_id int,"
            "pickup_zone string,pickup_borough string,trip_distance double,temp_c double,"
            "relative_humidity_pct double,humidity_pct_v2 double,precipitation_mm double,"
            "wind_speed_kmh double,weather_condition_code int,pm25_ug_m3 double,aqi_max int,"
            "aq_station_count long",
        )
        register_analysis_views(spark_, integrated)
        return dataset_module.add_split(spark_.sql(sql_text("zone_hour_demand", "ml")), cfg_)

    monkeypatch.setattr(dataset_module, "build_training_dataset", fake_build_training_dataset)
    approach_b = dataset_module.build_training_dataset(spark, platform_cfg, cfg)
    b_total = approach_b.agg(F.sum("trip_count")).first()[0]
    assert b_total == a_total


def test_summarize_reports_rows_by_split(spark):
    df = spark.createDataFrame(
        [(1, "train"), (0, "train"), (2, "test")], "trip_count long,split string"
    )
    summary = compare.summarize(df, "trip_count")
    assert summary["rows"] == 3
    assert summary["total_demand"] == 3
    assert summary["rows_by_split"] == {"train": 2, "test": 1}
