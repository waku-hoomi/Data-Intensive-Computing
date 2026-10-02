from datetime import datetime, timezone

from pyspark.sql import functions as F

from urban_platform.ml.train import load_model, train_and_evaluate


def _tiny_demand_df(spark):
    rows = []
    zones = [(10, "Zone A", "Brooklyn"), (20, "Zone B", "Manhattan")]
    for zone_id, zone, borough in zones:
        for day in range(1, 11):
            for hour in (8, 18):
                ts = datetime(2024, 1, day, hour, tzinfo=timezone.utc)
                demand = (hour // 9) * 3 + (zone_id // 10)
                rows.append((zone_id, zone, borough, ts, float(demand), 5.0 + day * 0.1,
                             60.0, 55.0, 0.0, 3.0, 1, 8.0, 40, 1))
    df = spark.createDataFrame(
        rows,
        "pu_location_id int,pickup_zone string,pickup_borough string,pickup_hour timestamp,"
        "trip_count double,temp_c double,relative_humidity_pct double,humidity_pct_v2 double,"
        "precipitation_mm double,wind_speed_kmh double,weather_condition_code int,"
        "pm25_ug_m3 double,aqi_max int,aq_station_count long",
    ).withColumn("local_hour", F.from_utc_timestamp("pickup_hour", "America/New_York"))
    # 16 days isn't enough to cross the real validation/test cutoffs, so split
    # explicitly here rather than through config-based calendar dates.
    return df.withColumn(
        "split",
        F.when(F.dayofmonth("pickup_hour") <= 6, F.lit("train"))
        .when(F.dayofmonth("pickup_hour") <= 8, F.lit("validation"))
        .otherwise(F.lit("test")),
    )


def test_train_and_evaluate_saves_a_reloadable_model(spark, tmp_path, monkeypatch):
    import urban_platform.ml.train as train_module

    monkeypatch.setattr(train_module, "ARTIFACTS", tmp_path / "artifacts")
    monkeypatch.setattr(train_module, "MODELS", tmp_path / "models")
    cfg = {
        "label_col": "trip_count",
        "model": {"max_iter": 5, "max_depth": 2, "seed": 2221},
    }
    df = _tiny_demand_df(spark)
    report = train_and_evaluate(spark, object(), cfg, run_id="unit-test", training_df=df)

    assert report["train_rows"] > 0 and report["test_rows"] > 0
    assert report["validation"]["rmse"] >= 0
    assert report["test"]["rmse"] >= 0
    assert (tmp_path / "artifacts" / "train_unit-test.json").exists()

    reloaded = load_model("unit-test")
    predictions = reloaded.transform(df.filter(df.split == "test"))
    assert predictions.count() == df.filter(df.split == "test").count()
