from datetime import datetime, timezone

from pyspark.sql import functions as F

from urban_platform.ml import dataset
from urban_platform.ml.features import build_feature_pipeline, feature_columns


def _demand_df(spark):
    rows = [
        (10, "Zone A", "Brooklyn", datetime(2024, 2, 1, 10, tzinfo=timezone.utc), 2,
         5.0, 60.0, 55.0, 0.0, 3.0, 1, None, None, None),
        (10, "Zone A", "Brooklyn", datetime(2024, 2, 1, 11, tzinfo=timezone.utc), 4,
         6.0, 61.0, 56.0, 0.5, 4.0, 1, 8.0, 40, 1),
        (20, "Zone B", "Manhattan", datetime(2024, 2, 1, 10, tzinfo=timezone.utc), 0,
         5.5, 59.0, 54.0, 0.0, 2.0, 2, None, None, None),
    ]
    return spark.createDataFrame(
        rows,
        "pu_location_id int,pickup_zone string,pickup_borough string,pickup_hour timestamp,"
        "trip_count long,temp_c double,relative_humidity_pct double,humidity_pct_v2 double,"
        "precipitation_mm double,wind_speed_kmh double,weather_condition_code int,"
        "pm25_ug_m3 double,aqi_max int,aq_station_count long",
    ).withColumn("local_hour", F.from_utc_timestamp("pickup_hour", "America/New_York"))


def test_categoricals_are_indexed_not_used_raw(spark):
    df = _demand_df(spark)
    model = build_feature_pipeline().fit(df)
    out = model.transform(df).first()
    assert out["pu_location_id_idx"] is not None
    assert out["pickup_borough_idx"] is not None


def test_missing_air_quality_is_imputed_not_dropped(spark):
    df = _demand_df(spark)
    model = build_feature_pipeline().fit(df)
    out = model.transform(df)
    # No rows lost and no nulls left in the assembled feature vector.
    assert out.count() == 3
    assert out.filter(F.col("pm25_ug_m3_imputed").isNull()).count() == 0
    assert out.filter(F.col("features").isNull()).count() == 0


def test_temporal_features_derived(spark):
    df = _demand_df(spark)
    out = build_feature_pipeline().fit(df).transform(df)
    hours = {row["hour_of_day"] for row in out.collect()}
    # 2024-02-01 is a Thursday; NYC local hour differs from UTC by -5.
    assert hours == {5, 6}
    assert all(row["is_weekend"] == 0 for row in out.collect())


def test_feature_vector_has_expected_width(spark):
    df = _demand_df(spark)
    out = build_feature_pipeline().fit(df).transform(df).first()
    assert len(out["features"]) == len(feature_columns())
