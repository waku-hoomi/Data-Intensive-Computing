"""Dataset-specific transformation rules (Task 3 & Task 4).

Each function takes a raw Spark DataFrame (as loaded verbatim from the
source file) and returns a DataFrame conforming to the platform's common
data model:
  - snake_case column names
  - all temporal columns as a proper `timestamp` type, normalized to UTC
  - consistent numeric types (DoubleType for measurements, IntegerType for
    counts/codes)
  - missing values passed through as SQL NULL (never sentinel values like
    -999 or empty string) so downstream quality checks and aggregations
    behave uniformly across every dataset

This is intentionally the ONLY dataset-specific code in the ingestion
framework. Everything generic (schema validation, dedup, null/range
checks, Delta write, metadata logging) lives in ingestion/pipeline.py and
does not know these datasets exist.
"""
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType

# NYC TLC publishes tpep_pickup_datetime / tpep_dropoff_datetime in local
# America/New_York wall-clock time (TLC data dictionary). Weather and air
# quality below are standardized to UTC. Converting taxi timestamps to UTC
# at ingestion time -- rather than at query time -- means every downstream
# consumer (Task 5 integration, Task 2 Week 2 analytical queries) can join
# on UTC hour without ever having to remember this asymmetry again.
NYC_TZ = "America/New_York"


def transform_taxi_zone_lookup(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("LocationID").cast(IntegerType()).alias("location_id"),
        F.col("Borough").cast(StringType()).alias("borough"),
        F.col("Zone").cast(StringType()).alias("zone"),
        F.col("service_zone").cast(StringType()).alias("service_zone"),
    )


def transform_weather(df: DataFrame) -> DataFrame:
    # Source columns are year/month/day/hour + NOAA ISD-derived measurements,
    # already in UTC (ISD/Meteostat convention) -- no timezone conversion
    # needed, just assembly into a single timestamp column. Raw columns are
    # read as strings (see pipeline._load_raw), so cast to int explicitly
    # before handing them to make_timestamp.
    ts = F.make_timestamp(
        F.col("year").cast(IntegerType()),
        F.col("month").cast(IntegerType()),
        F.col("day").cast(IntegerType()),
        F.col("hour").cast(IntegerType()),
        F.lit(0),
        F.lit(0.0),
    )
    return df.select(
        ts.alias("observation_ts"),
        F.col("temp").cast(DoubleType()).alias("temp_c"),
        F.col("rhum").cast(IntegerType()).alias("relative_humidity_pct"),
        F.col("prcp").cast(DoubleType()).alias("precipitation_mm"),
        F.col("snwd").cast(DoubleType()).alias("snow_depth_mm"),
        F.col("wdir").cast(IntegerType()).alias("wind_direction_deg"),
        F.col("wspd").cast(DoubleType()).alias("wind_speed_kmh"),
        F.col("wpgt").cast(DoubleType()).alias("wind_gust_kmh"),
        F.col("pres").cast(DoubleType()).alias("pressure_hpa"),
        F.col("cldc").cast(IntegerType()).alias("cloud_cover_okta"),
        F.col("coco").cast(IntegerType()).alias("condition_code"),
        F.year(ts).alias("obs_year"),
        F.month(ts).alias("obs_month"),
    )


def transform_air_quality(df: DataFrame) -> DataFrame:
    # EPA AQS ships both *_Local and *_GMT variants of date/time. We use the
    # *_GMT columns as the UTC source of truth, mirroring the weather
    # dataset, so the two can be joined on UTC hour with no adjustment.
    ts = F.to_timestamp(F.concat_ws(" ", F.col("Date GMT"), F.col("Time GMT")), "yyyy-MM-dd HH:mm")
    station_id = F.concat_ws(
        "-",
        F.lpad(F.col("State Code").cast(StringType()), 2, "0"),
        F.lpad(F.col("County Code").cast(StringType()), 3, "0"),
        F.lpad(F.col("Site Num").cast(StringType()), 4, "0"),
    )
    return (
        df.filter(F.col("Parameter Name") == "PM2.5 - Local Conditions")
        .select(
            station_id.alias("station_id"),
            F.col("Parameter Code").cast(IntegerType()).alias("parameter_code"),
            F.col("POC").cast(IntegerType()).alias("poc"),
            F.col("Latitude").cast(DoubleType()).alias("latitude"),
            F.col("Longitude").cast(DoubleType()).alias("longitude"),
            ts.alias("observation_ts"),
            F.col("Sample Measurement").cast(DoubleType()).alias("pm25_ug_m3"),
            F.col("Units of Measure").cast(StringType()).alias("units"),
            F.col("State Name").cast(StringType()).alias("state_name"),
            F.col("County Name").cast(StringType()).alias("county_name"),
            F.year(ts).alias("obs_year"),
            F.month(ts).alias("obs_month"),
        )
    )


def transform_taxi_trips(df: DataFrame) -> DataFrame:
    pickup_ts = F.to_utc_timestamp(F.col("tpep_pickup_datetime"), NYC_TZ)
    dropoff_ts = F.to_utc_timestamp(F.col("tpep_dropoff_datetime"), NYC_TZ)

    # The TLC trip records carry no natural primary key. A deterministic
    # surrogate key (hash of every raw field) lets re-ingesting the same
    # file produce identical trip_ids -- important for idempotent reruns
    # and for Week 3's incremental-update story -- while a plain
    # monotonically_increasing_id() would change on every run.
    raw_cols = [c for c in df.columns]
    trip_id = F.sha2(F.concat_ws("||", *[F.col(c).cast(StringType()) for c in raw_cols]), 256)

    return df.select(
        trip_id.alias("trip_id"),
        F.col("VendorID").cast(IntegerType()).alias("vendor_id"),
        pickup_ts.alias("pickup_ts"),
        dropoff_ts.alias("dropoff_ts"),
        F.col("passenger_count").cast(IntegerType()).alias("passenger_count"),
        F.col("trip_distance").cast(DoubleType()).alias("trip_distance"),
        F.col("RatecodeID").cast(IntegerType()).alias("rate_code_id"),
        F.col("store_and_fwd_flag").cast(StringType()).alias("store_and_fwd_flag"),
        F.col("PULocationID").cast(IntegerType()).alias("pu_location_id"),
        F.col("DOLocationID").cast(IntegerType()).alias("do_location_id"),
        F.col("payment_type").cast(IntegerType()).alias("payment_type"),
        F.col("fare_amount").cast(DoubleType()).alias("fare_amount"),
        F.col("extra").cast(DoubleType()).alias("extra"),
        F.col("mta_tax").cast(DoubleType()).alias("mta_tax"),
        F.col("tip_amount").cast(DoubleType()).alias("tip_amount"),
        F.col("tolls_amount").cast(DoubleType()).alias("tolls_amount"),
        F.col("improvement_surcharge").cast(DoubleType()).alias("improvement_surcharge"),
        F.col("total_amount").cast(DoubleType()).alias("total_amount"),
        F.col("congestion_surcharge").cast(DoubleType()).alias("congestion_surcharge"),
        F.col("Airport_fee").cast(DoubleType()).alias("airport_fee"),
        F.year(pickup_ts).alias("pickup_year"),
        F.month(pickup_ts).alias("pickup_month"),
    )