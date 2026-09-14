"""Dataset-specific transformation rules (Task 3 & Task 4).

Each function takes a raw Spark DataFrame (as loaded verbatim from the
source file) and returns a DataFrame conforming to the platform's common
data model:
  - snake_case column names
  - all temporal columns as a proper `timestamp` type, normalized to UTC
  - consistent numeric types (DoubleType for measurements, IntegerType for
    counts/codes)
  - invalid numerical parses become NULL even with Spark ANSI enabled;
    lookup text sentinels are normalized and configured ranges reject
    impossible measurements (there is no universal numeric sentinel rule).

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


def _text(name):
    value = F.trim(F.col(name).cast("string"))
    return F.when(F.upper(value).isin("", "N/A", "NULL", "NA"), F.lit(None)).otherwise(value)


def transform_taxi_zone_lookup(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("LocationID").try_cast("int").alias("location_id"),
        _text("Borough").alias("borough"),
        _text("Zone").alias("zone"),
        _text("service_zone").alias("service_zone"),
    )


def transform_weather(df: DataFrame) -> DataFrame:
    # Assemble year/month/day/hour in the UTC session. This interpretation
    # requires the provenance decision recorded in config/analytics.yaml;
    # it is not independently established by the CSV column names.
    ts = F.try_make_timestamp(
        F.col("year").try_cast("int"),
        F.col("month").try_cast("int"),
        F.col("day").try_cast("int"),
        F.col("hour").try_cast("int"),
        F.lit(0),
        F.lit(0.0),
    )
    return df.select(
        ts.alias("observation_ts"),
        F.col("temp").try_cast("double").alias("temp_c"),
        F.col("rhum").try_cast("int").alias("relative_humidity_pct"),
        F.col("prcp").try_cast("double").alias("precipitation_mm"),
        F.col("snwd").try_cast("double").alias("snow_depth_mm"),
        F.col("wdir").try_cast("int").alias("wind_direction_deg"),
        F.col("wspd").try_cast("double").alias("wind_speed_kmh"),
        F.col("wpgt").try_cast("double").alias("wind_gust_kmh"),
        F.col("pres").try_cast("double").alias("pressure_hpa"),
        F.col("cldc").try_cast("int").alias("cloud_cover_okta"),
        F.col("coco").try_cast("int").alias("condition_code"),
        F.year(ts).alias("obs_year"),
        F.month(ts).alias("obs_month"),
    )


def transform_air_quality(df: DataFrame) -> DataFrame:
    # EPA AQS ships both *_Local and *_GMT variants of date/time. We use the
    # *_GMT columns as the UTC source of truth, mirroring the weather
    # dataset, so the two can be joined on UTC hour with no adjustment.
    ts = F.try_to_timestamp(F.concat_ws(" ", _text("Date GMT"), _text("Time GMT")), F.lit("yyyy-MM-dd HH:mm"))
    station_id = F.concat_ws(
        "-",
        F.lpad(F.col("State Code").cast(StringType()), 2, "0"),
        F.lpad(F.col("County Code").cast(StringType()), 3, "0"),
        F.lpad(F.col("Site Num").cast(StringType()), 4, "0"),
    )
    valid_id = (_text("State Code").rlike(r"^\d{1,2}$") & _text("County Code").rlike(r"^\d{1,3}$")
                & _text("Site Num").rlike(r"^\d{1,4}$"))
    station_id = F.when(valid_id, station_id)
    return (
        df.filter(F.col("Parameter Name") == "PM2.5 - Local Conditions")
        .select(
            station_id.alias("station_id"),
            F.col("Parameter Code").try_cast("int").alias("parameter_code"),
            F.col("POC").try_cast("int").alias("poc"),
            F.col("Latitude").try_cast("double").alias("latitude"),
            F.col("Longitude").try_cast("double").alias("longitude"),
            ts.alias("observation_ts"),
            F.col("Sample Measurement").try_cast("double").alias("pm25_ug_m3"),
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

    # No explicit source trip ID. Preserve sorted named fields and NULL
    # positions in JSON before hashing; validation profiles candidate keys.
    raw_cols = sorted(df.columns)
    identity = F.to_json(F.struct(*[F.col(c) for c in raw_cols]), {"ignoreNullFields": "false"})
    trip_id = F.sha2(identity, 256)

    return df.select(
        trip_id.alias("trip_id"),
        F.col("VendorID").try_cast("int").alias("vendor_id"),
        pickup_ts.alias("pickup_ts"),
        dropoff_ts.alias("dropoff_ts"),
        F.col("passenger_count").try_cast("int").alias("passenger_count"),
        F.col("trip_distance").try_cast("double").alias("trip_distance"),
        F.col("RatecodeID").try_cast("int").alias("rate_code_id"),
        F.col("store_and_fwd_flag").cast(StringType()).alias("store_and_fwd_flag"),
        F.col("PULocationID").try_cast("int").alias("pu_location_id"),
        F.col("DOLocationID").try_cast("int").alias("do_location_id"),
        F.col("payment_type").try_cast("int").alias("payment_type"),
        F.col("fare_amount").try_cast("double").alias("fare_amount"),
        F.col("extra").try_cast("double").alias("extra"),
        F.col("mta_tax").try_cast("double").alias("mta_tax"),
        F.col("tip_amount").try_cast("double").alias("tip_amount"),
        F.col("tolls_amount").try_cast("double").alias("tolls_amount"),
        F.col("improvement_surcharge").try_cast("double").alias("improvement_surcharge"),
        F.col("total_amount").try_cast("double").alias("total_amount"),
        F.col("congestion_surcharge").try_cast("double").alias("congestion_surcharge"),
        F.col("Airport_fee").try_cast("double").alias("airport_fee"),
        F.year(pickup_ts).alias("pickup_year"),
        F.month(pickup_ts).alias("pickup_month"),
    )
