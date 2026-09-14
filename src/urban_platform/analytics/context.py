"""Shared analytical scope and small temporal views; no implicit weather approval."""
from pathlib import Path
import yaml
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[3]
SQL_ROOT = ROOT / "sql"
QUERY_IDS = ["01_zone_monthly", "02_weather_distance", "03_air_quality_demand",
             "04_weather_variation", "05_peak_hours", "06_monthly_trend"]
WEATHER_QUERIES = {"02_weather_distance", "04_weather_variation"}


def analytics_config():
    return yaml.safe_load((ROOT / "config/analytics.yaml").read_text())


def require_weather_confirmation():
    cfg = analytics_config()["analysis"]
    if not cfg["weather_interpretation_approved"]:
        raise ValueError("Weather interpretation is awaiting the user's decision; see config/analytics.yaml")


def sql_text(name, directory="queries"):
    return (SQL_ROOT / directory / f"{name}.sql").read_text()


def register_analysis_views(spark, integrated_df, cfg=None):
    cfg = cfg or analytics_config()["analysis"]
    tz = cfg["timezone"]
    start, end = cfg["start_date"], cfg["end_date_exclusive"]
    # Values originate from local configuration, not arbitrary SQL user input.
    from datetime import date
    date.fromisoformat(start)
    date.fromisoformat(end)
    if tz != "America/New_York":
        raise ValueError("This version supports the approved NYC timezone only")
    integrated_df.createOrReplaceTempView("integrated_source")
    spark.sql(f"""CREATE OR REPLACE TEMP VIEW analysis_trips AS
      SELECT *, from_utc_timestamp(pickup_ts, '{tz}') AS pickup_local,
        date(from_utc_timestamp(pickup_ts, '{tz}')) AS local_date,
        date_trunc('hour', pickup_ts) AS pickup_hour,
        date_format(from_utc_timestamp(pickup_ts, '{tz}'), 'yyyy-MM') AS local_month,
        (unix_timestamp(dropoff_ts)-unix_timestamp(pickup_ts))/60.0 AS duration_min
      FROM integrated_source
      WHERE pickup_ts >= to_utc_timestamp(TIMESTAMP '{start} 00:00:00', '{tz}')
        AND pickup_ts < to_utc_timestamp(TIMESTAMP '{end} 00:00:00', '{tz}')
    """)
    spark.sql(f"""CREATE OR REPLACE TEMP VIEW hour_spine AS
      SELECT pickup_hour, from_utc_timestamp(pickup_hour, '{tz}') AS local_hour
      FROM (SELECT explode(sequence(
        to_utc_timestamp(TIMESTAMP '{start} 00:00:00', '{tz}'),
        to_utc_timestamp(TIMESTAMP '{end} 00:00:00', '{tz}') - INTERVAL 1 HOUR,
        INTERVAL 1 HOUR)) AS pickup_hour)
    """)
    spark.sql("""CREATE OR REPLACE TEMP VIEW city_hours AS
      SELECT h.*, c.weather_condition_code, coalesce(c.trip_count,0L) AS trip_count
      FROM hour_spine h LEFT JOIN (
        SELECT pickup_hour, max(weather_condition_code) AS weather_condition_code,
               count(*) AS trip_count FROM analysis_trips GROUP BY pickup_hour
      ) c USING(pickup_hour)
    """)
    spark.sql("""CREATE OR REPLACE TEMP VIEW zone_weather_hours AS
      SELECT z.pu_location_id, z.pickup_zone, z.pickup_borough, h.pickup_hour,
             h.weather_condition_code, coalesce(t.trip_count,0L) AS trip_count,
             coalesce(t.distance_sum,0D) AS distance_sum,
             coalesce(t.distance_n,0L) AS distance_n
      FROM (SELECT DISTINCT pu_location_id, pickup_zone, pickup_borough
            FROM analysis_trips WHERE pu_location_id IS NOT NULL) z
      CROSS JOIN city_hours h
      LEFT JOIN (SELECT pu_location_id, pickup_hour, count(*) AS trip_count,
                        sum(trip_distance) AS distance_sum, count(trip_distance) AS distance_n
                 FROM analysis_trips GROUP BY pu_location_id,pickup_hour) t
      ON z.pu_location_id=t.pu_location_id AND h.pickup_hour=t.pickup_hour
    """)
    spark.sql("""CREATE OR REPLACE TEMP VIEW borough_hours AS
      SELECT b.pickup_borough, h.pickup_hour, h.local_hour,
             coalesce(t.trip_count,0L) AS trip_count, t.pm25_ug_m3,
             t.aq_station_count, t.pm25_ug_m3 IS NOT NULL AS aq_available
      FROM (SELECT DISTINCT pickup_borough FROM analysis_trips
            WHERE pickup_borough IS NOT NULL) b CROSS JOIN hour_spine h
      LEFT JOIN (SELECT pickup_borough,pickup_hour,count(*) AS trip_count,
                        max(pm25_ug_m3) AS pm25_ug_m3, max(aq_station_count) AS aq_station_count
                 FROM analysis_trips GROUP BY pickup_borough,pickup_hour) t
      ON b.pickup_borough=t.pickup_borough AND h.pickup_hour=t.pickup_hour
    """)
    spark.sql(f"CREATE OR REPLACE TEMP VIEW analysis_settings AS SELECT {int(cfg['min_weather_hours'])} AS min_weather_hours")


def read_integrated(spark, platform_cfg):
    return spark.read.format("delta").load(str(Path(platform_cfg.gold_dir) / "integrated_taxi_trips"))
