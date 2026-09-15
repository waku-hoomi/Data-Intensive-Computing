WITH demand AS (
  SELECT weekday(local_hour)+1 AS weekday_iso, hour(local_hour) AS local_hour_of_day,
         count(*) AS observed_hours, sum(trip_count) AS trip_count,
         avg(trip_count) AS mean_hourly_trips
  FROM city_hours GROUP BY weekday(local_hour)+1, hour(local_hour)
), ranked AS (
  SELECT *, dense_rank() OVER (PARTITION BY weekday_iso ORDER BY mean_hourly_trips DESC) AS peak_rank
  FROM demand
)
SELECT * FROM ranked WHERE peak_rank=1
