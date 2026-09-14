WITH calendar AS (
  SELECT date_format(local_hour,'yyyy-MM') AS local_month,count(DISTINCT date(local_hour)) AS calendar_days
  FROM hour_spine GROUP BY date_format(local_hour,'yyyy-MM')
), counts AS (
  SELECT date_format(local_date,'yyyy-MM') AS local_month,sum(trip_count) AS trip_count
  FROM product_daily_mobility_summary GROUP BY date_format(local_date,'yyyy-MM')
), monthly AS (
  SELECT c.local_month,c.calendar_days,coalesce(t.trip_count,0L) AS trip_count
  FROM calendar c LEFT JOIN counts t USING(local_month)
)
SELECT *,trip_count/cast(calendar_days AS DOUBLE) AS mean_daily_trips,
       (trip_count-lag(trip_count) OVER (ORDER BY local_month))*100.0 /
       nullif(lag(trip_count) OVER (ORDER BY local_month),0) AS month_over_month_pct
FROM monthly
