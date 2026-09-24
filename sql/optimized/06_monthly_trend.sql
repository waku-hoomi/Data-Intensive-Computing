WITH calendar AS (
  SELECT date_format(local_hour,'yyyy-MM') AS local_month,
         count(DISTINCT date(local_hour)) AS calendar_days,
         day(last_day(max(date(local_hour)))) AS days_in_month
  FROM hour_spine GROUP BY date_format(local_hour,'yyyy-MM')
), counts AS (
  SELECT date_format(local_date,'yyyy-MM') AS local_month,sum(trip_count) AS trip_count
  FROM product_daily_mobility_summary GROUP BY date_format(local_date,'yyyy-MM')
), monthly AS (
  SELECT c.local_month,c.calendar_days,c.days_in_month,
         coalesce(t.trip_count,0L) AS trip_count
  FROM calendar c LEFT JOIN counts t USING(local_month)
), ranked AS (
  SELECT *,lag(trip_count) OVER (ORDER BY local_month) AS prior_trip_count,
         lag(calendar_days = days_in_month) OVER (ORDER BY local_month) AS prior_complete
  FROM monthly
)
SELECT local_month,calendar_days,trip_count,
       trip_count/cast(calendar_days AS DOUBLE) AS mean_daily_trips,
       calendar_days = days_in_month AS is_complete_month,
       CASE WHEN calendar_days = days_in_month AND prior_complete
            THEN (trip_count-prior_trip_count)*100.0/nullif(prior_trip_count,0)
            ELSE NULL END AS month_over_month_pct
FROM ranked
