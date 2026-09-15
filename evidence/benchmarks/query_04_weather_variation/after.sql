WITH rates AS (
  SELECT pu_location_id,pickup_zone,pickup_borough,weather_condition_code,observed_hours,mean_hourly_trips
  FROM product_weather_impact_summary
  WHERE weather_condition_code IS NOT NULL AND observed_hours >= (SELECT min_weather_hours FROM analysis_settings)
), variation AS (
  SELECT pu_location_id,pickup_zone,pickup_borough,count(*) AS weather_categories,
         min(mean_hourly_trips) AS min_hourly_trips,max(mean_hourly_trips) AS max_hourly_trips,
         max(mean_hourly_trips)-min(mean_hourly_trips) AS absolute_variation,
         (max(mean_hourly_trips)-min(mean_hourly_trips))/nullif(avg(mean_hourly_trips),0) AS relative_variation
  FROM rates GROUP BY pu_location_id,pickup_zone,pickup_borough HAVING count(*) >= 2
)
SELECT *,dense_rank() OVER (ORDER BY absolute_variation DESC) AS variation_rank FROM variation
