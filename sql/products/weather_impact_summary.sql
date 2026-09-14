SELECT pu_location_id,pickup_zone,pickup_borough,weather_condition_code,
       count(*) AS observed_hours,sum(trip_count) AS trip_count,
       sum(distance_sum) AS distance_sum,sum(distance_n) AS distance_n,
       avg(trip_count) AS mean_hourly_trips
FROM zone_weather_hours
GROUP BY pu_location_id,pickup_zone,pickup_borough,weather_condition_code
