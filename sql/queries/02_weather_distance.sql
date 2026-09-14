SELECT weather_condition_code, count(*) AS trip_count,
       count(trip_distance) AS valid_distance_count,
       avg(trip_distance) AS avg_distance_miles
FROM analysis_trips
GROUP BY weather_condition_code
