SELECT local_month,pu_location_id,pickup_zone,pickup_borough,count(*) AS trip_count,
       avg(trip_distance) AS avg_distance_miles,avg(duration_min) AS avg_duration_min,
       avg(fare_amount) AS avg_fare
FROM analysis_trips GROUP BY local_month,pu_location_id,pickup_zone,pickup_borough
