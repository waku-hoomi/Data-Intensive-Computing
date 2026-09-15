SELECT local_month, pu_location_id, pickup_zone, pickup_borough, count(*) AS trip_count
FROM analysis_trips
GROUP BY local_month, pu_location_id, pickup_zone, pickup_borough
