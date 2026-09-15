SELECT local_month, pu_location_id, pickup_zone, pickup_borough, count(*) AS trip_count
FROM analysis_trips WHERE local_month='2024-02'
GROUP BY local_month, pu_location_id, pickup_zone, pickup_borough
