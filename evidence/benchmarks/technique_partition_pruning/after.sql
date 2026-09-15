SELECT local_month, pu_location_id, pickup_zone, pickup_borough, count(*) AS trip_count
FROM analysis_trips WHERE local_month='2024-02' AND pickup_year=2024 AND pickup_month IN (2,3)
GROUP BY local_month, pu_location_id, pickup_zone, pickup_borough
