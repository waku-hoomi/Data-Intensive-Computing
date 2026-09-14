SELECT local_date, pickup_borough, count(*) AS trip_count,
       sum(trip_distance) AS distance_sum, count(trip_distance) AS distance_n,
       sum(duration_min) AS duration_sum, count(duration_min) AS duration_n,
       sum(fare_amount) AS fare_sum, count(fare_amount) AS fare_n
FROM analysis_trips GROUP BY local_date, pickup_borough
