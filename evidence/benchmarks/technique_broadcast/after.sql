SELECT /*+ BROADCAST(z) */ z.borough,count(*) AS trip_count
          FROM bronze_taxi_trips t LEFT JOIN bronze_taxi_zone_lookup z ON t.pu_location_id=z.location_id
          WHERE t.pickup_ts >= TIMESTAMP '2024-01-01 05:00:00'
            AND t.pickup_ts < TIMESTAMP '2024-04-01 04:00:00'
          GROUP BY z.borough