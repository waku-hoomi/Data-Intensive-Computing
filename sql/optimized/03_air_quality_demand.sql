SELECT pickup_borough,count(*) AS observed_hours,
       avg(pm25_ug_m3) AS mean_pm25_ug_m3,avg(trip_count) AS mean_hourly_trips,
       try_divide(covar_pop(pm25_ug_m3,cast(trip_count AS DOUBLE)),
                  stddev_pop(pm25_ug_m3)*stddev_pop(cast(trip_count AS DOUBLE))) AS pearson_r
FROM product_air_quality_impact_summary WHERE aq_available GROUP BY pickup_borough
