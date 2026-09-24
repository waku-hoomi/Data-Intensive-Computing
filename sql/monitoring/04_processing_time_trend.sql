SELECT dataset_name,stage,started_at_utc,run_id,duration_seconds,
       avg(duration_seconds) OVER (
         PARTITION BY dataset_name,stage ORDER BY started_at_utc
         ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
       ) AS three_run_mean_seconds
FROM monitoring_pipeline_runs
WHERE dataset_name IS NOT NULL
ORDER BY dataset_name,stage,started_at_utc
