SELECT dataset_name,count(*) AS executions,
       avg(duration_seconds) AS mean_seconds,
       max(duration_seconds) AS longest_seconds
FROM monitoring_pipeline_runs
WHERE stage = 'bronze' AND dataset_name IS NOT NULL
GROUP BY dataset_name ORDER BY mean_seconds DESC
