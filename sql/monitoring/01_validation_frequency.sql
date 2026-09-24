SELECT dataset_name,
       count(*) AS executions,
       sum(CASE WHEN validation_failures > 0 THEN 1 ELSE 0 END) AS failed_validation_executions,
       sum(validation_failures) AS validation_failures,
       sum(rejected) AS rejected_records
FROM monitoring_pipeline_runs
WHERE stage = 'bronze' AND dataset_name IS NOT NULL
GROUP BY dataset_name
ORDER BY failed_validation_executions DESC,validation_failures DESC
