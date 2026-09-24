SELECT run_id,dataset_name,started_at_utc,processed,inserted,updated,
       duplicates,rejected,validation_failures,status
FROM monitoring_pipeline_runs
WHERE stage = 'bronze'
ORDER BY started_at_utc,run_id,dataset_name
