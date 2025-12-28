CREATE TABLE sample_superstore.superstore.job_audit_metadata (
  workspace_id STRING,
  workspace_url STRING,
  job_id STRING,
  job_name STRING,
  job_repair_count STRING,
  job_run_id STRING,
  job_start_time_timestamp TIMESTAMP,
  job_end_time_timestamp TIMESTAMP,
  job_trigger_time_timestamp TIMESTAMP,
  job_trigger_type STRING,
  task_name STRING,
  task_run_id STRING,
  task_notebook_path STRING,
  task_execution_count STRING,
  dml_statistics STRUCT<dml_statistics: STRUCT<affected_row_count: STRING, deleted_row_count: STRING, inserted_row_count: STRING, source_row_count: STRING, updated_row_count: STRING>>,
  error_result STRUCT<error_code: STRING, error_details: STRUCT<argument: STRING, debugInfo: STRING, filename: STRING, location: STRING, message: STRING, type: STRING>>
)