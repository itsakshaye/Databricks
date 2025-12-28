# Metadata-Driven Ingestion Framework

A simple, metadata-driven batch ingestion framework for Databricks automates data movement from the **bronze** to **silver** layer using easy-to-manage JSON configuration files. Supports **Append**, **Override**, **SCD1**, and **SCD2** strategies with built-in auditing.

---

## Key Features

- **Metadata-Driven:** Add or modify ingestion for tables by editing JSON—no code changes required.  
- **Flexible Load Types:** Supports `Append`, `Override`, `SCD1`, and `SCD2`.  
- **Comprehensive Audit:** Captures run metadata, DML counts, and error details.  
- **Configurable:** Transformations, filters, and incremental loading via watermarks in JSON.  
- **Lakehouse-Friendly:** Designed for bronze → silver data flows.

---

## Quick Start

1. Upload the Utilities folder to either a Databricks volume or a common code repository within Databricks.
2. Create a JSON configuration file specifying your source, target, and desired load strategy (refer to the Configuration section for details).
3. Place the configuration file into the /Config directory or your preferred configuration storage location.
4. Configure a job or task in Databricks (or your preferred orchestrator) to reference both the Utilities folder path and the configuration file.
5. Run the ingestion notebook or job; audit records will be generated automatically.

---

## Configuration Reference

All workflow logic is governed by a JSON config.

### Required Fields
- `source_details.table_name` — Fully qualified source table name or storage path.  
- `target_details.table_name` — Fully qualified target table name or storage path.  
- `target_details.column_list` — Array of columns to write to the target.  
- `target_details.key_column_list` — Array of key columns for upserts/matching.  
- `target_details.sequence_by_column_list` — Columns for ordering/deduplication (important for SCD2).  
- `target_details.operation_column` — Column indicating operation type (insert/update/delete).  
- `target_details.stored_as_type` — Storage format (e.g., `"delta"`, `"parquet"`).

### Optional Fields
- `source_details.except_column_list` — Columns to exclude from ingestion.  
- `source_details.transform_column_list` — Column-level transformations (expressions or function refs).  
- `source_details.where_clause` — SQL WHERE clause for source selection.  
- `source_details.watermark_column` — Column for incremental loads (timestamp/incrementing value).  
- `target_details.where_clause` — WHERE clause to scope target-side operations.  
- `target_details.watermark_column` — Watermark column used for incremental compare in target.  
- `target_details.track_history_column_list` — Columns to monitor for SCD2 history changes.  
- `target_details.track_history_except_column_list` — Columns to exclude from SCD2 tracking.  

---

## Example Configs

Sample configurations for different load strategies (**Append**, **Override**, **SCD1**, **SCD2**) are available in the [`/Config`](./Config) directory and can be used as templates.

---

## Auditing & Monitoring

Each run logs:
- Job/task metadata (user, workspace, job/run id, timestamps)  
- Source and target identifiers  
- DML stats (records inserted/updated/deleted)  
- Errors and stack traces (if any)

Audit records are designed to be stored queryably (e.g., Delta audit table) for dashboards and alerts.

---

## Contributing

Contributions welcome.