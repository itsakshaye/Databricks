import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any

from pyspark.sql.functions import from_json, schema_of_json, col

from logger_configurator import LoggerConfigurator


class AuditLogger:
    def __init__(self, spark, target_table: str, logger: logging.Logger = None):
        self.spark = spark
        self.target_table = target_table
        self._loggerConfigurator = LoggerConfigurator()

    def prepare_data(
        self,
        sys_metadata_json: str,
        stats_data: Dict[str, Any],
        log_level: str = "INFO",
    ) -> Dict[str, Any]:
        logger = self._loggerConfigurator.get_logger(
            "auditlogger.prepare_data", log_level
        )
        try:
            _task_sys_metadata = json.loads(sys_metadata_json)
        except json.JSONDecodeError as e:
            logger.error(f"Error occurred while parsing system metadata JSON: {e}")
            _task_sys_metadata = {}

        _final_data = {**_task_sys_metadata, **stats_data}
        _final_data_no_underscore = {k.lstrip("_"): v for k, v in _final_data.items()}

        timestamp_keys = [
            key
            for key in _final_data_no_underscore
            if key.lower().endswith("timestamp_ms")
        ]
        for col_name in timestamp_keys:
            ms_val = _final_data_no_underscore.get(col_name)
            try:
                if ms_val is not None:
                    ts = datetime.fromtimestamp(int(ms_val) / 1000, tz=timezone.utc)
                    new_col = col_name.replace("_ms", "")
                    _final_data_no_underscore[new_col] = ts
                del _final_data_no_underscore[col_name]
            except (ValueError, TypeError) as e:
                logger.warning(
                    f"Could not convert value '{ms_val}' for column '{col_name}' from milliseconds to timestamp: {e}"
                )
        return _final_data_no_underscore

    def audit_write(
        self,
        spark,
        sys_metadata_json: str,
        stats_data: Dict[str, Any],
        log_level: str = "INFO",
    ) -> None:
        logger = self._loggerConfigurator.get_logger(
            "auditlogger.audit_write", log_level
        )
        data = self.prepare_data(sys_metadata_json, stats_data, log_level)
        df = self.spark.createDataFrame([data])

        for json_col in ["error_result", "dml_statistics"]:
            if json_col in df.columns:
                json_val = df.select(json_col).first()[json_col]
                if json_val:
                    try:
                        json_schema = schema_of_json(json_val)
                        df = df.withColumn(
                            json_col, from_json(col(json_col), json_schema)
                        )
                    except Exception as e:
                        logger.warning(f"Failed to parse {json_col} JSON schema: {e}")

        try:
            df.write.mode("append").option("mergeSchema", "true").saveAsTable(
                self.target_table
            )
            logger.info(f"Successfully wrote job execution audit event data to {self.target_table}.")
        except Exception as e:
            logger.error(f"Failed to write job execution audit event data to {self.target_table}: {e}")