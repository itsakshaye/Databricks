import json
import time
from typing import List, Any, Optional, Tuple, Dict
import logging
import os
import uuid
from metadata_parser import *
from source_sql_builder import *
from target_sql_builder import *
from logger_configurator import *


class TargetWriter:
    def __init__(self, spark):
        self.spark = spark
        self._metadataParser = MetadataParser(spark)
        self._sourceSQLBuilder = SourceSQLBuilder()
        self._targetSQLBuilder = TargetSQLBuilder()
        self._loggerConfigurator = LoggerConfigurator()

    @staticmethod
    def get_trgt_query_execute_stats(result_df: str) -> Tuple[int, int, int, int]:
        merege_res = result_df.collect()[0].asDict()
        if merege_res:
            affected_rows = int(merege_res.get("num_affected_rows", 0))
            updated_rows = int(merege_res.get("num_updated_rows", 0))
            deleted_rows = int(merege_res.get("num_deleted_rows", 0))
            inserted_rows = int(merege_res.get("num_inserted_rows", 0))
        else:
            affected_rows = updated_rows = deleted_rows = inserted_rows = 0
        return affected_rows, updated_rows, deleted_rows, inserted_rows

    def execute_sql_query(self, meta_file: str, log_level: str = "INFO") -> Tuple[int, int, int, int, int]:
        source_row_count = affected_rows = updated_rows = deleted_rows = inserted_rows = 0
        logger = self._loggerConfigurator.get_logger(
            logger_name="targetwriter.execute_sql_query", log_level=log_level
        )
        meta_data = self._metadataParser.load_metadata(meta_file=meta_file, log_level=log_level)
        parsed_vars = self._metadataParser.parse_metadata(metadata=meta_data, log_level=log_level)
        src_stage_temp_view = parsed_vars.get("_src_stage_temp_view_name")
        trgt_full_table_name = parsed_vars.get("_trgt_full_table_name")
        trgt_watermark_col = parsed_vars.get("_trgt_watermark_col")
        trgt_watermark_query = parsed_vars.get("_trgt_watermark_max_query")

        if trgt_watermark_query is not None:
            max_watermark_value = self.spark.sql(trgt_watermark_query).collect()[0]["MAX_WATERMARK_VALUE"]
        else:
            max_watermark_value = ""

        if parsed_vars.get("_trgt_stored_as") == "SCD1":
            src_extract_sql = self._sourceSQLBuilder.get_scd1_src_sql_query(parsed_dict=parsed_vars)
            trgt_execute_sql = self._targetSQLBuilder.get_merge_scd1_trgt_sql_query(parsed_dict=parsed_vars)
            del_trunc_sql = ""
        elif parsed_vars.get("_trgt_stored_as") == "SCD2":
            src_extract_sql = self._sourceSQLBuilder.get_scd2_src_sql_query(parsed_dict=parsed_vars)
            trgt_execute_sql = self._targetSQLBuilder.get_merge_scd2_trgt_sql_query(parsed_dict=parsed_vars)
            del_trunc_sql = ""
        elif parsed_vars.get("_trgt_stored_as") == "APPEND":
            src_extract_sql = self._sourceSQLBuilder.get_src_select_sql_query(parsed_dict=parsed_vars)
            trgt_execute_sql = self._targetSQLBuilder.get_append_trgt_sql_query(parsed_dict=parsed_vars)
            del_trunc_sql = self._targetSQLBuilder.get_delete_trgt_sql_query(parsed_dict=parsed_vars)
        elif parsed_vars.get("_trgt_stored_as") == "OVERRIDE":
            src_extract_sql = self._sourceSQLBuilder.get_src_select_sql_query(parsed_dict=parsed_vars)
            trgt_execute_sql = self._targetSQLBuilder.get_append_trgt_sql_query(parsed_dict=parsed_vars)
            del_trunc_sql = self._targetSQLBuilder.get_truncate_trgt_sql_query(parsed_dict=parsed_vars)
        else:
            src_extract_sql = trgt_execute_sql = del_trunc_sql = ""
            logger.error("Invalid load strategy. Supported strategies are SCD1, SCD2, APPEND, and OVERRIDE.")
            raise ValueError("Invalid load strategy. Supported strategies are SCD1, SCD2, APPEND, or OVERRIDE.")

        src_extract_sql = str(src_extract_sql).replace(
            "@PARAM_VAR_TRGT_WATERMARK_VALUE", str(max_watermark_value)
        )
        trgt_execute_sql = str(trgt_execute_sql).replace(
            "@PARAM_VAR_TRGT_WATERMARK_VALUE", str(max_watermark_value)
        )
        del_trunc_sql = str(del_trunc_sql).replace(
            "@PARAM_VAR_TRGT_WATERMARK_VALUE", str(max_watermark_value)
        )

        logger.debug(f"Source extraction SQL query:\n{src_extract_sql}")
        logger.debug(f"Target execution SQL query:\n{trgt_execute_sql}")
        logger.debug(f"Target delete or truncate SQL query:\n{del_trunc_sql}")

        try:
            self.spark.sql(src_extract_sql).createOrReplaceTempView(src_stage_temp_view)
            source_row_count = self.spark.table(src_stage_temp_view).count()
            logger.info(f"Source extraction SQL executed successfully & temporary view '{src_stage_temp_view}' created.")
            logger.info(f"Rows in temporary view '{src_stage_temp_view}': {source_row_count}")

            if source_row_count > 0:
                try:
                    if parsed_vars.get("_trgt_stored_as") in ["APPEND", "OVERRIDE"]:
                        self.spark.sql(
                            del_trunc_sql.replace(
                                "@PARAM_VAR_TRGT_WATERMARK_VALUE",
                                str(max_watermark_value),
                            )
                        )
                        logger.info("Successfully truncated or deleted the target table data as per configuration.")

                    merge_result = self.spark.sql(trgt_execute_sql)
                    affected_rows, updated_rows, deleted_rows, inserted_rows = (
                        self.get_trgt_query_execute_stats(result_df=merge_result)
                    )
                    logger.info(
                        f"Merge operation counts — "
                        f"Affected: {affected_rows} "
                        f"Inserted: {inserted_rows} "
                        f"Updated: {updated_rows} "
                        f"Deleted: {deleted_rows}."
                    )
                except Exception as e:
                    affected_rows = updated_rows = deleted_rows = inserted_rows = 0
                    logger.error(f"Error occurred while performing the merge operation.: {e}")
                    raise
            else:
                logger.info("Merge operation was not performed because the temporary view contains no records.")
                affected_rows = updated_rows = deleted_rows = inserted_rows = 0

        except Exception as e:
            source_row_count = 0
            logger.error(f"Error occurred while creating the temporary view or executing the source SQL query.:{e}")
            raise
        finally:
            try:
                temp_view_table_list = [
                    view.name
                    for view in self.spark.catalog.listTables()
                    if view.tableType == "TEMPORARY"
                ]
                if src_stage_temp_view.lower() in temp_view_table_list:
                    self.spark.catalog.dropTempView(src_stage_temp_view)
                    logger.info(f"Temporary view '{src_stage_temp_view}' dropped successfully.")
                else:
                    logger.info(f"Temporary view '{src_stage_temp_view}' does not exist.")
            except Exception as e:
                logger.warning(f"Error occurred while dropping the temporary view '{src_stage_temp_view}'.: {e}")

        return (
            source_row_count,
            affected_rows,
            updated_rows,
            deleted_rows,
            inserted_rows,
        )