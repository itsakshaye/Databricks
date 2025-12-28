import json
import time
from typing import List, Any, Optional, Tuple, Dict
import logging
import os
import uuid
from logger_configurator import *


class GenericUtils:
    @staticmethod
    def generate_hash_expression(columns: List[str] | str, delimiter: str = ",") -> str:
        if isinstance(columns, str):
            columns_ = [
                col.strip() for col in columns.split(",") if col and col.strip()
            ]
        elif isinstance(columns, list):
            columns_ = [col for col in columns if col and col.strip()]
        else:
            raise ValueError(
                "Invalid input type for 'columns'. Expected a list or a string."
            )

        coalesce_cast = [
            f"COALESCE(CAST({col} AS STRING), '<NULLS>')" for col in columns_
        ]
        concat_expr = (
            f"CONCAT('{delimiter}', "
            f"{f', {delimiter}, '.join(coalesce_cast)}, "
            f"'{delimiter}')"
        )
        return f"HASH({concat_expr})"

    @staticmethod
    def array_to_comma_separated(
        array: List[Any],
        delimiter: str = ",",
        quote_char: str = "'",
        max_items: int = 10000,
    ) -> str:
        unique_arr = list({str(item).strip() for item in array})
        sliced_arr = unique_arr[:max_items]
        quoted_arr = [f"{quote_char}{col}{quote_char}" for col in sliced_arr]
        return f"{delimiter} ".join(quoted_arr)

    @staticmethod
    def get_flatten_json_key(json_config: dict, parent_key: str = "") -> list:
        if json_config is None:
            return []
        _all_keys = []
        for key, val in json_config.items():
            _new_key = f"{parent_key}.{key}" if parent_key else key
            if isinstance(val, dict):
                _all_keys += GenericUtils.get_flatten_json_key(
                    json_config=val, parent_key=_new_key
                )
            else:
                _all_keys.append(_new_key)
        return _all_keys


class MetadataParser:
    def __init__(self, spark):
        self.spark = spark
        self._space_exp = "          "
        self._loggerConfigurator = LoggerConfigurator()
        self._required = [
            "source_details.table_name",
            "target_details.table_name",
            "target_details.column_list",
            "target_details.key_column_list",
            "target_details.sequence_by_column_list",
            "target_details.operation_column",
            "target_details.stored_as_type",
        ]
        self._optional = [
            "source_details.except_column_list",
            "source_details.transform_column_list",
            "source_details.where_clause",
            "source_details.watermark_column",
            "target_details.where_clause",
            "target_details.watermark_column",
            "target_details.track_history_column_list",
            "target_details.track_history_except_column_list",
            "target_details.apply_as_soft_delete"
        ]

    def validate_metadata_config(
        self, json_config: dict, log_level: str = "INFO"
    ) -> None:
        logger = self._loggerConfigurator.get_logger(
            logger_name="metadataparser.validate_metadata_config", log_level=log_level
        )
        data_keys = GenericUtils.get_flatten_json_key(
            json_config=json_config, parent_key=""
        )
        missing_required = [
            required_field
            for required_field in self._required
            if required_field not in data_keys
        ]
        unknown_fields = [
            data_key
            for data_key in data_keys
            if data_key not in (self._required + self._optional)
        ]

        if missing_required:
            logger.error(
                f"In JSON metadata, missing required fields: {missing_required}"
            )
            raise ValueError(f"In JSON metadata, missing required fields: {missing_required}")
        if unknown_fields:
            logger.warning(f"Found unknown fields in JSON metadata: {unknown_fields}")
        logger.info("JSON metadata config validation passed successfully.")

    def load_metadata(self, meta_file: str, log_level: str = "INFO") -> Dict:
        logger = self._loggerConfigurator.get_logger(
            logger_name="metadataparser.load_metadata", log_level=log_level
        )
        if not os.path.isfile(meta_file):
            logger.error(f"Metadata JSON file '{meta_file}' is missing or unavailable.")
            raise FileNotFoundError(
                f"Metadata JSON file '{meta_file}' is missing or unavailable."
            )
        try:
            with open(meta_file, "r") as conf_file:
                metadata = json.load(conf_file)
            logger.info(f"Metadata JSON file '{meta_file}' loaded successfully.")
            logger.debug(f"Metadata JSON Content:\n{metadata}")
            self.validate_metadata_config(json_config=metadata, log_level=log_level)
            return metadata
        except json.JSONDecodeError as e:
            logger.error(
                f"Failed to decode metadata JSON file '{meta_file}'. Please check that file contains valid JSON: {e}"
            )
            raise

    def get_table_columns_excluding(
        self, table_name: str, except_columns: list
    ) -> list:
        columns = self.spark.catalog.listColumns(table_name)
        return [col.name for col in columns if col.name not in except_columns]

    def get_stage_temp_view_name(self, meta_dict: dict) -> str:
        return "temp_view_{}_{}".format(
            str(meta_dict.get("table_name")).replace(".", "_"), str(uuid.uuid4().hex)
        ).lower()

    def get_src_exp_columns(self, meta_dict: dict) -> str:
        except_column_list = meta_dict.get("except_column_list", [])
        transform_column_list = meta_dict.get("transform_column_list", [])

        if len(except_column_list) > 0:
            except_cols_str = GenericUtils.array_to_comma_separated(except_column_list, ',', '`')
            except_column_list_str = f"* EXCEPT ({except_cols_str}),\n"
        else:
            except_column_list_str = f"*,\n"

        if len(transform_column_list) > 0:
            transform_column_list_str = "{}".format(
                ",\n".join(
                    [
                        f"{self._space_exp}  {col_exp}"
                        for col_exp in transform_column_list
                    ]
                ).lstrip()
            )
        else:
            transform_column_list_str = ""

        return f"{except_column_list_str}{self._space_exp}  {transform_column_list_str}"

    def get_src_trgt_where_clause(self, meta_dict: dict) -> str:
        where_clause = str(meta_dict.get("where_clause")).strip()
        if where_clause not in ["", "None"]:
            return f"\n{self._space_exp}AND {where_clause}"
        else:
            return ""

    def get_src_watermark_col(
        self, meta_dict: dict, load_ts: str, date_part: str = "TIMESTAMP"
    ) -> str:
        watermark_col = str(meta_dict.get("watermark_column")).strip()
        if watermark_col not in ["", "None"]:
            return (
                f"\n{self._space_exp}AND {watermark_col} >= {date_part}('@PARAM_VAR_TRGT_WATERMARK_VALUE')"
                f"\n{self._space_exp}AND {watermark_col} <= {date_part}('{load_ts}')"
            )
        else:
            return ""

    def get_src_order_by_sequence_cols(self, meta_dict: dict) -> str:
        sequence_by = meta_dict.get("sequence_by_column_list", [])
        if len(sequence_by) > 0:
            return ", ".join([f"`{seq_col}` DESC" for seq_col in sequence_by])
        else:
            return "NULL DESC"

    def get_merge_select_update_cols(self, meta_dict: dict) -> Tuple[str, str, str]:
        target_columns = meta_dict.get("column_list", [])
        target_stored_as = meta_dict.get("stored_as_type","")
        if target_stored_as in [ "SCD2"]:
            target_columns += ["__MERGE_JOIN_HASH_KEY", "__MERGE_ROW_CHG_HIST_HASH_KEY"]
        elif target_stored_as in ["SCD1"]:
            target_columns += ["__MERGE_JOIN_HASH_KEY"]
                
        exclude_columns = [
            "__MERGE_CREATE_TS",
            "__MERGE_JOIN_HASH_KEY",
        ] + meta_dict.get("key_column_list", [])
        merge_insert_columns = ",\n".join(
            f"{self._space_exp}`{col}`" for col in target_columns
        ).lstrip()
        merge_insert_val_columns = ",\n".join(
            f"{self._space_exp}SRC.`{col}`" for col in target_columns
        ).lstrip()
        merge_update_columns = ",\n".join(
            f"{self._space_exp}TRGT.`{col}` = SRC.`{col}`"
            for col in target_columns
            if col not in exclude_columns
        ).lstrip()
        return merge_insert_columns, merge_insert_val_columns, merge_update_columns

    def get_trgt_watermark_max_query(
        self, table_name: str, watermark_col: str, date_part: str = "TIMESTAMP"
    ) -> str | None:
        if watermark_col not in ["", "None"]:
            return f"""
                SELECT
                    COALESCE({date_part}(MAX({watermark_col})), {date_part}('1900-12-31')) AS MAX_WATERMARK_VALUE
                FROM {table_name}
                WHERE 1 = 1
                """
        else:
            return None

    def parse_metadata(self, metadata: Dict, log_level: str = "INFO") -> Dict:
        logger = self._loggerConfigurator.get_logger(
            logger_name="metadataparser.parse_metadata", log_level=log_level
        )
        try:
            parsed_dict = {}
            src_details = dict(metadata.get("source_details", {}))
            trgt_details = dict(metadata.get("target_details", {}))
            curr_time = time.strftime(
                "%Y-%m-%d %H:%M:%S", time.localtime(time.time() - 10)
            )
            parsed_dict["_curr_load_ts"] = curr_time
            parsed_dict["_src_full_table_name"] = src_details.get("table_name", "")
            parsed_dict["_src_exp_columns"] = self.get_src_exp_columns(src_details)
            parsed_dict["_src_where_clause"] = self.get_src_trgt_where_clause(src_details)
            parsed_dict["_src_watermark_col"] = self.get_src_watermark_col(src_details, curr_time)
            parsed_dict["_src_operation_flag"] = trgt_details.get("operation_column", "")

            parsed_dict["_src_partition_by_cols"] = (
                GenericUtils.array_to_comma_separated(
                    trgt_details.get("key_column_list", []), ",", "`"
                )
            )
            parsed_dict["_src_join_key_cols_hash_key_expr"] = (
                GenericUtils.generate_hash_expression(
                    trgt_details.get("key_column_list", []), "|"
                )
            )

            _src_tbl_name = src_details.get("table_name", "")
            _src_history_track_cols = trgt_details.get("track_history_column_list", [])
            _src_history_track_except_cols = trgt_details.get("track_history_except_column_list", [])

            if len(_src_history_track_cols) == 0:
                _src_history_track_cols_final = self.get_table_columns_excluding(
                    table_name=_src_tbl_name,
                    except_columns=_src_history_track_except_cols,
                )
            else:
                _src_history_track_cols_final = _src_history_track_cols

            parsed_dict["_src_hist_track_cols_hash_key_expr"] = (
                GenericUtils.generate_hash_expression(
                    _src_history_track_cols_final, "|"
                )
            )
            parsed_dict["_src_order_by_sequence_cols"] = (
                self.get_src_order_by_sequence_cols(trgt_details)
            )
            parsed_dict["_src_stage_temp_view_name"] = self.get_stage_temp_view_name(
                src_details
            )
            parsed_dict["_trgt_full_table_name"] = trgt_details.get("table_name", "")
            parsed_dict["_trgt_stored_as"] = trgt_details.get("stored_as_type", "")
            parsed_dict["_trgt_watermark_col"] = trgt_details.get("watermark_column", None)
            parsed_dict["_trgt_where_clause"] = self.get_src_trgt_where_clause(trgt_details)
            (
                parsed_dict["_trgt_merge_insert_columns"],
                parsed_dict["_trgt_merge_insert_val_columns"],
                parsed_dict["_trgt_merge_update_columns"],
            ) = self.get_merge_select_update_cols(trgt_details)
            parsed_dict["_trgt_watermark_max_query"] = (
                self.get_trgt_watermark_max_query(
                    trgt_details.get("table_name", ""),
                    str(trgt_details.get("watermark_column")).strip()
                )
            )
            logger.info(f"JSON metadata parsed successfully.")
            logger.debug(f"Parse JSON metadata:\n{parsed_dict}")
            return parsed_dict
        except Exception as e:
            logger.error(f"Error occurred while parsing the metadata:\n{e}")
            raise