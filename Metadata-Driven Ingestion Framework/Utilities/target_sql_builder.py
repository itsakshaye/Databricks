# Databricks notebook source
import json
import time
from typing import List, Any, Optional, Tuple, Dict
import logging
import os
import uuid

logger = logging.getLogger('target.sql.builder')
logger.setLevel(logging.INFO)

class TargetSQLBuilder:
    def __init__(self):
        pass

    @staticmethod
    def get_append_trgt_sql_query(parsed_dict: dict) -> str:
        return f"""
        INSERT INTO {parsed_dict.get("_trgt_full_table_name")}
        (
          {parsed_dict.get("_trgt_merge_insert_columns")},
          `__ROW_CREATE_TS`
        )
        SELECT
          {parsed_dict.get("_trgt_merge_insert_val_columns")},
          TIMESTAMP('{parsed_dict.get("_curr_load_ts")}') AS __ROW_CREATE_TS
        FROM `{parsed_dict.get("_src_stage_temp_view_name")}` AS SRC
        """

    @staticmethod
    def get_truncate_trgt_sql_query(parsed_dict: dict) -> str:
        return f"""TRUNCATE TABLE {parsed_dict.get("_trgt_full_table_name")}"""

    @staticmethod
    def get_delete_trgt_sql_query(parsed_dict: dict) -> str:
        _space_exp = "            "
        if parsed_dict.get("_trgt_where_clause", "").strip() != "":
            return f"""
            DELETE FROM {parsed_dict.get("_trgt_full_table_name")}
            WHERE 1 = 1{_space_exp}{parsed_dict.get("_trgt_where_clause")}
            """
        else:
            return f"""
            SELECT 'Delete operation skipped: No data was removed from the target table (i.e. {parsed_dict.get("_trgt_full_table_name")}).' AS MSG
            """

    @staticmethod
    def get_merge_scd1_trgt_sql_query(parsed_dict: dict) -> str:
        return f"""
        MERGE INTO {parsed_dict.get("_trgt_full_table_name")} AS TRGT
        USING `{parsed_dict.get("_src_stage_temp_view_name")}` AS SRC
        ON TRGT.__MERGE_JOIN_HASH_KEY = SRC.__MERGE_JOIN_HASH_KEY
        AND 1 = 1

        WHEN MATCHED AND SRC.SRC_OPERATION_FLG IN( 'DELETE_HARD' ) THEN
        DELETE

        WHEN MATCHED AND SRC.SRC_OPERATION_FLG IN( 'CHG_UPDATE' ) THEN
        UPDATE
        SET
          {parsed_dict.get("_trgt_merge_update_columns")},
          TRGT.`__MERGE_UPDATE_TS` = TIMESTAMP('{parsed_dict.get("_curr_load_ts")}')

        WHEN NOT MATCHED BY TARGET THEN
        INSERT
        (
          {parsed_dict.get("_trgt_merge_insert_columns")},
          `__MERGE_CREATE_TS`,
          `__MERGE_UPDATE_TS`
        )
        VALUES
        (
          {parsed_dict.get("_trgt_merge_insert_val_columns")},
          TIMESTAMP('{parsed_dict.get("_curr_load_ts")}'),
          TIMESTAMP('{parsed_dict.get("_curr_load_ts")}')
        );
        """

    @staticmethod
    def get_merge_scd2_trgt_sql_query(parsed_dict: dict) -> str:
        return f"""
        MERGE INTO {parsed_dict.get("_trgt_full_table_name")} AS TRGT
        USING `{parsed_dict.get("_src_stage_temp_view_name")}` AS SRC
        ON TRGT.__MERGE_JOIN_HASH_KEY = SRC.__MERGE_JOIN_HASH_KEY_FINAL
        AND TRGT.__VRSN_CURR_FLG = 'Y'

        WHEN MATCHED AND SRC.MERGE_FLAG_ IN( 'SAME_DAY_DELETE', 'CHG_UPDATE_NEW', 'DELETE_SOFT' ) AND TRGT.__VRSN_CURR_FLG = 'Y' THEN
        UPDATE
        SET
          TRGT.__VRSN_END_DT = CASE WHEN TRGT.__VRSN_START_DT = CURRENT_DATE() THEN CURRENT_DATE() ELSE CURRENT_DATE()-1 END,
          TRGT.__VRSN_CURR_FLG = 'N',
          TRGT.__MERGE_UPDATE_TS = TIMESTAMP('{parsed_dict.get("_curr_load_ts")}')

        WHEN MATCHED AND SRC.MERGE_FLAG_ IN( 'SAME_DAY_UPDATE', 'CHG_UPDATE_OLD' ) AND TRGT.__VRSN_CURR_FLG = 'Y' THEN
        UPDATE
        SET
          {parsed_dict.get("_trgt_merge_update_columns")},
          TRGT.`__MERGE_UPDATE_TS` = TIMESTAMP('{parsed_dict.get("_curr_load_ts")}')

        WHEN NOT MATCHED BY TARGET THEN
        INSERT
        (
          {parsed_dict.get("_trgt_merge_insert_columns")},
          `__VRSN_START_DT`,
          `__VRSN_END_DT`,
          `__VRSN_CURR_FLG`,
          `__MERGE_CREATE_TS`,
          `__MERGE_UPDATE_TS`
        )
        VALUES
        (
          {parsed_dict.get("_trgt_merge_insert_val_columns")},
          CURRENT_DATE(),
          CASE WHEN SRC.MERGE_FLAG_ = 'DELETE_SOFT' THEN CURRENT_DATE() ELSE DATE('9999-12-31') END,
          CASE WHEN SRC.MERGE_FLAG_ = 'DELETE_SOFT' THEN 'N' ELSE 'Y' END,
          TIMESTAMP('{parsed_dict.get("_curr_load_ts")}'),
          TIMESTAMP('{parsed_dict.get("_curr_load_ts")}')
        );
        """