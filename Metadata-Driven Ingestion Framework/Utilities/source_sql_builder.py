import json
import time
from typing import List, Any, Optional, Tuple, Dict
import logging
import os
import uuid

logger = logging.getLogger('source.sql.builder')
logger.setLevel(logging.INFO)

class SourceSQLBuilder:
    def __init__(self):
        pass

    @staticmethod
    def get_src_select_sql_query(parsed_dict: dict) -> str:
        return f"""
        WITH CTE_SOURCE_DELTA AS (
          SELECT
            {parsed_dict.get("_src_exp_columns")}
          FROM {parsed_dict.get("_src_full_table_name")}
          WHERE 1 = 1
          AND UPPER(`{parsed_dict.get("_src_operation_flag")}`) NOT IN ('DELETE', 'DEL', 'D') {parsed_dict.get("_src_watermark_col")}{parsed_dict.get("_src_where_clause")} 
          QUALIFY ROW_NUMBER() OVER ( PARTITION BY {parsed_dict.get("_src_partition_by_cols")} ORDER BY {parsed_dict.get("_src_order_by_sequence_cols")} ) = 1
        )
        SELECT * FROM CTE_SOURCE_DELTA
        """

    @staticmethod
    def get_scd1_src_sql_query(parsed_dict: dict) -> str:
        return f"""
        WITH CTE_SOURCE_DELTA AS (
          SELECT
            {parsed_dict.get("_src_exp_columns")},
            {parsed_dict.get("_src_join_key_cols_hash_key_expr")} AS __MERGE_JOIN_HASH_KEY,
            CASE 
              WHEN UPPER(`{parsed_dict.get("_src_operation_flag")}`) IN ('INSERT', 'INS', 'I', 'C', 'A') THEN 'INSERT'
              WHEN UPPER(`{parsed_dict.get("_src_operation_flag")}`) IN ('UPDATE', 'UPD', 'U', 'M') THEN 'CHG_UPDATE'
              WHEN UPPER(`{parsed_dict.get("_src_operation_flag")}`) IN ('DELETE', 'DEL', 'D') THEN 'DELETE_HARD'
              ELSE 'NULL'
            END AS SRC_OPERATION_FLG,
            {parsed_dict.get("_src_operation_flag", '')} AS SRC_OPERATION_FLG_ORIG
          FROM {parsed_dict.get("_src_full_table_name")}
          WHERE 1 = 1 {parsed_dict.get("_src_watermark_col")}{parsed_dict.get("_src_where_clause")} 
          QUALIFY ROW_NUMBER() OVER ( PARTITION BY {parsed_dict.get("_src_partition_by_cols")} ORDER BY {parsed_dict.get("_src_order_by_sequence_cols")} ) = 1
        )
        SELECT * FROM CTE_SOURCE_DELTA
        WHERE SRC_OPERATION_FLG NOT IN('NULL')
        """

    @staticmethod
    def get_scd2_src_sql_query(parsed_dict: dict) -> str:
        return f"""
        WITH CTE_SOURCE_DELTA AS (
          SELECT
            {parsed_dict.get("_src_exp_columns")},
            {parsed_dict.get("_src_join_key_cols_hash_key_expr")} AS __MERGE_JOIN_HASH_KEY,
            {parsed_dict.get("_src_hist_track_cols_hash_key_expr")} AS __MERGE_ROW_CHG_HIST_HASH_KEY,
            CASE 
              WHEN UPPER(`{parsed_dict.get("_src_operation_flag")}`) IN ('INSERT', 'INS', 'I', 'C', 'A') THEN 'INSERT'
              WHEN UPPER(`{parsed_dict.get("_src_operation_flag")}`) IN ('UPDATE', 'UPD', 'U', 'M') THEN 'CHG_UPDATE'
              WHEN UPPER(`{parsed_dict.get("_src_operation_flag")}`) IN ('DELETE', 'DEL', 'D') THEN 'DELETE_SOFT'
              ELSE 'NULL'
            END AS SRC_OPERATION_FLG,
            `{parsed_dict.get("_src_operation_flag")}` AS SRC_OPERATION_FLG_ORIG
          FROM {parsed_dict.get("_src_full_table_name")}
          WHERE 1 = 1 {parsed_dict.get("_src_watermark_col")}{parsed_dict.get("_src_where_clause")} 
          QUALIFY ROW_NUMBER() OVER ( PARTITION BY {parsed_dict.get("_src_partition_by_cols")} ORDER BY {parsed_dict.get("_src_order_by_sequence_cols")} ) = 1
        )
        , CTE_LATEST_ROWS_ONLY AS (
          SELECT
            __MERGE_JOIN_HASH_KEY,
            __MERGE_ROW_CHG_HIST_HASH_KEY,
            __VRSN_START_DT,
            __VRSN_CURR_FLG
          FROM {parsed_dict.get("_trgt_full_table_name")}
          QUALIFY ROW_NUMBER() OVER ( PARTITION BY `__MERGE_JOIN_HASH_KEY` ORDER BY `__VRSN_START_DT` DESC ) = 1
        )
        , CTE_SOURCE_DELTA_FINAL AS (
          SELECT
            DTL.* EXCEPT(SRC_OPERATION_FLG),
            CASE
              WHEN LATEST.__VRSN_START_DT IS NULL THEN ARRAY(DTL.SRC_OPERATION_FLG)
              WHEN LATEST.__VRSN_START_DT IS NOT NULL AND LATEST.__VRSN_START_DT = CURRENT_DATE() AND DTL.SRC_OPERATION_FLG = 'DELETE_SOFT' THEN ARRAY('SAME_DAY_DELETE') 
              WHEN LATEST.__VRSN_START_DT IS NOT NULL AND LATEST.__VRSN_START_DT = CURRENT_DATE() AND DTL.SRC_OPERATION_FLG = 'CHG_UPDATE' THEN ARRAY('SAME_DAY_UPDATE') 
              WHEN LATEST.__VRSN_START_DT IS NOT NULL AND LATEST.__VRSN_START_DT != CURRENT_DATE() AND DTL.SRC_OPERATION_FLG = 'INSERT' AND LATEST.__VRSN_CURR_FLG = 'N' THEN ARRAY('RE_INSERT')
              WHEN LATEST.__VRSN_START_DT IS NOT NULL AND DTL.SRC_OPERATION_FLG = 'DELETE_SOFT' THEN ARRAY('DELETE_SOFT')
              WHEN LATEST.__VRSN_START_DT IS NOT NULL AND DTL.__MERGE_ROW_CHG_HIST_HASH_KEY IS NOT DISTINCT FROM LATEST.__MERGE_ROW_CHG_HIST_HASH_KEY AND DTL.SRC_OPERATION_FLG = 'CHG_UPDATE' THEN ARRAY('CHG_UPDATE_OLD')
              WHEN LATEST.__VRSN_START_DT IS NOT NULL AND DTL.__MERGE_ROW_CHG_HIST_HASH_KEY IS DISTINCT FROM LATEST.__MERGE_ROW_CHG_HIST_HASH_KEY AND DTL.SRC_OPERATION_FLG = 'CHG_UPDATE' THEN ARRAY('CHG_UPDATE_NEW', 'CHG_INSERT')
              ELSE ARRAY('NULL')
            END AS SRC_OPERATION_FLG
          FROM CTE_SOURCE_DELTA AS DTL
          LEFT OUTER JOIN CTE_LATEST_ROWS_ONLY AS LATEST
          ON DTL.__MERGE_JOIN_HASH_KEY = LATEST.__MERGE_JOIN_HASH_KEY
        )
        SELECT
          CASE 
            WHEN MERGE_FLAG_ IN('RE_INSERT', 'CHG_INSERT') THEN NULL
            ELSE DTL.__MERGE_JOIN_HASH_KEY
          END AS __MERGE_JOIN_HASH_KEY_FINAL,
          DTL.__MERGE_JOIN_HASH_KEY AS __MERGE_JOIN_HASH_KEY,
          MERGE_FLAG_ AS MERGE_FLAG_,
          DTL.* EXCEPT(__MERGE_JOIN_HASH_KEY, SRC_OPERATION_FLG)
        FROM CTE_SOURCE_DELTA_FINAL AS DTL
        LATERAL VIEW OUTER EXPLODE(DTL.SRC_OPERATION_FLG) AS MERGE_FLAG_
        WHERE MERGE_FLAG_ NOT IN('NO_CHANGE', 'NULL')
        """