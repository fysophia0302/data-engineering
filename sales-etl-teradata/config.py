"""
config.py
=========
Centralised configuration for the Sales ETL pipeline.
All environment-specific values are read from environment variables
or set here as constants — never hard-coded credentials.
"""

import os
from datetime import date, timedelta

# ============================================================
# Credentials
# ============================================================
TD_USERNAME = os.environ.get("TD_USERNAME")
TD_HOST     = os.environ.get("TD_HOST", "your-teradata-host")

# ============================================================
# File Config
# ============================================================
FILE_NAME    = "SAMPLE_SALE_DATA.csv"
RECEIVED_DIR = r"C:\data\etl_pipeline\input"

# ============================================================
# Date Config
# ============================================================
LOAD_DATE = date.today()
TODAY     = date.today().strftime("%Y%m%d")
NEXT_DAY  = (date.today() + timedelta(days=1)).strftime("%Y%m%d")

# ============================================================
# Directory Config
# ============================================================
ARCHIVE_DIR = f"{RECEIVED_DIR}\\{TODAY}_archived"
OUTPUT_DIR  = f"{RECEIVED_DIR}\\{TODAY}_ready_to_load"
GCS_DIR     = f"{RECEIVED_DIR}\\gcs_{TODAY}"

# ============================================================
# Teradata Config
# ============================================================
TD_SCHEMA  = "STAGING_DB"
BATCH_SIZE = 1000

# ============================================================
# Column Mapping
# ============================================================
COLUMN_MAP = {
    "CORP_YR_NUM"         : "CORP_YR_NUM",
    "CORP_PD_NUM"         : "CORP_PD_NUM",
    "SRC"                 : "INPUT_SRC",
    "CO_CD"               : "CO_CD",
    "DIST_VEND_NUM"       : "DIST_VEND_NUM",
    "DIST_VEND_NM"        : "DIST_VEND_NM",
    "VEND_MFG_NUM"        : "VEND_MFG_NUM",
    "VEND_MFG_NM"         : "VEND_MFG_NM",
    "VEND_MFG_ITEM_NUMBER": "VEND_MFG_ITEM_NUM",
    "DIST_ITEM_NUMBER"    : "DIST_ITEM_NUM",
    "LCL_ARTCL_NUM"       : "LCL_ARTCL_NUM",
    "UPC"                 : "RTL_UPC_CD",
    "ARTCL_CAT"           : "ARTCL_CAT",
    "PRODUCT_DESC"        : "ARTCL_MED_DESC",
    "SITE"                : "SITE_NUM",
    "SHIP_BANNER"         : "BAN_NUM",
    "SHIP_REGION"         : "RGN_NUM",
    "SHIP_PROV"           : "SITE_PROV_CD",
    "WT_UOM"              : "WGT_UOM_CD",
    "SHIP_UOM"            : "SHIP_UOM_CD",
    "SHIP_UOM_DESC"       : "SHIP_UOM_DESC",
    "SHIP_INNER_UOM"      : "SHIP_INNER_UOM_CD",
    "GROSS_WT_SHIP_UOM"   : "SHIP_GROSS_WT",
    "NET_WT_SHIP_UOM"     : "SHIP_NET_WT",
    "SHIP_INNER_QTY"      : "SHIP_INNER_QTY",
    "SIZE_NUM_QTY"        : "SIZE_UOM_QTY",
    "SIZE_UOM"            : "SIZE_UOM_CD",
    "SHIP_QTY"            : "SHIP_QTY",
    "SHIP_SL_AMT"         : "SHIP_SL_AMT",
}

TABLE_COLUMNS = [
    "CORP_YR_NUM", "CORP_PD_NUM", "CO_CD", "INPUT_SRC",
    "DIST_VEND_NUM", "DIST_VEND_NM", "VEND_MFG_NUM", "VEND_MFG_NM",
    "VEND_MFG_ITEM_NUM", "DIST_ITEM_NUM", "LCL_ARTCL_NUM", "RTL_UPC_CD",
    "ARTCL_MED_DESC", "SITE_NUM", "BAN_NUM", "RGN_NUM", "SITE_PROV_CD",
    "WGT_UOM_CD", "SHIP_UOM_CD", "SHIP_UOM_DESC", "SHIP_INNER_UOM_CD",
    "SHIP_GROSS_WT", "SHIP_NET_WT", "SHIP_INNER_QTY", "SIZE_UOM_CD",
    "SIZE_UOM_QTY", "SHIP_QTY", "SHIP_SL_AMT", "FILE_LOAD_DT",
]

MAX_LEN = {
    "CO_CD": 3, "INPUT_SRC": 20, "DIST_VEND_NUM": 10,
    "DIST_VEND_NM": 40, "VEND_MFG_NUM": 40, "VEND_MFG_NM": 40,
    "VEND_MFG_ITEM_NUM": 40, "DIST_ITEM_NUM": 40, "LCL_ARTCL_NUM": 18,
    "RTL_UPC_CD": 40, "ARTCL_MED_DESC": 60, "SITE_NUM": 4,
    "BAN_NUM": 3, "RGN_NUM": 4, "SITE_PROV_CD": 2, "WGT_UOM_CD": 5,
    "SHIP_UOM_CD": 5, "SHIP_UOM_DESC": 40, "SHIP_INNER_UOM_CD": 5,
    "SIZE_UOM_CD": 5,
}
