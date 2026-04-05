"""
config.py
=========
Centralised configuration for the e-commerce order ETL pipeline.
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
FILE_NAME    = "SAMPLE_ORDER_DATA.csv"
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
    "ORDER_YR"           : "ORDER_YR",
    "ORDER_MTH"          : "ORDER_MTH",
    "SRC"                : "INPUT_SRC",
    "CO_CD"              : "CO_CD",
    "SELLER_ID"          : "SELLER_ID",
    "SELLER_NM"          : "SELLER_NM",
    "BRAND_ID"           : "BRAND_ID",
    "BRAND_NM"           : "BRAND_NM",
    "BRAND_ITEM_NUMBER"  : "BRAND_ITEM_NUM",
    "DIST_ITEM_NUMBER"   : "DIST_ITEM_NUM",
    "PRODUCT_NUM"        : "PRODUCT_NUM",
    "SKU_CD"             : "SKU_CD",
    "PRODUCT_CAT"        : "PRODUCT_CAT",
    "ITEM_DESC"          : "ITEM_DESC",
    "WAREHOUSE_ID"       : "WAREHOUSE_ID",
    "CHANNEL_CD"         : "CHANNEL_CD",
    "COUNTRY_CD"         : "COUNTRY_CD",
    "STATE_CD"           : "STATE_CD",
    "WT_UOM"             : "WGT_UOM_CD",
    "SHIP_UOM"           : "SHIP_UOM_CD",
    "SHIP_UOM_DESC"      : "SHIP_UOM_DESC",
    "SHIP_INNER_UOM"     : "SHIP_INNER_UOM_CD",
    "GROSS_WT"           : "SHIP_GROSS_WT",
    "NET_WT"             : "SHIP_NET_WT",
    "SHIP_INNER_QTY"     : "SHIP_INNER_QTY",
    "SIZE_NUM_QTY"       : "SIZE_UOM_QTY",
    "SIZE_UOM"           : "SIZE_UOM_CD",
    "ORDER_QTY"          : "ORDER_QTY",
    "ORDER_AMT"          : "ORDER_AMT",
}

TABLE_COLUMNS = [
    "ORDER_YR", "ORDER_MTH", "CO_CD", "INPUT_SRC",
    "SELLER_ID", "SELLER_NM", "BRAND_ID", "BRAND_NM",
    "BRAND_ITEM_NUM", "DIST_ITEM_NUM", "PRODUCT_NUM", "SKU_CD",
    "ITEM_DESC", "WAREHOUSE_ID", "CHANNEL_CD", "COUNTRY_CD", "STATE_CD",
    "WGT_UOM_CD", "SHIP_UOM_CD", "SHIP_UOM_DESC", "SHIP_INNER_UOM_CD",
    "SHIP_GROSS_WT", "SHIP_NET_WT", "SHIP_INNER_QTY", "SIZE_UOM_CD",
    "SIZE_UOM_QTY", "ORDER_QTY", "ORDER_AMT", "FILE_LOAD_DT",
]

MAX_LEN = {
    "CO_CD": 3, "INPUT_SRC": 20, "SELLER_ID": 10,
    "SELLER_NM": 40, "BRAND_ID": 40, "BRAND_NM": 40,
    "BRAND_ITEM_NUM": 40, "DIST_ITEM_NUM": 40, "PRODUCT_NUM": 18,
    "SKU_CD": 40, "ITEM_DESC": 60, "WAREHOUSE_ID": 4,
    "CHANNEL_CD": 3, "COUNTRY_CD": 4, "STATE_CD": 2, "WGT_UOM_CD": 5,
    "SHIP_UOM_CD": 5, "SHIP_UOM_DESC": 40, "SHIP_INNER_UOM_CD": 5,
    "SIZE_UOM_CD": 5,
}
