"""
loader.py
=========
Teradata staging table creation and bulk insert.
"""

import pandas as pd
import teradatasql
from config import TD_HOST, TD_USERNAME, TD_SCHEMA, BATCH_SIZE
import keyring


def get_connection():
    """Establish a Teradata connection using keyring-stored credentials."""
    password = keyring.get_password("teradata_dev", TD_USERNAME)
    return teradatasql.connect(host=TD_HOST, user=TD_USERNAME, password=password)


def create_staging_table(cursor, table_name: str, log):
    """Drop (if exists) and recreate the staging table."""
    try:
        cursor.execute(f"DROP TABLE {TD_SCHEMA}.{table_name}")
        log.info(f"Dropped existing table: {TD_SCHEMA}.{table_name}")
    except Exception:
        log.info("Table did not exist — skipping DROP")

    create_sql = f"""
    CREATE MULTISET TABLE {TD_SCHEMA}.{table_name} ,FALLBACK,
         NO BEFORE JOURNAL, NO AFTER JOURNAL,
         CHECKSUM = DEFAULT, DEFAULT MERGEBLOCKRATIO
    (
          ORDER_YR           VARCHAR(20)  CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
          ORDER_MTH          VARCHAR(20)  CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
          CO_CD              VARCHAR(10)  CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
          INPUT_SRC          VARCHAR(20)  CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
          SELLER_ID          VARCHAR(40)  CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
          SELLER_NM          VARCHAR(40)  CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
          BRAND_ID           VARCHAR(40)  CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
          BRAND_NM           VARCHAR(40)  CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
          BRAND_ITEM_NUM     VARCHAR(40)  CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
          DIST_ITEM_NUM      VARCHAR(40)  CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
          PRODUCT_NUM        VARCHAR(40)  CHARACTER SET LATIN NOT CASESPECIFIC,
          SKU_CD             VARCHAR(40)  CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
          ITEM_DESC          VARCHAR(60)  CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
          WAREHOUSE_ID       VARCHAR(10)  CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
          CHANNEL_CD         VARCHAR(10)  CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
          COUNTRY_CD         VARCHAR(10)  CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
          STATE_CD           VARCHAR(10)  CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
          WGT_UOM_CD         VARCHAR(10)  CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
          SHIP_UOM_CD        VARCHAR(10)  CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
          SHIP_UOM_DESC      VARCHAR(40)  CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
          SHIP_INNER_UOM_CD  VARCHAR(10)  CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
          SHIP_GROSS_WT      VARCHAR(40)  CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
          SHIP_NET_WT        VARCHAR(40)  CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
          SHIP_INNER_QTY     VARCHAR(40)  CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
          SIZE_UOM_CD        VARCHAR(10)  CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
          SIZE_UOM_QTY       VARCHAR(10)  CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
          ORDER_QTY          VARCHAR(10)  CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
          ORDER_AMT          VARCHAR(10)  CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
          FILE_LOAD_DT       DATE FORMAT 'YYYY-MM-DD' NOT NULL
    )
    PRIMARY INDEX ( ORDER_YR, ORDER_MTH, SELLER_ID, SKU_CD );
    """
    cursor.execute(create_sql)
    log.info(f"Table created: {TD_SCHEMA}.{table_name}")


def sanitize_row(row) -> tuple:
    """Convert pandas-specific types to plain Python types for the Teradata driver."""
    result = []
    for val in row:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            result.append('')
        elif isinstance(val, pd.Timestamp):
            result.append(val.date())
        elif hasattr(val, 'item'):
            result.append(val.item())
        else:
            v = str(val).strip()
            result.append(v if v.lower() != 'nan' else '')
    return tuple(result)


def bulk_insert(conn, cursor, clean_df: pd.DataFrame, table_name: str, log) -> tuple[int, int]:
    """Batch-insert clean rows into the staging table. Returns (success, failed) counts."""
    data_tuples  = [sanitize_row(row) for row in clean_df.itertuples(index=False, name=None)]
    cols         = ", ".join(clean_df.columns.tolist())
    placeholders = ", ".join(["?"] * len(clean_df.columns))
    insert_sql   = f"INSERT INTO {TD_SCHEMA}.{table_name} ({cols}) VALUES ({placeholders})"

    total   = len(data_tuples)
    success = 0
    failed  = 0

    log.info(f"Starting insert — {total} rows in batches of {BATCH_SIZE}")

    for i in range(0, total, BATCH_SIZE):
        batch     = data_tuples[i:i + BATCH_SIZE]
        batch_end = min(i + BATCH_SIZE, total)
        try:
            cursor.executemany(insert_sql, batch)
            conn.commit()
            success += len(batch)
            log.info(f"  ✔ Inserted rows {i+1}–{batch_end} / {total}")
        except Exception as e:
            failed += len(batch)
            log.error(f"  ✘ Batch {i+1}–{batch_end} FAILED: {e}")
            conn.rollback()

    return success, failed
