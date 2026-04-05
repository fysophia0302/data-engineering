"""
main.py
=======
Entry point for the Sales ETL pipeline.

Run:
    python main.py

Phases
------
1. Extract  — read CSV, scan and clean special characters, export GCS CSV
2. Transform — promote header, map columns, coerce types, truncate, split duplicates
3. Load     — export ready-to-load CSVs, bulk-insert into Teradata staging table
"""

import logging
import os
import shutil

import pandas as pd

from config import (
    FILE_NAME, FULL_PATH, RECEIVED_DIR,
    ARCHIVE_DIR, OUTPUT_DIR, GCS_DIR,
    LOAD_DATE, NEXT_DAY, TABLE_COLUMNS,
)
from cleaner import scan_special_chars, apply_cleaning
from transformer import (
    promote_header, rename_and_reorder,
    coerce_types, truncate_columns, split_clean_duplicates,
)
from loader import get_connection, create_staging_table, bulk_insert

# ============================================================
# Setup directories
# ============================================================
for d in [GCS_DIR, OUTPUT_DIR, ARCHIVE_DIR]:
    os.makedirs(d, exist_ok=True)

# ============================================================
# Logging
# ============================================================
FILE_STEM = os.path.splitext(FILE_NAME)[0]
TODAY_STR = LOAD_DATE.strftime("%Y%m%d")
LOG_DIR   = os.path.join(RECEIVED_DIR, "Logs")
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOG_DIR, f"{FILE_STEM}_{TODAY_STR}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

log.info("=" * 60)
log.info("ETL START")
log.info(f"Source file : {FULL_PATH}")
log.info(f"Load date   : {LOAD_DATE}")


# ============================================================
# PHASE 1 — Extract
# ============================================================
log.info("-" * 60)
log.info("PHASE 1 — Extract")

df_raw = pd.read_csv(FULL_PATH, dtype=str, header=None)
rows_raw_total = df_raw.shape[0]
rows_raw_data  = rows_raw_total - 1
log.info(f"Raw CSV shape : {rows_raw_total} rows × {df_raw.shape[1]} cols")

hits = scan_special_chars(df_raw)
if hits:
    log.info(f"Special characters found: {hits}")
else:
    log.info("No special characters found")

df_raw = apply_cleaning(df_raw)

original_name    = os.path.splitext(os.path.basename(FILE_NAME))[0]
gcs_csv_filename = f"incoming_SALE_{original_name}_{NEXT_DAY}.csv"
gcs_csv_path     = os.path.join(GCS_DIR, gcs_csv_filename)
df_raw.to_csv(gcs_csv_path, index=False, header=False, encoding="utf-8")
log.info(f"GCS CSV exported: {gcs_csv_path}")


# ============================================================
# PHASE 2 — Transform
# ============================================================
log.info("-" * 60)
log.info("PHASE 2 — Transform")

df = promote_header(df_raw.copy())
log.info(f"Rows after header promotion: {len(df)}")

df = rename_and_reorder(df, log)
df = coerce_types(df)
df = truncate_columns(df, log)

# Drop all-empty rows
rows_before_drop = len(df)
df.dropna(how="all", subset=[c for c in TABLE_COLUMNS if c != "FILE_LOAD_DT"], inplace=True)
rows_dropped = rows_before_drop - len(df)
log.info(f"Rows dropped (all-empty): {rows_dropped}")

clean_df, duplicates_df = split_clean_duplicates(df)
log.info(f"Clean rows     : {len(clean_df)}")
log.info(f"Duplicate rows : {len(duplicates_df)}")


# ============================================================
# PHASE 3 — Export CSVs + Archive
# ============================================================
log.info("-" * 60)
log.info("PHASE 3 — Export")

output_csv      = os.path.join(OUTPUT_DIR, f"{original_name}_ready.csv")
duplicate_csv   = os.path.join(OUTPUT_DIR, f"{original_name}_duplicate.csv")

clean_df.to_csv(output_csv,    index=False, encoding="utf-8-sig")
duplicates_df.to_csv(duplicate_csv, index=False, encoding="utf-8-sig")
log.info(f"Ready CSV exported     : {output_csv}")
log.info(f"Duplicate CSV exported : {duplicate_csv}")

shutil.copy2(FULL_PATH, os.path.join(ARCHIVE_DIR, FILE_NAME))
log.info(f"Original file archived : {ARCHIVE_DIR}")


# ============================================================
# PHASE 4 — Load to Teradata
# ============================================================
log.info("-" * 60)
log.info("PHASE 4 — Teradata Load")

table_name = gcs_csv_filename.replace(".csv", "").replace("-", "_")
log.info(f"Target table: STAGING_DB.{table_name}")

conn   = get_connection()
cursor = conn.cursor()

create_staging_table(cursor, table_name, log)
success, failed = bulk_insert(conn, cursor, clean_df, table_name, log)

cursor.close()
conn.close()


# ============================================================
# Summary
# ============================================================
log.info("=" * 60)
log.info("ETL SUMMARY")
log.info(f"  Source file          : {FILE_NAME}")
log.info(f"  Raw data rows        : {rows_raw_data}")
log.info(f"  Rows dropped (empty) : {rows_dropped}")
log.info(f"  Clean rows           : {len(clean_df)}")
log.info(f"  Duplicate rows       : {len(duplicates_df)}")
log.info(f"  Rows inserted        : {success}")
log.info(f"  Rows failed          : {failed}")
if success == len(clean_df):
    log.info("  ✅ All clean rows loaded successfully")
else:
    log.warning("  ⚠️  Row count mismatch — check failed batches above")
log.info(f"  Target table         : STAGING_DB.{table_name}")
log.info("ETL END")
log.info("=" * 60)
