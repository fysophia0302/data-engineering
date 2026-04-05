"""
transformer.py
==============
Column mapping, type coercion, truncation, and duplicate detection.
"""

import numpy as np
import pandas as pd
from config import COLUMN_MAP, TABLE_COLUMNS, MAX_LEN, LOAD_DATE


def promote_header(df: pd.DataFrame) -> pd.DataFrame:
    """Promote row 0 to column names and drop it from the data."""
    df.columns = df.iloc[0]
    df = df[1:].reset_index(drop=True)
    df.columns = df.columns.str.strip()
    return df


def rename_and_reorder(df: pd.DataFrame, log) -> pd.DataFrame:
    """Apply column mapping, stamp FILE_LOAD_DT, reorder to match target table."""
    df = df.rename(columns=COLUMN_MAP)
    df["FILE_LOAD_DT"] = LOAD_DATE

    missing_cols = [c for c in TABLE_COLUMNS if c not in df.columns]
    if missing_cols:
        log.warning(f"Missing columns — will be filled with NULL: {missing_cols}")
        for c in missing_cols:
            df[c] = None
    else:
        log.info("All expected columns present")

    return df[TABLE_COLUMNS].copy()


def coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    """Apply type coercions and null handling to all columns."""

    def clean_decimal(col):
        return pd.to_numeric(col, errors='coerce').fillna(0).round(2)

    def clean_int(col):
        return pd.to_numeric(col, errors='coerce').fillna(0).astype(int)

    def empty_to_null(col):
        return col.replace(r'^\s*$', np.nan, regex=True)

    # Nullable string columns
    df['SIZE_UOM_CD']       = empty_to_null(df['SIZE_UOM_CD'])
    df['SHIP_INNER_UOM_CD'] = empty_to_null(df['SHIP_INNER_UOM_CD'])

    # Decimal columns
    for col in ['SHIP_GROSS_WT', 'SHIP_NET_WT', 'SHIP_QTY', 'SHIP_SL_AMT']:
        df[col] = clean_decimal(df[col])

    # Integer columns
    for col in ['SIZE_UOM_QTY', 'SHIP_INNER_QTY']:
        df[col] = clean_int(df[col])

    # String columns — strip whitespace and sentinel strings
    string_cols = [
        "CORP_YR_NUM", "CORP_PD_NUM", "CO_CD", "INPUT_SRC",
        "DIST_VEND_NUM", "DIST_VEND_NM", "VEND_MFG_NUM", "VEND_MFG_NM",
        "VEND_MFG_ITEM_NUM", "DIST_ITEM_NUM", "LCL_ARTCL_NUM",
        "RTL_UPC_CD", "ARTCL_MED_DESC", "SITE_NUM", "BAN_NUM", "RGN_NUM",
        "SITE_PROV_CD", "WGT_UOM_CD", "SHIP_UOM_CD", "SHIP_UOM_DESC",
        "SHIP_INNER_UOM_CD", "SIZE_UOM_CD",
    ]
    for col in string_cols:
        df[col] = df[col].astype(str).str.strip().replace(['nan', 'None', 'NULL'], '')

    # Normalise vendor name and description
    df["VEND_MFG_NM"]    = df["VEND_MFG_NM"].str.strip().str.title()
    df["ARTCL_MED_DESC"] = df["ARTCL_MED_DESC"].str.strip()

    return df


def truncate_columns(df: pd.DataFrame, log) -> pd.DataFrame:
    """Truncate VARCHAR columns to their maximum defined lengths."""
    for col, max_len in MAX_LEN.items():
        if col in df.columns:
            too_long = df[col].astype(str).str.len() > max_len
            count = too_long.sum()
            if count > 0:
                log.warning(f"{col}: {count} values truncated to {max_len} chars")
            df[col] = df[col].astype(str).str.slice(0, max_len)
    return df


def split_clean_duplicates(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split dataframe into unique rows and duplicate rows."""
    duplicates_mask = df.duplicated(keep=False)
    clean_df        = df.drop_duplicates(keep='first')
    duplicates_df   = df[duplicates_mask]
    return clean_df, duplicates_df
