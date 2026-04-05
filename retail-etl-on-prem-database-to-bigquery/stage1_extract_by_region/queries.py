"""
queries.py
==========
DDL and DML templates for the sales data extraction pipeline.
Volatile tables are used to stage intermediate results within
each Teradata session before the final extraction.
"""

import pyarrow as pa

# ============================================================
# DDL — Volatile Table Definitions
# ============================================================

# core transaction table — filters to relevant order types
TXN_BASE = """
CREATE MULTISET VOLATILE TABLE txn_base AS (
    SELECT
        order_yr, order_pd, product_id, sku_cd,
        seller_id, channel_cd, region_cd,
        order_qty, order_amt, unit_price
    FROM source_db.order_transactions
    WHERE order_status = 'COMPLETED'
      AND channel_cd IN ('ONLINE', 'INSTORE')
) WITH DATA PRIMARY INDEX(product_id, sku_cd) ON COMMIT PRESERVE ROWS;
"""

# product lookup — joins product metadata
PRODUCT_LOOKUP = """
CREATE MULTISET VOLATILE TABLE product_lookup AS (
    SELECT
        p.product_id,
        p.sku_cd,
        p.category_cd,
        p.subcategory_cd,
        p.unit_weight,
        p.tax_rate,
        p.discount_rate
    FROM source_db.product_master p
    WHERE p.active_flag = 'Y'
) WITH DATA PRIMARY INDEX(product_id, sku_cd) ON COMMIT PRESERVE ROWS;
"""

# regional config — latest rate snapshot per region
REGION_CONFIG = """
CREATE MULTISET VOLATILE TABLE region_config AS (
    SELECT region_cd, shipping_rate, handling_fee
    FROM source_db.regional_config
    WHERE snapshot_dt = (SELECT MAX(snapshot_dt) FROM source_db.regional_config)
) WITH DATA PRIMARY INDEX(region_cd) ON COMMIT PRESERVE ROWS;
"""

DDL_TABLE_LIST = [
    TXN_BASE,
    PRODUCT_LOOKUP,
    REGION_CONFIG,
]

# ============================================================
# DML — Final extraction query, parameterized by region
# ============================================================

DML_QUERY = """
SELECT
    t.order_yr,
    t.order_pd,
    t.region_cd,
    t.seller_id,
    t.channel_cd,
    t.product_id,
    t.sku_cd,
    p.category_cd,
    p.subcategory_cd,
    t.order_qty,
    t.order_amt,
    t.unit_price,
    t.order_qty * p.unit_weight                        AS total_weight,
    t.order_amt * p.tax_rate                           AS tax_amt,
    t.order_amt * p.discount_rate                      AS discount_amt,
    t.order_qty * r.shipping_rate + r.handling_fee     AS shipping_cost
FROM txn_base t
LEFT JOIN product_lookup p ON t.product_id = p.product_id
                           AND t.sku_cd    = p.sku_cd
LEFT JOIN region_config r  ON t.region_cd  = r.region_cd
WHERE t.region_cd = '{region}'
"""

# ============================================================
# PyArrow Schema
# ============================================================

base_fields = [
    ("order_yr",       pa.int16()),
    ("order_pd",       pa.int16()),
    ("region_cd",      pa.string()),
    ("seller_id",      pa.string()),
    ("channel_cd",     pa.string()),
    ("product_id",     pa.string()),
    ("sku_cd",         pa.string()),
    ("category_cd",    pa.string()),
    ("subcategory_cd", pa.string()),
    ("order_qty",      pa.decimal128(17, 5)),
    ("order_amt",      pa.decimal128(17, 5)),
    ("unit_price",     pa.decimal128(17, 5)),
]

high_precision_fields = [
    "total_weight",
    "tax_amt",
    "discount_amt",
    "shipping_cost",
]

FINAL_SCHEMA = pa.schema(
    base_fields + [(name, pa.decimal128(38, 11)) for name in high_precision_fields]
)
