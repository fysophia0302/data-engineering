"""
queries.py
==========
DDL and DML query templates for the retail ETL pipeline.
Table names and business logic are generalized for portfolio use.
"""

import pyarrow as pa

# ============================================================
# DDL — Volatile Table Definitions
# ============================================================

SALES_BASE = """
CREATE MULTISET VOLATILE TABLE sales_base AS (
    SELECT fiscal_yr, fiscal_pd, product_id, barcode,
           co_cd, supplier_id, brand_cd,
           category_0, category_3, uom_cd,
           sold_qty, sold_amt
    FROM source_db.sales_fact
    WHERE brand_cd IN ('1', '2')
      AND uom_cd <> 'KG'
) WITH DATA PRIMARY INDEX(product_id, barcode) ON COMMIT PRESERVE ROWS;
"""

WEIGHT_LOOKUP = """
CREATE MULTISET VOLATILE TABLE weight_lookup AS (
    SELECT barcode,
           material_type_id,
           AVG(pkg_weight) AS avg_weight
    FROM source_db.product_weight_fact
    WHERE product_category = 'GEN'
    GROUP BY 1, 2
) WITH DATA PRIMARY INDEX(barcode) ON COMMIT PRESERVE ROWS;
"""

WEIGHT_TRANSPOSED = """
CREATE MULTISET VOLATILE TABLE weight_transposed AS (
    SELECT *
    FROM weight_lookup
    PIVOT (SUM(avg_weight) AS wgt FOR material_type_id IN (
        'MAT_A' AS MAT_A, 'MAT_B' AS MAT_B, 'MAT_C' AS MAT_C,
        'MAT_D' AS MAT_D, 'MAT_E' AS MAT_E, 'MAT_F' AS MAT_F
    )) dt
) WITH DATA ON COMMIT PRESERVE ROWS;
"""

RATE_TABLE = """
CREATE MULTISET VOLATILE TABLE rate_table AS (
    SELECT region_cd, material_type_id, rate_amt
    FROM source_db.regional_rates
    WHERE load_dt = (SELECT MAX(load_dt) FROM source_db.regional_rates)
) WITH DATA PRIMARY INDEX(region_cd) ON COMMIT PRESERVE ROWS;
"""

DDL_TABLE_LIST = [
    SALES_BASE,
    WEIGHT_LOOKUP,
    WEIGHT_TRANSPOSED,
    RATE_TABLE,
]

# ============================================================
# DML — Final extraction query, parameterized by region
# ============================================================

DML_QUERY = """
SELECT
    s.fiscal_yr,
    s.fiscal_pd,
    s.co_cd,
    s.region_cd,
    s.product_id,
    s.barcode,
    s.supplier_id,
    s.brand_cd,
    s.category_0,
    s.category_3,
    s.uom_cd,
    s.sold_qty,
    s.sold_amt,
    f.deposit_flag,
    f.hazard_flag,
    f.ecology_flag,
    COALESCE(w.MAT_A_wgt, 0) * s.sold_qty             AS MAT_A_weight,
    COALESCE(w.MAT_B_wgt, 0) * s.sold_qty             AS MAT_B_weight,
    COALESCE(w.MAT_C_wgt, 0) * s.sold_qty             AS MAT_C_weight,
    COALESCE(w.MAT_A_wgt, 0) * s.sold_qty * r.MAT_A_rate AS MAT_A_cost,
    COALESCE(w.MAT_B_wgt, 0) * s.sold_qty * r.MAT_B_rate AS MAT_B_cost,
    COALESCE(w.MAT_C_wgt, 0) * s.sold_qty * r.MAT_C_rate AS MAT_C_cost
FROM sales_base s
LEFT JOIN weight_transposed w ON s.barcode    = w.barcode
LEFT JOIN rate_table r        ON s.region_cd  = r.region_cd
LEFT JOIN product_flags f     ON s.product_id = f.product_id
WHERE s.region_cd = '{region}'
"""

# ============================================================
# PyArrow Schema
# ============================================================

base_fields = [
    ("fiscal_yr",    pa.int16()),
    ("fiscal_pd",    pa.int16()),
    ("co_cd",        pa.string()),
    ("region_cd",    pa.string()),
    ("product_id",   pa.string()),
    ("barcode",      pa.string()),
    ("supplier_id",  pa.string()),
    ("brand_cd",     pa.string()),
    ("category_0",   pa.string()),
    ("category_3",   pa.string()),
    ("uom_cd",       pa.string()),
    ("sold_qty",     pa.decimal128(17, 5)),
    ("sold_amt",     pa.decimal128(17, 5)),
    ("deposit_flag", pa.string()),
    ("hazard_flag",  pa.string()),
    ("ecology_flag", pa.string()),
]

high_precision_fields = [
    "MAT_A_weight", "MAT_B_weight", "MAT_C_weight",
    "MAT_A_cost",   "MAT_B_cost",   "MAT_C_cost",
]

FINAL_SCHEMA = pa.schema(
    base_fields + [(name, pa.decimal128(38, 11)) for name in high_precision_fields]
)
