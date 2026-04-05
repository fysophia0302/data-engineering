"""
main.py
=======
Orchestrates the full retail ETL pipeline.

Phase 1 — extract_by_region:
    Pulls sales data from Teradata in parallel by region,
    writes Parquet to GCS, loads into BigQuery.

Phase 2 — product_flag_etl:
    Pulls product classification flags from Teradata,
    uploads to GCS, loads into BigQuery.

Usage:
    python main.py
"""

import logging

from stage1_extract_by_region.extract_by_region import main as run_extract_by_region
from stage2_product_flag.product_flag_etl import run_product_flag_etl

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(message)s',
)
log = logging.getLogger(__name__)


def main():
    log.info("=== ETL pipeline starting ===")

    log.info("--- Phase 1: regional sales extraction ---")
    run_extract_by_region()
    log.info("--- Phase 1 complete ---")

    log.info("--- Phase 2: product flag extraction ---")
    run_product_flag_etl()
    log.info("--- Phase 2 complete ---")

    log.info("=== ETL pipeline done ===")


if __name__ == "__main__":
    main()
