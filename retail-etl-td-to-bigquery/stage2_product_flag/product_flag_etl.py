"""
product_flag_etl.py
===================
Pulls a product classification table from Teradata,
writes it to Parquet, uploads to GCS, then loads into BigQuery.

This is a one-shot load — runs once per refresh cycle and
truncates the target table each time.
"""

import json
import logging
import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import bigquery, secretmanager, storage
from teradatasql import connect

log = logging.getLogger(__name__)

# pull from env — nothing sensitive in code
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCS_BUCKET     = os.environ.get("GCS_BUCKET", "retail-etl-dev")
BQ_DATASET     = os.environ.get("BQ_DATASET", "retail_etl_dev")
TD_SECRET_ID   = os.environ.get("TD_SECRET_ID", "teradata-credential-secret")
TMP_DIR        = r"tmp/product_flag"
GCS_PATH       = "product_flag"
BQ_TABLE       = "product_flag"
PARQUET_NAME   = "product_flag.parquet"


def get_td_secret() -> dict:
    """Fetch Teradata credentials from Secret Manager at runtime."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{GCP_PROJECT_ID}/secrets/{TD_SECRET_ID}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return json.loads(response.payload.data.decode("UTF-8"))


# query is generalized — real table names omitted
QUERY = """
SELECT
    p.product_id,
    p.product_desc,
    h.category_0_cd,
    h.category_0_desc,
    h.subcategory_cd,
    h.subcategory_desc,
    p.net_weight_qty,
    'Y' AS flag_ind
FROM source_db.product_master p
INNER JOIN source_db.product_hierarchy h
    ON p.product_id = h.product_id
WHERE p.brand_type_cd = '1'
  AND h.category_0_cd IN ('CAT001', 'CAT002', 'CAT003')
"""

SCHEMA = pa.schema([
    ("product_id",       pa.string()),
    ("product_desc",     pa.string()),
    ("category_0_cd",    pa.string()),
    ("category_0_desc",  pa.string()),
    ("subcategory_cd",   pa.string()),
    ("subcategory_desc", pa.string()),
    ("net_weight_qty",   pa.decimal128(17, 3)),
    ("flag_ind",         pa.string()),
])


def extract_td_to_gcs_parquet(query, bucket_name, gcs_blob_path) -> int:
    """
    Run the query against Teradata, stream results into a local Parquet file
    in chunks, then push the file to GCS.
    Returns the total row count.
    """
    td_credential = get_td_secret()
    os.makedirs(TMP_DIR, exist_ok=True)
    local_file = os.path.join(TMP_DIR, PARQUET_NAME)
    chunk_size = 100000
    writer     = None

    try:
        with connect(
            host=td_credential["host"],
            user=td_credential["username"],
            password=td_credential["password"],
            logmech=td_credential["logmech"],
        ) as conn:
            log.info("connected to Teradata")
            cur = conn.cursor()
            cur.execute(query)
            col_names = [desc[0] for desc in cur.description]

            writer     = pq.ParquetWriter(local_file, schema=SCHEMA, compression='snappy')
            total_rows = 0

            while True:
                rows = cur.fetchmany(chunk_size)
                if not rows:
                    break
                df_chunk = pd.DataFrame(rows, columns=col_names)
                writer.write_table(pa.Table.from_pandas(df_chunk, schema=SCHEMA))
                total_rows += len(rows)
                log.info(f"{total_rows} rows written so far...")

            cur.close()

        if writer:
            writer.close()
            writer = None

        if total_rows > 0:
            log.info(f"extraction done ({total_rows} rows) — uploading to GCS")
            upload_to_gcs(local_file, bucket_name, gcs_blob_path)
            log.info(f"uploaded to gs://{bucket_name}/{gcs_blob_path}")

        return total_rows

    except Exception as e:
        log.error(f"extraction failed: {e}")
        raise

    finally:
        if writer:
            try:
                writer.close()
            except Exception:
                pass
        if os.path.exists(local_file):
            try:
                os.remove(local_file)
            except Exception as e:
                log.warning(f"could not remove temp file {local_file}: {e}")


def upload_to_gcs(local_file: str, bucket_name: str, gcs_path: str):
    """Push a local file up to GCS."""
    client = storage.Client()
    client.bucket(bucket_name).blob(gcs_path).upload_from_filename(local_file)


def load_gcs_to_bq(gcs_uri: str, table_id: str, write_mode: str = "WRITE_TRUNCATE"):
    """Load a Parquet file from GCS into BigQuery, truncating the table each run."""
    client     = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=write_mode,
        autodetect=True,
    )
    log.info(f"starting BQ load: {gcs_uri} → {table_id}")
    load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    load_job.result()
    log.info(f"load complete — {client.get_table(table_id).num_rows} rows in table")


def run_product_flag_etl():
    """Entry point — extract from Teradata, upload to GCS, load into BigQuery."""
    full_table_id  = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    gcs_blob_path  = f"{GCS_PATH}/{PARQUET_NAME}"
    gcs_uri        = f"gs://{GCS_BUCKET}/{gcs_blob_path}"

    row_count = extract_td_to_gcs_parquet(QUERY, GCS_BUCKET, gcs_blob_path)

    if row_count > 0:
        load_gcs_to_bq(gcs_uri, full_table_id)
    else:
        log.info("no rows returned — skipping BQ load")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    run_product_flag_etl()
