"""
extract_by_region.py
====================
Pulls order transaction data out of Teradata region by region,
writes each chunk to Parquet, pushes to GCS, then loads the
whole thing into BigQuery in one shot.

Usage:
    called from main.py, or run directly:
    python extract_by_region.py
"""

import json
import logging
import os
import re
from multiprocessing import Pool

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import bigquery, secretmanager, storage
from stage1_extract_by_region.queries import DDL_TABLE_LIST, DML_QUERY, FINAL_SCHEMA, high_precision_fields
from teradatasql import connect

log = logging.getLogger(__name__)

# pull from env — nothing sensitive in code
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BQ_DATASET     = os.environ.get("BQ_DATASET", "sales_etl_dev")
TD_SECRET_ID   = os.environ.get("TD_SECRET_ID", "teradata-credential-secret")
GCS_BUCKET     = os.environ.get("GCS_BUCKET", "sales-etl-dev")
TMP_DIR        = r"tmp/stage1"
GCS_PATH       = "order_txn_output"
BQ_TABLE       = "order_transactions_stage1"

REGIONS = ['R01', 'R02', 'R03', 'R04', 'R05', 'R06', 'R07', 'R08', 'R09', 'R10', 'R11', 'R12']


def get_td_secret() -> dict:
    """Fetch Teradata credentials from Secret Manager at runtime."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{GCP_PROJECT_ID}/secrets/{TD_SECRET_ID}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return json.loads(response.payload.data.decode("UTF-8"))


def clean_bq_column_name(name: str) -> str:
    """Strip special characters so column names are safe for BigQuery."""
    name = re.sub(r'[^a-zA-Z0-9_]+', '_', name)
    name = re.sub(r'_{2,}', '_', name)
    return name.strip('_').lower()


# remap column names in the schema before writing Parquet
name_mapping   = {field: clean_bq_column_name(field) for field in high_precision_fields}
cleaned_fields = []
for field in FINAL_SCHEMA:
    new_name = name_mapping.get(field.name, clean_bq_column_name(field.name))
    cleaned_fields.append(pa.field(new_name, field.type, field.nullable, field.metadata))

CLEANED_PARQUET_SCHEMA = pa.schema(cleaned_fields)


def process_region(args) -> int | None:
    """
    Worker function — one region per process.
    Runs DDL to build volatile tables, fires the DML,
    streams results into a local Parquet file, then uploads to GCS.
    Returns the row count, or None if something went wrong.
    """
    region, td_credential = args
    local_file = os.path.join(TMP_DIR, f"data_{region}.parquet")
    gcs_path   = f"{GCS_PATH}/data_{region}.parquet"
    dml_query  = DML_QUERY.format(region=region)
    chunk_size = 100000
    total_rows = 0

    try:
        log.info(f"[{region}] connecting...")
        with connect(
            host=td_credential["host"],
            user=td_credential["username"],
            password=td_credential["password"],
            logmech=td_credential["logmech"],
        ) as conn:
            cur = conn.cursor()

            for ddl in DDL_TABLE_LIST:
                cur.execute(ddl)
            log.info(f"[{region}] volatile tables ready")

            cur.execute(dml_query)
            log.info(f"[{region}] query running, streaming results...")

            writer = pq.ParquetWriter(local_file, CLEANED_PARQUET_SCHEMA, compression='snappy')
            while True:
                rows = cur.fetchmany(chunk_size)
                if not rows:
                    break
                columns = list(zip(*rows))
                arrays  = [
                    pa.array(columns[i], type=CLEANED_PARQUET_SCHEMA[i].type)
                    for i in range(len(CLEANED_PARQUET_SCHEMA))
                ]
                writer.write_table(pa.Table.from_arrays(arrays, schema=CLEANED_PARQUET_SCHEMA))
                total_rows += len(rows)
                log.info(f"[{region}] {total_rows} rows written so far")

            writer.close()
            cur.close()

        if total_rows > 0:
            log.info(f"[{region}] done ({total_rows} rows) — uploading to GCS")
            upload_to_gcs(local_file, GCS_BUCKET, gcs_path)
            os.remove(local_file)
            log.info(f"[{region}] upload complete")
            return total_rows
        else:
            log.info(f"[{region}] no rows returned")
            if os.path.exists(local_file):
                os.remove(local_file)
            return 0

    except Exception as e:
        log.error(f"[{region}] failed: {e}", exc_info=True)
        return None


def upload_to_gcs(local_file: str, bucket_name: str, gcs_path: str):
    """Push a local file up to GCS."""
    client = storage.Client()
    client.bucket(bucket_name).blob(gcs_path).upload_from_filename(local_file)


def load_gcs_to_bq():
    """
    Load all regional Parquet files from GCS into a single BigQuery table.
    Uses WRITE_TRUNCATE so each run replaces the previous data.
    """
    client   = bigquery.Client()
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    log.info(f"loading into {table_id}")

    try:
        schema = client.get_table(table_id).schema
    except Exception as e:
        log.error(f"could not fetch schema: {e}")
        raise

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schema,
    )

    uri      = f"gs://{GCS_BUCKET}/{GCS_PATH}/data_*.parquet"
    load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
    log.info(f"BQ load job: {load_job.job_id}")
    load_job.result()

    log.info(f"loaded {client.get_table(table_id).num_rows} rows into {table_id}")


def main():
    os.makedirs(TMP_DIR, exist_ok=True)

    td_credential = get_td_secret()
    log.info(f"kicking off ETL across {len(REGIONS)} regions")

    with Pool(processes=6) as pool:
        results = pool.map(process_region, [(r, td_credential) for r in REGIONS])

    log.info(f"all regions done — {sum(r for r in results if r is not None)} total rows")
    load_gcs_to_bq()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(processName)s] %(message)s')
    main()
