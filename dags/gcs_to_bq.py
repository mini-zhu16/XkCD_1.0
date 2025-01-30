import os
from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery
from airflow.utils.dates import days_ago
from datetime import timedelta

# GCP variables
_GCP_CONN_ID = os.getenv("GCP_CONN_ID", "google_cloud")
_GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "xkcd-raw-data")
_INGEST_FOLDER_NAME = os.getenv("INGEST_FOLDER_NAME", "xkcd")
_PROJECT_ID = os.getenv("PROJECT_ID", "xkcd-449310")
_BQ_DATASET_NAME = os.getenv("BQ_DATASET_NAME", "xkcd_dataset")
_BQ_TABLE_NAME = os.getenv("BQ_TABLE_NAME", "xkcd_comics")
_PROCESSED_FILES_LOG = f"{_INGEST_FOLDER_NAME}/processed_files.txt"  # File to track processed files

default_args = {
    "owner": "Minni",
    "start_date": days_ago(1),  # Use days_ago for relative start date
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

@dag(
    dag_id="gcs_to_bigquery_ingestion",
    default_args=default_args,
    schedule_interval="@once",  # Run daily
    tags=["gcs", "bigquery"],
)
def gcs_to_bigquery_dag():

    @task
    def get_processed_files():
        """Retrieve the list of already processed files from GCS."""
        gcs_hook = GCSHook(gcp_conn_id=_GCP_CONN_ID)
        processed_files = []

        # Check if the processed files log exists in GCS
        if gcs_hook.exists(bucket_name=_GCS_BUCKET_NAME, object_name=_PROCESSED_FILES_LOG):
            # Download the processed files log
            processed_files_bytes = gcs_hook.download(
                bucket_name=_GCS_BUCKET_NAME,
                object_name=_PROCESSED_FILES_LOG,
            )
            processed_files = processed_files_bytes.decode("utf-8").splitlines()

        return set(processed_files)  # Return a set for faster lookup

    # Use GCSListObjectsOperator to list files in GCS
    list_gcs_files = GCSListObjectsOperator(
        task_id="list_gcs_files",
        bucket=_GCS_BUCKET_NAME,
        prefix=_INGEST_FOLDER_NAME + "/",
        gcp_conn_id=_GCP_CONN_ID,
    )

    @task
    def filter_new_csv_files(processed_files, ti=None):
        """Filter out files that have already been processed and that are not csv files."""
        all_files = ti.xcom_pull(task_ids="list_gcs_files")
        return [file for file in all_files if file.endswith(".csv") and file not in processed_files]

    @task
    def load_gcs_to_bq(new_files):
        """Load CSV files from GCS into BigQuery using the BigQuery Python Client."""
        if not new_files:  # Skip if there are no new files
            return "No new files to load."

        # Initialize BigQuery client
        bq_hook = BigQueryHook(gcp_conn_id=_GCP_CONN_ID)
        client = bq_hook.get_client()

        # Define the BigQuery table reference
        table_ref = f"{_PROJECT_ID}.{_BQ_DATASET_NAME}.{_BQ_TABLE_NAME}"

        # Configure the load job
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            autodetect=True,  # Automatically detect schema
            skip_leading_rows=1,  # Skip the header row
            allow_quoted_newlines=True,  # Allow newlines in quoted fields
            field_delimiter=",",  # Set the field delimiter
            write_disposition="WRITE_APPEND",  # Append to the table
            # schema=schema,
        )

        # Load each file into BigQuery
        for file in new_files:
            uri = f"gs://{_GCS_BUCKET_NAME}/{file}"
            load_job = client.load_table_from_uri(
                uri,
                table_ref,
                job_config=job_config,
            )
            load_job.result()  # Wait for the job to complete

            if load_job.errors:
                raise Exception(f"Errors occurred while loading {file}: {load_job.errors}")
            
            # Update the file metadata columns with the source file information
            update_query = f"""
            ALTER TABLE `{table_ref}`
            ADD COLUMN IF NOT EXISTS source_file_name STRING,
            ADD COLUMN IF NOT EXISTS source_file_path STRING,
            ADD COLUMN IF NOT EXISTS created_at TIMESTAMP;

            UPDATE `{table_ref}`
            SET source_file_name = '{os.path.basename(file)}',
                source_file_path = '{file}',
                created_at = CURRENT_TIMESTAMP()
            WHERE source_file_name IS NULL
            """
            client.query(update_query).result()

        return f"Loaded {len(new_files)} files into BigQuery."

    @task
    def update_processed_files(new_files):
        """Update the processed files log in GCS with the newly processed files."""
        if new_files:  # Only update if there are new files
            gcs_hook = GCSHook(gcp_conn_id=_GCP_CONN_ID)

            # Download the existing processed files log
            if gcs_hook.exists(bucket_name=_GCS_BUCKET_NAME, object_name=_PROCESSED_FILES_LOG):
                processed_files_bytes = gcs_hook.download(
                    bucket_name=_GCS_BUCKET_NAME,
                    object_name=_PROCESSED_FILES_LOG,
                )
                processed_files = processed_files_bytes.decode("utf-8").splitlines()
            else:
                processed_files = []

            # Add the newly processed files to the log
            processed_files.extend(new_files)

            # Upload the updated log back to GCS
            processed_files_content = "\n".join(processed_files)
            gcs_hook.upload(
                bucket_name=_GCS_BUCKET_NAME,
                object_name=_PROCESSED_FILES_LOG,
                data=processed_files_content.encode("utf-8"),
            )
            return f"Updated processed files log with {len(new_files)} new files."
        else:
            return "No new files to update."

    # Task dependencies
    processed_files = get_processed_files()
    all_files = list_gcs_files
    new_files = filter_new_csv_files(processed_files)
    load_result = load_gcs_to_bq(new_files)
    update_processed_files = update_processed_files(new_files)

    processed_files >> all_files >> new_files >> load_result >> update_processed_files



# Instantiate the DAG
gcs_to_bigquery_dag()