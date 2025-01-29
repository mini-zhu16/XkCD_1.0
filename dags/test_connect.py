from airflow import DAG
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "Minni",
    "start_date": datetime(2025, 1, 28),
    "retries": 5,
    "retry_delay": timedelta(minutes=2)
}

GCS_BUCKET_NAME = "minni_test_bucket"
PROJECT_ID = "xkcd-449310"
LOCATION = "EU"  # Bucket location (e.g., US, EU)

# create a bucket
with DAG(
    default_args=default_args,
    dag_id = "create_bucket",
    schedule = "@once",
    catchup = False
):
    create_bucket_task = GCSCreateBucketOperator(
        task_id = "create_bucket",
        bucket_name = GCS_BUCKET_NAME,
        project_id = PROJECT_ID,
        location = LOCATION,
        gcp_conn_id = "google_cloud"
    )

    create_bucket_task

