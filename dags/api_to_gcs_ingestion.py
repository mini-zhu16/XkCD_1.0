from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import io
import os
import logging
from datetime import datetime, timedelta
from include.api_functions import get_comic_data

# GCP variables
_GCP_CONN_ID = os.getenv("GCP_CONN_ID", "google_cloud")
_GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "xkcd-raw-data")
_INGEST_FOLDER_NAME = os.getenv("INGEST_FOLDER_NAME", "xkcd")
_PROJECT_ID = os.getenv("PROJECT_ID", "xkcd-449310")

default_args = {
    "owner": "Minni",
    "start_date": datetime(2025, 1, 28),
    "retries": 5,
    "retry_delay": timedelta(minutes=2)
}

@dag(
    default_args=default_args,
    dag_id = "ingest_api_to_gcs",
    schedule = "@once",
    catchup = False
)
def api_to_GCS():
    # create a bucket to store the raw api data if not exist
    # because it is an operator-based task, no need to use task decorator
    create_bucket_task = GCSCreateBucketOperator(
            task_id = "create_bucket",
            bucket_name = _GCS_BUCKET_NAME,
            project_id = _PROJECT_ID,
            location = "EU",
            gcp_conn_id = _GCP_CONN_ID
    )

    @task
    # task decorator is used to turn python functions into airflow tasks
    # fetch all available comic data
    def fetch_comic_data():
        # get the comic data
        comic_df = get_comic_data()
        return comic_df
    
    @task
    # store the fetched data into GCS
    def upload_to_gcs(comic_df):
        # write the dataframe into buffer
        csv_buffer = io.BytesIO()
        comic_df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        # specify the file name and file path
        file_name = "comic_data.csv"
        file_path = f"{_INGEST_FOLDER_NAME}/{file_name}.csv"

        # upload the csv file to GCS
        gcs_hook = GCSHook(gcp_conn_id=_GCP_CONN_ID)
        gcs_hook.upload(
            bucket_name=_GCS_BUCKET_NAME, 
            object_name=file_path, 
            data=csv_buffer
        )

        logging.info(f"Uploaded comic data to {gcs_file_path}")




            


