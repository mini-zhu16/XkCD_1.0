import io
import os
import logging
from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
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
    
    fetch_comic_data_obj = fetch_comic_data()

    @task
    def get_latest_comic_number(comic_data):
        latest_comic_number = comic_data["num"].max()
        return latest_comic_number
    get_latest_comic_number_obj = get_latest_comic_number(fetch_comic_data_obj)
    
    @task
    # store the fetched comic data into GCS
    def upload_to_gcs(comic_df):
        # write the dataframe into buffer
        csv_buffer = io.BytesIO()
        comic_df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        csv_bytes = csv_buffer.getvalue()

        # specify the file name and file path
        date_str = datetime.now().strftime("%Y%m%d_%H%M")
        data_file_name = "comic_data"
        data_file_path = f"{_INGEST_FOLDER_NAME}/{data_file_name}_{date_str}.csv"

        # upload the csv file to GCS
        gcs_hook = GCSHook(gcp_conn_id=_GCP_CONN_ID)
        gcs_hook.upload(
            bucket_name=_GCS_BUCKET_NAME, 
            object_name=data_file_path, 
            data=csv_bytes
        )

        logging.info(f"Uploaded comic data to {data_file_path}")

    upload_to_gcs_obj = upload_to_gcs(fetch_comic_data_obj)

    @task
    # save the last fecthed comic number to a txt file
    def upload_last_fetched_comic_num(latest_comic_number):
        last_fetched_comic_content = f"Last Fetched Comic ID: {latest_comic_number}"
        last_fetched_comic_bytes = last_fetched_comic_content.encode('utf-8')
        last_fetched_comic_file_path = f"{_INGEST_FOLDER_NAME}/last_fetched_comic.txt"

        # upload the txt file to GCS
        gcs_hook = GCSHook(gcp_conn_id=_GCP_CONN_ID)
        gcs_hook.upload(
            bucket_name=_GCS_BUCKET_NAME,
            object_name=last_fetched_comic_file_path,
            data=last_fetched_comic_bytes
        )
        logging.info(f"Uploaded the latest fetched comic number to {last_fetched_comic_file_path}")

    upload_last_fetched_comic_num_obj = upload_last_fetched_comic_num(get_latest_comic_number_obj)


    create_bucket_task >> fetch_comic_data_obj >> get_latest_comic_number_obj >> upload_to_gcs_obj >> upload_last_fetched_comic_num_obj

api_to_GCS()