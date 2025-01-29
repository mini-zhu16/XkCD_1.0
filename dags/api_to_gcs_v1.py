import io
import os
import logging
from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.models.baseoperator import chain
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from datetime import datetime, timedelta
from include.api_functions import get_comic_data, get_current_comic_number

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
    dag_id = "api_to_gs_withoutXcom",
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
    def get_latest_comic_number():
        # check the latest available comic number
        latest_comic_number = get_current_comic_number()
        return latest_comic_number

    @task
    def get_last_fetched_comic_num():
        # check the last fetched comic number
        gcs_hook = GCSHook(gcp_conn_id=_GCP_CONN_ID)
        last_comic_file_path = f"{_INGEST_FOLDER_NAME}/last_fetched_comic.txt"

        #check if the file exists in the bucket
        if gcs_hook.exists(bucket_name=_GCS_BUCKET_NAME, object_name=last_comic_file_path):                
            # Download the last fetched comic number from GCS
            last_fetched_comic_bytes = gcs_hook.download(bucket_name=_GCS_BUCKET_NAME, object_name=last_comic_file_path)
            last_fetched_comic_content = last_fetched_comic_bytes.decode("utf-8")
            last_fetched_comic_number = int(last_fetched_comic_content.split(":")[1].strip())
            return last_fetched_comic_number
        else:
            return 0

    @task
    def is_new_comic_available(latest_comic_number, last_fetched_comic_number):
        # check if there is new comic available from the api
        new_comic_available = latest_comic_number > last_fetched_comic_number
        return new_comic_available
    
    @task.branch
    def decide_next_task(new_comic_available):
        # decide the next task based on the availability of new comic
        if new_comic_available:
            return 'fetch_comic_data'
        else:
            return 'upload_last_fetched_comic_num'
        
    @task
    def fetch_comic_data(start_num):
        # fetch the available new comic data
        comic_df = get_comic_data(start_num = start_num)
        return comic_df
    
    @task
    def upload_to_gcs(comic_df):
        # store the fetched comic data into GCS
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

    @task
    def upload_last_fetched_comic_num(latest_comic_number):
        # save the last fecthed comic number to a txt file
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

    # task dependencies
    latest_comic_number = get_latest_comic_number()
    last_fetched_comic_number = get_last_fetched_comic_num()
    new_comic_available = is_new_comic_available(latest_comic_number, last_fetched_comic_number)
    next_task = decide_next_task(new_comic_available)

    # Set up dependencies
    create_bucket_task >> latest_comic_number >> last_fetched_comic_number >> new_comic_available >> next_task

    # Conditional tasks
    fetch_comic = fetch_comic_data(last_fetched_comic_number)
    upload_comic = upload_to_gcs(fetch_comic)
    upload_last_fetched = upload_last_fetched_comic_num(latest_comic_number)

    # Define the branching logic
    next_task >> fetch_comic >> upload_comic
    next_task >> upload_last_fetched

api_to_GCS()

    # create_bucket_task >> get_latest_comic_number_obj
    # create_bucket_task >> get_last_fetched_comic_num_obj

    # [get_latest_comic_number_obj, get_last_fetched_comic_num_obj] >> is_new_comic_available_obj
    # is_new_comic_available_obj >> decide_next_task_obj

    # decide_next_task_obj >> fetch_comic_data_obj
    # decide_next_task_obj >> upload_last_fetched_comic_num_obj

    # fetch_comic_data_obj >> upload_to_gcs_obj
    # upload_to_gcs_obj >> upload_last_fetched_comic_num_obj

