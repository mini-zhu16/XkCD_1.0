FROM quay.io/astronomer/astro-runtime:12.6.0

# install dbt into a virtual environment
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-bigquery && deactivate

# set up environment variables
ENV GCP_CONN_ID="google_cloud"
ENV GCS_BUCKET_NAME="xkcd-raw-data"
ENV INGEST_FOLDER_NAME="xkcd"
ENV PROJECT_ID="xkcd-449310"
ENV BQ_DATASET_NAME="xkcd_dataset"
ENV BQ_TABLE_NAME="xkcd_comics"