from airflow import DAG
from airflow.utils.dates import days_ago
from cosmos import DbtDag

# Define the path to your dbt project
DBT_PROJECT_PATH = "dags/XkCD-transform/xkcd_dbt"

# Create the DAG using Cosmos
with DAG(
    "dbt_pipeline",
    start_date=days_ago(1),
    schedule_interval="@daily",  # Adjust as needed
    catchup=False,
) as dag:
    
    dbt_task = DbtDag(
        dbt_project_path=DBT_PROJECT_PATH,
        profile_name="xkcd_dbt",  # Matches your dbt profiles.yml entry
        conn_id="google_cloud",  # Airflow connection ID for BigQuery
    )

    dbt_task
