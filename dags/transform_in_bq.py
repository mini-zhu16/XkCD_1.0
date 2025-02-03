from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import TestBehavior
from cosmos.profiles import GoogleCloudServiceAccountFileProfileMapping
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
import os
from datetime import datetime

airflow_home = os.environ["AIRFLOW_HOME"]
_PROJECT_ID = os.getenv("PROJECT_ID", "xkcd-449310")
_BQ_DATASET_NAME = os.getenv("BQ_DATASET_NAME", "xkcd_dataset")

# Define the profile configuration for BigQuery
profile_config = ProfileConfig(
    profile_name="xkcd_dbt", 
    target_name="dev",       
    profile_mapping=GoogleCloudServiceAccountFileProfileMapping(
        conn_id="google_cloud", 
        profile_args={
            "project": _PROJECT_ID,  
            "dataset": _BQ_DATASET_NAME, 
        },
    ),
)

# Define the Airflow DAG for running DBT transformations
my_cosmos_dag = DbtDag(
    # Project configuration pointing to the location of DBT project files
    project_config=ProjectConfig(
        f"{airflow_home}/dags/dbt/xkcd_dbt",
    ),
    # Render configuration for DBT, defining test behavior to build DBT tests
    render_config=RenderConfig(
        test_behavior=TestBehavior.BUILD,
    ),
    # Profile configuration that holds the BigQuery connection setup
    profile_config=profile_config,
    # Execution configuration for the DAG, specifying the path to the DBT executable
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{airflow_home}/dbt_venv/bin/dbt",
    ),
    # normal dag parameters
    schedule_interval="@once",
    start_date=days_ago(1),
    catchup=False,
    dag_id="bigquery_transformations",
    default_args={"retries": 2},
)
