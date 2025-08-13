from datetime import datetime
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# Import callables inside tasks
from scripts.extract_api_data import run as extract_run
from scripts.transform_spark import run as transform_run
from scripts.ge_validate import run as ge_run
from scripts.load_to_snowflake import run as load_run

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="etl_pipeline",
    description="API → MinIO → Spark → GE → Snowflake",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["batch", "spark", "snowflake", "ge"],
) as dag:

    extract = PythonOperator(
        task_id="extract_api_to_s3",
        python_callable=extract_run,
    )

    transform = PythonOperator(
        task_id="transform_with_spark",
        python_callable=transform_run,
    )

    validate = PythonOperator(
        task_id="validate_with_great_expectations",
        python_callable=ge_run,
    )

    load = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=load_run,
    )

    extract >> transform >> validate >> load
