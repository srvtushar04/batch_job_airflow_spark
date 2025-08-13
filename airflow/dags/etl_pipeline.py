from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator

# Import project tasks
from scripts.extract_api_data import run as extract_run
from scripts.transform_spark import run as transform_run
from scripts.quality_checks import run as quality_run
from scripts.load_snowflake import run as load_run

DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="etl_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    description="Daily ETL: extract -> transform -> quality -> load",
    tags=["batch", "spark", "portfolio"],
) as dag:

    def _extract(**context):
        run_date = context["ds"]  # YYYY-MM-DD
        output_dir = "/opt/airflow/data/raw"
        os.makedirs(output_dir, exist_ok=True)
        return extract_run(run_date=run_date, output_dir=output_dir)

    def _transform(**context):
        run_date = context["ds"]
        raw_dir = "/opt/airflow/data/raw"
        processed_dir = "/opt/airflow/data/processed"
        cfg_path = "/opt/airflow/configs/spark_config.json"
        os.makedirs(processed_dir, exist_ok=True)
        return transform_run(run_date=run_date, raw_dir=raw_dir, processed_dir=processed_dir, spark_config_path=cfg_path)

    def _quality(**context):
        run_date = context["ds"]
        processed_dir = "/opt/airflow/data/processed"
        return quality_run(run_date=run_date, processed_dir=processed_dir)

    def _load(**context):
        run_date = context["ds"]
        processed_dir = "/opt/airflow/data/processed"
        final_dir = "/opt/airflow/data/final"
        os.makedirs(final_dir, exist_ok=True)
        return load_run(run_date=run_date, processed_dir=processed_dir, final_dir=final_dir)

    extract_task = PythonOperator(task_id="extract_api_data", python_callable=_extract)
    transform_task = PythonOperator(task_id="transform_with_spark", python_callable=_transform)
    quality_task = PythonOperator(task_id="run_quality_checks", python_callable=_quality)
    load_task = PythonOperator(task_id="load_destination", python_callable=_load)

    extract_task >> transform_task >> quality_task >> load_task
