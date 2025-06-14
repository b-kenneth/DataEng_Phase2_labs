from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from extract_and_validate import extract_and_validate
from transform import transform

# === DAG Default Config ===
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# === DAG Definition ===
with DAG(
    dag_id="music_etl_pipeline",
    default_args=default_args,
    description="ETL for music streaming data using S3 + Redshift",
    schedule_interval="@daily",
    start_date=datetime(2025, 6, 12),
    catchup=False,
    tags=["music", "ETL", "s3"],
) as dag:

    extract_validate_task = PythonOperator(
        task_id="extract_and_validate",
        python_callable=extract_and_validate,
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform,
    )

    extract_validate_task >> transform_task
