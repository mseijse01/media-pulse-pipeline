from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def verify_db_connection():
    conn = BaseHook.get_connection("project_postgres")
    print(f"Connected to: {conn.host}:{conn.port}/{conn.schema}")
    print("DB connection verified successfully")


with DAG(
    dag_id="media_pulse_weekly",
    description="Weekly brand attention pipeline — Wikipedia, Trends, Market",
    schedule_interval="0 6 * * 1",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["media-pulse", "weekly"],
) as dag:
    verify_connection = PythonOperator(
        task_id="verify_db_connection",
        python_callable=verify_db_connection,
    )

    extract_wikipedia = EmptyOperator(task_id="extract_wikipedia")
    extract_trends = EmptyOperator(task_id="extract_trends")
    extract_market = EmptyOperator(task_id="extract_market")

    normalize_and_join = EmptyOperator(task_id="normalize_and_join")
    quality_check = EmptyOperator(task_id="quality_check")
    load_to_postgres = EmptyOperator(task_id="load_to_postgres")

    verify_connection >> [extract_wikipedia, extract_trends, extract_market]
    [extract_wikipedia, extract_trends, extract_market] >> normalize_and_join
    normalize_and_join >> quality_check
    quality_check >> load_to_postgres
