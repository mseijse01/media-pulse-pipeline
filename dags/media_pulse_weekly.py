from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="media_pulse_weekly",
    description="Weekly brand attention pipeline — Wikipedia, Trends, Market",
    schedule_interval="0 6 * * 1",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["media-pulse", "weekly"],
) as dag:
    extract_wikipedia = EmptyOperator(task_id="extract_wikipedia")
    extract_trends = EmptyOperator(task_id="extract_trends")
    extract_market = EmptyOperator(task_id="extract_market")

    normalize_and_join = EmptyOperator(task_id="normalize_and_join")
    quality_check = EmptyOperator(task_id="quality_check")
    load_to_postgres = EmptyOperator(task_id="load_to_postgres")

    [extract_wikipedia, extract_trends, extract_market] >> normalize_and_join
    normalize_and_join >> quality_check
    quality_check >> load_to_postgres
