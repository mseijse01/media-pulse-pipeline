from datetime import datetime, timedelta
import json
import os
import wikipediaapi

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

BRANDS = ["Nike", "Coca-Cola", "Netflix", "Apple", "Samsung"]
DATA_DIR = "/opt/airflow/data"


def verify_db_connection():
    conn = BaseHook.get_connection("project_postgres")
    print(f"Connected to: {conn.host}:{conn.port}/{conn.schema}")
    print("DB connection verified successfully")


def get_week_start(execution_date) -> str:
    monday = execution_date.start_of("week")
    return monday.strftime("%Y-%m-%d")


def extract_wikipedia(**context):
    execution_date = context["data_interval_start"]
    week_start = get_week_start(execution_date)

    wiki = wikipediaapi.Wikipedia(
        language="en", user_agent="media-pulse-pipeline/1.0 (portfolio project)"
    )

    results = {}
    for brand in BRANDS:
        page = wiki.page(brand)
        if page.exists():
            results[brand] = {
                "brand": brand,
                "week_start": week_start,
                "page_exists": True,
                "summary_length": len(page.summary),
                "title": page.title,
            }
            print(
                f"Wikipedia — {brand}: page found, summary length {len(page.summary)}"
            )
        else:
            results[brand] = {
                "brand": brand,
                "week_start": week_start,
                "page_exists": False,
                "summary_length": 0,
                "title": None,
            }
            print(f"Wikipedia — {brand}: page NOT found")

    os.makedirs(DATA_DIR, exist_ok=True)
    output_path = os.path.join(DATA_DIR, f"wikipedia_{week_start}.json")
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)

    print(f"Saved Wikipedia data to {output_path}")
    return output_path


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

    extract_wiki = PythonOperator(
        task_id="extract_wikipedia",
        python_callable=extract_wikipedia,
    )

    extract_trends = EmptyOperator(task_id="extract_trends")
    extract_market = EmptyOperator(task_id="extract_market")

    normalize_and_join = EmptyOperator(task_id="normalize_and_join")
    quality_check = EmptyOperator(task_id="quality_check")
    load_to_postgres = EmptyOperator(task_id="load_to_postgres")

    verify_connection >> [extract_wiki, extract_trends, extract_market]
    [extract_wiki, extract_trends, extract_market] >> normalize_and_join
    normalize_and_join >> quality_check
    quality_check >> load_to_postgres
