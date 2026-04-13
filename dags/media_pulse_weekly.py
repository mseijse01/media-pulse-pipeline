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
BRAND_TICKERS = {
    "Nike": "NKE",
    "Coca-Cola": "KO",
    "Netflix": "NFLX",
    "Apple": "AAPL",
    "Samsung": "005930.KS",
}
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


def extract_market(**context):
    import yfinance as yf

    execution_date = context["data_interval_start"]
    week_start = get_week_start(execution_date)
    week_end = (datetime.strptime(week_start, "%Y-%m-%d") + timedelta(days=6)).strftime(
        "%Y-%m-%d"
    )

    results = {}
    for brand, ticker in BRAND_TICKERS.items():
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(start=week_start, end=week_end)

            if not hist.empty:
                close_price = round(float(hist["Close"].iloc[-1]), 4)
                results[brand] = {
                    "brand": brand,
                    "ticker": ticker,
                    "week_start": week_start,
                    "market_close": close_price,
                    "data_available": True,
                }
                print(f"Market — {brand} ({ticker}): close {close_price}")
            else:
                results[brand] = {
                    "brand": brand,
                    "ticker": ticker,
                    "week_start": week_start,
                    "market_close": None,
                    "data_available": False,
                }
                print(f"Market — {brand} ({ticker}): no data for week")

        except Exception as e:
            results[brand] = {
                "brand": brand,
                "ticker": ticker,
                "week_start": week_start,
                "market_close": None,
                "data_available": False,
            }
            print(f"Market — {brand} ({ticker}): ERROR — {e}")

    os.makedirs(DATA_DIR, exist_ok=True)
    output_path = os.path.join(DATA_DIR, f"market_{week_start}.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    print(f"Saved market data to {output_path}")
    return output_path


def extract_trends(**context):
    import time
    from pytrends.request import TrendReq
    from pytrends.exceptions import ResponseError

    execution_date = context["data_interval_start"]
    week_start = get_week_start(execution_date)
    week_end = (datetime.strptime(week_start, "%Y-%m-%d") + timedelta(days=6)).strftime(
        "%Y-%m-%d"
    )
    timeframe = f"{week_start} {week_end}"

    pytrends = TrendReq(hl="en-US", tz=0, timeout=(10, 25))

    results = {}
    for brand in BRANDS:
        try:
            time.sleep(5)
            pytrends.build_payload([brand], timeframe=timeframe, geo="")
            df = pytrends.interest_over_time()

            if not df.empty and brand in df.columns:
                avg_score = round(float(df[brand].mean()), 2)
                results[brand] = {
                    "brand": brand,
                    "week_start": week_start,
                    "trends_score": avg_score,
                    "data_available": True,
                }
                print(f"Trends — {brand}: score {avg_score}")
            else:
                results[brand] = {
                    "brand": brand,
                    "week_start": week_start,
                    "trends_score": None,
                    "data_available": False,
                }
                print(f"Trends — {brand}: empty response")

        except Exception as e:
            results[brand] = {
                "brand": brand,
                "week_start": week_start,
                "trends_score": None,
                "data_available": False,
            }
            print(f"Trends — {brand}: failed — {e}")

    os.makedirs(DATA_DIR, exist_ok=True)
    output_path = os.path.join(DATA_DIR, f"trends_{week_start}.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    print(f"Saved trends data to {output_path}")
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

    extract_trends = PythonOperator(
        task_id="extract_trends",
        python_callable=extract_trends,
    )

    extract_market = PythonOperator(
        task_id="extract_market",
        python_callable=extract_market,
    )

    normalize_and_join = EmptyOperator(task_id="normalize_and_join")
    quality_check = EmptyOperator(task_id="quality_check")
    load_to_postgres = EmptyOperator(task_id="load_to_postgres")

    verify_connection >> [extract_wiki, extract_trends, extract_market]
    [extract_wiki, extract_trends, extract_market] >> normalize_and_join
    normalize_and_join >> quality_check
    quality_check >> load_to_postgres
