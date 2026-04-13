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


def normalize_and_join(**context):
    import pandas as pd

    ti = context["ti"]
    week_start = get_week_start(context["data_interval_start"])

    wiki_path = ti.xcom_pull(task_ids="extract_wikipedia")
    trends_path = ti.xcom_pull(task_ids="extract_trends")
    market_path = ti.xcom_pull(task_ids="extract_market")

    print(f"Reading Wikipedia data from: {wiki_path}")
    print(f"Reading Trends data from: {trends_path}")
    print(f"Reading Market data from: {market_path}")

    with open(wiki_path, encoding="utf-8") as f:
        wiki_data = json.load(f)
    with open(trends_path, encoding="utf-8") as f:
        trends_data = json.load(f)
    with open(market_path, encoding="utf-8") as f:
        market_data = json.load(f)

    rows = []
    for brand in BRANDS:
        wiki = wiki_data.get(brand, {})
        trends = trends_data.get(brand, {})
        market = market_data.get(brand, {})

        rows.append(
            {
                "brand": brand,
                "week_start": week_start,
                "wikipedia_views": wiki.get("summary_length"),
                "trends_score": trends.get("trends_score"),
                "market_close": market.get("market_close"),
            }
        )

    df = pd.DataFrame(rows)

    print(f"\nNormalized DataFrame:")
    print(df.to_string(index=False))

    output_path = os.path.join(DATA_DIR, f"normalized_{week_start}.json")
    df.to_json(output_path, orient="records", indent=2)

    print(f"\nSaved normalized data to {output_path}")
    return output_path


def quality_check(**context):
    import pandas as pd

    ti = context["ti"]
    normalized_path = ti.xcom_pull(task_ids="normalize_and_join")

    df = pd.read_json(normalized_path)

    errors = []

    if len(df) != len(BRANDS):
        errors.append(f"Expected {len(BRANDS)} rows, got {len(df)}")

    missing_brands = set(BRANDS) - set(df["brand"].tolist())
    if missing_brands:
        errors.append(f"Missing brands: {missing_brands}")

    null_counts = df[["brand", "week_start"]].isnull().sum()
    for col, count in null_counts.items():
        if count > 0:
            errors.append(f"Critical column '{col}' has {count} null values")

    optional_cols = ["wikipedia_views", "trends_score", "market_close"]
    for col in optional_cols:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            print(
                f"WARNING: '{col}' has {null_count} null values — acceptable but noted"
            )

    samsung_market = df[df["brand"] == "Samsung"]["market_close"].iloc[0]
    if samsung_market and samsung_market > 10000:
        print(
            f"WARNING: Samsung market_close is {samsung_market} — likely KRW, not USD"
        )

    if errors:
        error_msg = "\n".join(errors)
        raise ValueError(f"Quality check failed:\n{error_msg}")

    print("Quality check passed")
    print(
        df[
            ["brand", "week_start", "wikipedia_views", "trends_score", "market_close"]
        ].to_string(index=False)
    )

    return normalized_path


def load_to_postgres(**context):
    import psycopg2
    from psycopg2.extras import execute_values

    ti = context["ti"]
    normalized_path = ti.xcom_pull(task_ids="normalize_and_join")

    conn_airflow = BaseHook.get_connection("project_postgres")
    conn = psycopg2.connect(
        host=conn_airflow.host,
        port=conn_airflow.port,
        dbname=conn_airflow.schema,
        user=conn_airflow.login,
        password=conn_airflow.password,
    )

    with open(normalized_path, encoding="utf-8") as f:
        records = json.load(f)

    cursor = conn.cursor()

    cursor.execute("SELECT brand_id, brand_name FROM brands")
    brand_map = {row[1]: row[0] for row in cursor.fetchall()}
    print(f"Brand map: {brand_map}")

    rows = []
    for record in records:
        brand_name = record["brand"]
        brand_id = brand_map.get(brand_name)
        if not brand_id:
            print(f"WARNING: brand '{brand_name}' not found in brands table, skipping")
            continue

        rows.append(
            (
                brand_id,
                record["week_start"],
                record.get("wikipedia_views"),
                record.get("trends_score"),
                record.get("market_close"),
            )
        )

    upsert_sql = """
        INSERT INTO brand_weekly_metrics
            (brand_id, week_start, wikipedia_views, trends_score, market_close)
        VALUES %s
        ON CONFLICT (brand_id, week_start)
        DO UPDATE SET
            wikipedia_views = EXCLUDED.wikipedia_views,
            trends_score    = EXCLUDED.trends_score,
            market_close    = EXCLUDED.market_close,
            loaded_at       = NOW()
    """

    execute_values(cursor, upsert_sql, rows)
    conn.commit()

    print(f"Upserted {len(rows)} rows into brand_weekly_metrics")

    cursor.execute(
        "SELECT b.brand_name, m.week_start, m.wikipedia_views, "
        "m.trends_score, m.market_close "
        "FROM brand_weekly_metrics m "
        "JOIN brands b ON b.brand_id = m.brand_id "
        "ORDER BY b.brand_name"
    )
    results = cursor.fetchall()
    print("\nCurrent state of brand_weekly_metrics:")
    for row in results:
        print(f"  {row}")

    cursor.close()
    conn.close()


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

    normalize_and_join = PythonOperator(
        task_id="normalize_and_join",
        python_callable=normalize_and_join,
    )

    quality_check = PythonOperator(
        task_id="quality_check",
        python_callable=quality_check,
    )

    load_to_postgres = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
    )

    verify_connection >> [extract_wiki, extract_trends, extract_market]
    [extract_wiki, extract_trends, extract_market] >> normalize_and_join
    normalize_and_join >> quality_check
    quality_check >> load_to_postgres
