"""
Backfill script for media-pulse-pipeline.

Fetches Wikipedia pageviews and market closing prices for the past 6 months
and loads them into brand_weekly_metrics via upsert.

Google Trends is excluded from the backfill due to rate limiting constraints
and granularity inconsistencies for historical data. Trends scores for
historical weeks will remain null and can be populated incrementally
by the weekly pipeline going forward.

Usage:
    python scripts/backfill.py
    python scripts/backfill.py --weeks 12  # backfill last 12 weeks only
    python scripts/backfill.py --dry-run   # print weeks without loading
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta
from decimal import Decimal

import psycopg2
import requests
import yfinance as yf
from psycopg2.extras import execute_values

DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "dbname": "mediapulse",
    "user": "mediapulse",
    "password": "mediapulse",
}

BRANDS = ["Nike", "Coca-Cola", "Netflix", "Apple", "Samsung"]

BRAND_WIKI_TITLES = {
    "Nike": "Nike,_Inc.",
    "Coca-Cola": "Coca-Cola",
    "Netflix": "Netflix",
    "Apple": "Apple_Inc.",
    "Samsung": "Samsung",
}

BRAND_TICKERS = {
    "Nike": "NKE",
    "Coca-Cola": "KO",
    "Netflix": "NFLX",
    "Apple": "AAPL",
    "Samsung": "005930.KS",
}

WIKI_HEADERS = {"User-Agent": "media-pulse-pipeline/1.0 (portfolio project)"}


def get_monday_dates(num_weeks: int) -> list[str]:
    today = datetime.now()
    current_monday = today - timedelta(days=today.weekday())
    mondays = []
    for i in range(1, num_weeks + 1):
        monday = current_monday - timedelta(weeks=i)
        mondays.append(monday.strftime("%Y-%m-%d"))
    return mondays


def fetch_wikipedia_views(week_start: str) -> dict:
    start_dt = datetime.strptime(week_start, "%Y-%m-%d")
    end_dt = start_dt + timedelta(days=6)
    start_str = start_dt.strftime("%Y%m%d")
    end_str = end_dt.strftime("%Y%m%d")

    results = {}
    for brand in BRANDS:
        wiki_title = BRAND_WIKI_TITLES[brand]
        url = (
            f"https://wikimedia.org/api/rest_v1/metrics/pageviews/"
            f"per-article/en.wikipedia/all-access/all-agents/"
            f"{wiki_title}/daily/{start_str}/{end_str}"
        )
        try:
            response = requests.get(url, headers=WIKI_HEADERS, timeout=10)
            if response.status_code == 200:
                data = response.json()
                total_views = sum(item["views"] for item in data.get("items", []))
                results[brand] = total_views
                print(f"  Wikipedia — {brand}: {total_views:,} views")
            else:
                results[brand] = None
                print(f"  Wikipedia — {brand}: status {response.status_code}")
        except Exception as e:
            results[brand] = None
            print(f"  Wikipedia — {brand}: ERROR — {e}")

    return results


def fetch_market_data(week_start: str) -> dict:
    end_dt = (datetime.strptime(week_start, "%Y-%m-%d") + timedelta(days=6)).strftime(
        "%Y-%m-%d"
    )

    usd_krw_rate = None
    try:
        fx = yf.Ticker("KRW=X")
        fx_hist = fx.history(start=week_start, end=end_dt)
        if not fx_hist.empty:
            usd_krw_rate = round(float(fx_hist["Close"].iloc[-1]), 4)
    except Exception as e:
        print(f"  FX rate fetch failed: {e}")

    results = {}
    for brand, ticker in BRAND_TICKERS.items():
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(start=week_start, end=end_dt)

            if not hist.empty:
                close_price = round(float(hist["Close"].iloc[-1]), 4)

                if ticker.endswith(".KS"):
                    currency = "KRW"
                    close_usd = (
                        round(close_price / usd_krw_rate, 4) if usd_krw_rate else None
                    )
                else:
                    currency = "USD"
                    close_usd = close_price

                results[brand] = {
                    "market_close": close_price,
                    "market_close_usd": close_usd,
                    "market_currency": currency,
                }
                print(
                    f"  Market — {brand}: {close_price} {currency}"
                    + (f" = ${close_usd} USD" if currency != "USD" else "")
                )
            else:
                results[brand] = {
                    "market_close": None,
                    "market_close_usd": None,
                    "market_currency": "KRW" if ticker.endswith(".KS") else "USD",
                }
                print(f"  Market — {brand}: no data")

        except Exception as e:
            results[brand] = {
                "market_close": None,
                "market_close_usd": None,
                "market_currency": "KRW" if ticker.endswith(".KS") else "USD",
            }
            print(f"  Market — {brand}: ERROR — {e}")

    return results


def load_week(
    conn,
    brand_map: dict,
    week_start: str,
    wiki_data: dict,
    market_data: dict,
    dry_run: bool = False,
) -> int:
    rows = []
    for brand in BRANDS:
        brand_id = brand_map.get(brand)
        if not brand_id:
            print(f"  WARNING: brand '{brand}' not in brands table, skipping")
            continue

        market = market_data.get(brand, {})
        rows.append(
            (
                brand_id,
                week_start,
                wiki_data.get(brand),
                None,
                market.get("market_close"),
                market.get("market_close_usd"),
                market.get("market_currency", "USD"),
            )
        )

    if dry_run:
        print(f"  DRY RUN — would upsert {len(rows)} rows")
        return len(rows)

    upsert_sql = """
        INSERT INTO brand_weekly_metrics
            (brand_id, week_start, wikipedia_views, trends_score,
             market_close, market_close_usd, market_currency)
        VALUES %s
        ON CONFLICT (brand_id, week_start)
        DO UPDATE SET
            wikipedia_views  = EXCLUDED.wikipedia_views,
            trends_score     = COALESCE(
                                 brand_weekly_metrics.trends_score,
                                 EXCLUDED.trends_score
                               ),
            market_close     = EXCLUDED.market_close,
            market_close_usd = EXCLUDED.market_close_usd,
            market_currency  = EXCLUDED.market_currency,
            loaded_at        = NOW()
    """

    cursor = conn.cursor()
    execute_values(cursor, upsert_sql, rows)
    conn.commit()
    cursor.close()
    return len(rows)


def main():
    parser = argparse.ArgumentParser(description="Backfill brand weekly metrics")
    parser.add_argument(
        "--weeks",
        type=int,
        default=26,
        help="Number of weeks to backfill (default: 26)",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Print weeks without loading to database"
    )
    args = parser.parse_args()

    print(f"Starting backfill — {args.weeks} weeks")
    if args.dry_run:
        print("DRY RUN MODE — no data will be loaded")
    print()

    conn = None
    brand_map = {}

    if not args.dry_run:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT brand_id, brand_name FROM brands")
        brand_map = {row[1]: row[0] for row in cursor.fetchall()}
        cursor.close()
        print(f"Connected to database. Brand map: {brand_map}")
        print()

    mondays = get_monday_dates(args.weeks)
    total_rows = 0

    for i, week_start in enumerate(mondays, 1):
        print(f"[{i}/{len(mondays)}] Week of {week_start}")

        wiki_data = fetch_wikipedia_views(week_start)
        time.sleep(0.5)

        market_data = fetch_market_data(week_start)
        time.sleep(0.5)

        rows_loaded = load_week(
            conn, brand_map, week_start, wiki_data, market_data, dry_run=args.dry_run
        )
        total_rows += rows_loaded
        print(f"  Loaded {rows_loaded} rows")
        print()

    if conn:
        conn.close()

    print(f"Backfill complete — {total_rows} rows across {len(mondays)} weeks")


if __name__ == "__main__":
    main()
