# Media Pulse Pipeline

A weekly data pipeline that tracks brand attention signals across multiple public data sources, normalises them into a common schema, and loads them into PostgreSQL for analysis.

Built as a portfolio project to demonstrate data engineering fundamentals ahead of a role in media analytics. The pipeline architecture mirrors real-world media measurement workflows — multi-source ingestion, scheduled orchestration, data quality gates, and idempotent loading.

---

## What it does

Every Monday at 06:00 UTC, the pipeline:

1. Verifies the database connection is healthy
2. Extracts brand attention signals from three sources in parallel
3. Normalises and aligns them to a common weekly schema
4. Runs data quality checks
5. Upserts results into PostgreSQL

Brands tracked: Nike, Coca-Cola, Netflix, Apple, Samsung.

---

## Data sources and why they are proxies

Real audience and media data (TV ratings, streaming viewership, ad exposure) is proprietary. The sources used here are publicly available proxies that approximate the same concepts:

| Source | Library | What it measures | Proxies |
|---|---|---|---|
| Wikipedia Pageviews API | `wikipedia-api` | Weekly page views per brand | Public attention / brand salience |
| Google Trends | `pytrends` | Relative search interest | Search-driven brand awareness |
| Market data | `yfinance` | Weekly close price | Contextual / environmental variable |

This is documented honestly. The pipeline architecture is identical to what you would build with real data — only the source changes.

---

## Tech stack

| Tool | Role |
|---|---|
| Apache Airflow 2.9.2 | Orchestration and scheduling |
| Docker + Docker Compose | Local infrastructure |
| Python 3.12 | Extraction and transformation |
| PostgreSQL 15 | Project data storage |
| PostgreSQL 13 | Airflow internal metadata |

---

## Project structure

media-pulse-pipeline/
├── dags/
│   └── media_pulse_weekly.py   # Main DAG definition
├── sql/
│   └── init.sql                # Schema + seed data (runs on first boot)
├── plugins/                    # Custom operators (empty for now)
├── config/                     # Airflow config overrides (empty for now)
├── docker-compose.yaml         # Full local infrastructure
├── requirements.txt            # Pinned dependencies (lockfile)
├── requirements-dev.txt        # Direct dependencies (human-readable)
└── .env.example                # Environment variable template

---

## How to run locally

**Prerequisites:** Docker Desktop, Git.

```bash
git clone https://github.com/mseijse01/media-pulse-pipeline.git
cd media-pulse-pipeline

# Set your user ID (Mac/Linux)
echo "AIRFLOW_UID=$(id -u)" > .env

# Initialise Airflow metadata database
docker compose up airflow-init

# Start everything
docker compose up -d

# Verify all containers are healthy
docker compose ps
```

Open `http://localhost:8080` — login with `airflow` / `airflow`.

The `project_postgres` connection and project schema are created automatically. No manual setup required.

---

## Database schema

```sql
brands                    -- dimension table (static)
  brand_id   SERIAL PK
  brand_name VARCHAR

brand_weekly_metrics      -- fact table (grows weekly)
  brand_id   FK → brands
  week_start DATE
  wikipedia_views  BIGINT
  trends_score     NUMERIC
  market_close     NUMERIC
  UNIQUE (brand_id, week_start)  -- idempotency constraint
```

---

## Project status

| Phase | Description | Status |
|---|---|---|
| Phase 1 | Infrastructure — Airflow, Docker, Postgres, DAG skeleton | ✅ Complete |
| Phase 2 | Data layer — real extractors, transformation, idempotent load | 🔄 Next |
| Phase 3 | Improvements — informed by Phase 2 (dbt, visualisation) | Planned |

---

## Background

This project was built as preparation for a role as Audience & Consumer Insights Specialist at Nielsen Research Media. The goal was to demonstrate the ability to build and maintain data pipelines — not just analyse data, but move it reliably, on a schedule, with quality checks. The tech stack (Airflow, Python, PostgreSQL, Docker) mirrors Nielsen's toolchain directly.