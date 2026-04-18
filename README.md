# Media Pulse Pipeline

A weekly data pipeline that tracks brand attention signals for five global brands across three public data sources, normalises them into PostgreSQL, transforms them with dbt, and surfaces insights via a Streamlit dashboard.

Built as a portfolio project to demonstrate data engineering fundamentals ahead of a role in media analytics. The pipeline architecture mirrors real-world media measurement workflows: multi-source ingestion, scheduled orchestration, idempotent loading, a transformation layer with tests, and an analytical visualisation layer.

---

## What it does

Every Monday at 06:00 UTC, the pipeline:

1. Verifies the database connection
2. Extracts brand attention signals from three sources **in parallel**
3. Normalises and aligns them to a common weekly schema
4. Runs data quality checks
5. Upserts results into PostgreSQL (idempotent — safe to re-run)

Brands tracked: **Nike, Coca-Cola, Netflix, Apple, Samsung**

---

## Data sources

Real audience and media data (TV ratings, streaming viewership, ad exposure) is proprietary. The sources used here are publicly available proxies that approximate the same concepts:

| Source | Library | What it measures | Proxies for |
|---|---|---|---|
| Wikipedia Pageviews API | `requests` | Weekly page views per brand article | Public attention / brand salience |
| Google Trends | `pytrends` | Relative search interest (0–100) | Search-driven brand awareness |
| Market data | `yfinance` | Weekly closing price | Brand value / contextual signal |

This is documented honestly. The pipeline architecture is identical to what you would build with real proprietary data — only the source APIs change.

**Note on Google Trends:** `pytrends` is rate-limited aggressively. `trends_score` is null for most historical rows and is populated incrementally by the weekly pipeline going forward.

**Note on Samsung:** Samsung trades on the Korea Stock Exchange in KRW. The pipeline fetches the USD/KRW spot rate weekly and stores both the raw KRW value (`market_close`) and the converted USD value (`market_close_usd`). Always use `market_close_usd` for cross-brand comparisons.

---

## Tech stack

| Layer | Tool | Role |
|---|---|---|
| Orchestration | Apache Airflow 2.9.2 | DAG scheduling, task dependencies, retry logic |
| Infrastructure | Docker + Docker Compose | 7-container local environment |
| Storage | PostgreSQL 15 (port 5433) | Project database |
| Transformation | dbt 1.8.0 | SQL models, tests, documentation |
| Analysis | Jupyter + pandas + statsmodels | Lead-lag correlation, OLS regression |
| Visualisation | Streamlit + Plotly | Interactive brand attention dashboard |
| Language | Python 3.12 | Extraction and transformation logic |

---

## Project structure

```
media-pulse-pipeline/
├── dags/
│   └── media_pulse_weekly.py      # Main DAG — extraction, normalisation, load
├── media_pulse/                   # dbt project
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_brand_weekly_metrics.sql   # view: joins brands + raw metrics
│   │   │   └── stg_brand_weekly_metrics.yml   # column docs + tests
│   │   └── marts/
│   │       ├── brand_weekly_enriched.sql       # table: WoW%, 4w rolling avg
│   │       └── brand_weekly_enriched.yml       # column docs + tests
│   └── dbt_project.yml
├── notebooks/
│   └── brand_attention_analysis.ipynb          # lead-lag correlation + OLS regression
├── scripts/
│   └── backfill.py                # 26-week historical backfill (CLI)
├── sql/
│   └── init.sql                   # Schema + seed data (runs on first boot)
├── streamlit/
│   └── app.py                     # Brand attention dashboard
├── docker-compose.yaml
├── Dockerfile                     # Custom Airflow image with pipeline dependencies
├── requirements.txt               # Pinned lockfile
├── requirements-dev.txt           # Direct dependencies
└── .env.example                   # Environment variable template
```

---

## How to run locally

**Prerequisites:** Docker Desktop, Git, Python 3.12.

```bash
git clone https://github.com/mseijse01/media-pulse-pipeline.git
cd media-pulse-pipeline

# Set your user ID (required for Airflow file permissions on Mac/Linux)
echo "AIRFLOW_UID=$(id -u)" > .env

# Start everything (first run initialises the Airflow metadata DB automatically)
docker compose up -d

# Verify all containers are healthy
docker compose ps
```

Open `http://localhost:8080` — login with `airflow` / `airflow`.

The `project_postgres` connection and project schema are created automatically on first boot. No manual setup required.

---

## Python environments

Two separate virtual environments are required due to incompatible dependency trees:

```bash
# Pipeline work, backfill script, Jupyter, Streamlit
source .venv-airflow-3.12/bin/activate

# dbt only
source .venv-dbt/bin/activate
cd media_pulse
```

> **Important:** Shut down Jupyter kernels before running `dbt run`. Open Jupyter connections hold locks on the `analytics` schema that prevent dbt from dropping and recreating its materialised tables.

---

## dbt models

```bash
source .venv-dbt/bin/activate
cd media_pulse
dbt run    # build staging view + enriched mart table
dbt test   # run column-level tests (not_null, accepted_values)
```

| Model | Materialisation | Description |
|---|---|---|
| `stg_brand_weekly_metrics` | view | Joins brands + raw metrics; no transformations |
| `brand_weekly_enriched` | table | Adds WoW % change, 4-week rolling avg for Wikipedia and market |

---

## Historical backfill

The pipeline starts from the current week. To populate historical data:

```bash
source .venv-airflow-3.12/bin/activate
python scripts/backfill.py --weeks 26   # last 26 weeks
python scripts/backfill.py --dry-run    # print dates without loading
```

Google Trends is excluded from the backfill due to rate limiting. Trends scores populate incrementally as the weekly pipeline runs.

---

## Streamlit dashboard

```bash
source .venv-airflow-3.12/bin/activate
streamlit run streamlit/app.py
```

Opens at `http://localhost:8501`. Queries `analytics.brand_weekly_enriched` directly.

Panels:
- Wikipedia views over time (line chart, all brands)
- Market close over time in USD (line chart, Samsung normalised)
- Wikipedia week-over-week change heatmap (brand × week)
- Latest week snapshot table
- Analytical finding from the Phase 5 correlation analysis

---

## Analytical findings

The notebook (`notebooks/brand_attention_analysis.ipynb`) asks: **does Wikipedia attention predict market price movement the following week?**

Key result from lead-lag correlation analysis (n=26 weeks, Oct 2025–Apr 2026):

| Brand | Same-week correlation | Wiki leads market (+1w) |
|---|---|---|
| Nike | +0.41 | +0.06 |
| Samsung | +0.47 | −0.06 |
| Netflix | −0.28 | −0.23 |
| Apple | +0.11 | +0.10 |
| Coca-Cola | +0.07 | −0.10 |

**The relationship is brand-specific, not universal.** Pooled across all brands: R²=0.003, p=0.535 — no significant signal. Brand-specific effects cancel out when aggregated. Netflix's negative correlation is interpretable: Wikipedia spikes for Netflix track controversy, not positive momentum.

This analysis is structurally identical to the input diagnostic phase of Marketing Mix Modelling — understanding which variables move together before building the full model.

---

## Database schema

```sql
-- Raw layer (pipeline writes here)
public.brands                    -- dimension table, 5 rows
  brand_id    SERIAL PRIMARY KEY
  brand_name  VARCHAR

public.brand_weekly_metrics      -- fact table
  brand_id         FK → brands
  week_start       DATE
  wikipedia_views  BIGINT
  trends_score     NUMERIC        -- null for most historical rows
  market_close     NUMERIC        -- native currency (KRW for Samsung)
  market_close_usd NUMERIC        -- always USD
  market_currency  VARCHAR        -- 'USD' or 'KRW'
  UNIQUE (brand_id, week_start)   -- idempotency constraint

-- Analytical layer (dbt writes here)
analytics.stg_brand_weekly_metrics   -- view: join + clean column names
analytics.brand_weekly_enriched      -- table: + WoW%, 4w rolling avg
```

---

## Project status

| Phase | Description | Status |
|---|---|---|
| Phase 1 | Infrastructure — Airflow, Docker, Postgres, DAG skeleton | ✅ Complete |
| Phase 2 | Data layer — real extractors, normalisation, idempotent load | ✅ Complete |
| Phase 3 | Robustness — Wikipedia real counts, Samsung FX, Trends caching, custom image | ✅ Complete |
| Phase 4 | Depth — 26-week backfill, dbt transformation layer | ✅ Complete |
| Phase 5 | Analysis — lead-lag correlation, OLS regression, key findings | ✅ Complete |
| Phase 6 | Visualisation — Streamlit dashboard | ✅ Complete |
| Phase 7 | Deployment — Supabase + Streamlit Cloud | Planned |
