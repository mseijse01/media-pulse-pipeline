"""
Media Pulse Pipeline — Brand Attention Dashboard
Phase 6: Streamlit visualisation layer

Queries analytics.brand_weekly_enriched (dbt mart) directly.
Connection string read from DATABASE_URL env var with local fallback —
switching to Supabase requires only setting that env var, no code changes.
"""

import os
import psycopg2
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Media Pulse Pipeline",
    page_icon="📡",
    layout="wide",
)

# ── Connection ─────────────────────────────────────────────────────────────────
# One env var to change when moving to Supabase. That's the whole migration.
DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://mediapulse:mediapulse@localhost:5433/mediapulse"
)


@st.cache_data(
    ttl=300
)  # cache for 5 minutes — avoids hammering DB on every widget interaction
def load_data() -> pd.DataFrame:
    """Load all data from the dbt mart. Returns a clean DataFrame."""
    conn = psycopg2.connect(DATABASE_URL)
    df = pd.read_sql(
        """
        SELECT
            brand_name,
            week_start,
            wikipedia_views,
            trends_score,
            market_close_usd,
            market_currency,
            wikipedia_wow_pct,
            wikipedia_4w_avg,
            market_wow_pct
        FROM analytics.brand_weekly_enriched
        ORDER BY brand_name, week_start
        """,
        conn,
        parse_dates=["week_start"],
    )
    conn.close()
    return df


# ── Load data ──────────────────────────────────────────────────────────────────
try:
    df = load_data()
except Exception as e:
    st.error(f"Could not connect to database: {e}")
    st.info(
        "Make sure your local Postgres is running on port 5433 (`docker compose up -d`)"
    )
    st.stop()

# ── Sidebar — filters ──────────────────────────────────────────────────────────
st.sidebar.title("📡 Media Pulse")
st.sidebar.markdown("Weekly brand attention signals · Oct 2025 – Apr 2026")

all_brands = sorted(df["brand_name"].unique())
selected_brands = st.sidebar.multiselect(
    "Brands",
    options=all_brands,
    default=all_brands,
)

min_date = df["week_start"].min()
max_date = df["week_start"].max()
date_range = st.sidebar.date_input(
    "Date range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)

# Guard: date_input returns a tuple only once both ends are selected
if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date, end_date = min_date, max_date

# Apply filters
filtered = df[
    df["brand_name"].isin(selected_brands)
    & (df["week_start"] >= pd.Timestamp(start_date))
    & (df["week_start"] <= pd.Timestamp(end_date))
].copy()

st.sidebar.markdown("---")
st.sidebar.caption(
    f"{len(filtered)} rows · {filtered['brand_name'].nunique()} brands · {filtered['week_start'].nunique()} weeks"
)

# ── Header ─────────────────────────────────────────────────────────────────────
st.title("Media Pulse Pipeline")
st.markdown(
    "Weekly brand attention signals across Wikipedia pageviews, Google Trends, and market close. "
    "Data sourced from public APIs; see [GitHub](https://github.com/mseijse01/media-pulse-pipeline) for methodology."
)

if filtered.empty:
    st.warning("No data for the selected filters.")
    st.stop()

# ── Row 1: Wikipedia views + Market close ─────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("Wikipedia Views Over Time")
    st.caption("Weekly pageviews — proxy for public brand attention / salience")

    fig_wiki = px.line(
        filtered,
        x="week_start",
        y="wikipedia_views",
        color="brand_name",
        labels={
            "week_start": "Week",
            "wikipedia_views": "Wikipedia Views",
            "brand_name": "Brand",
        },
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    fig_wiki.update_layout(
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(t=40, b=20),
        hovermode="x unified",
    )
    st.plotly_chart(fig_wiki, use_container_width=True)

with col2:
    st.subheader("Market Close (USD) Over Time")
    st.caption("Samsung converted KRW→USD for cross-brand comparability")

    fig_market = px.line(
        filtered,
        x="week_start",
        y="market_close_usd",
        color="brand_name",
        labels={
            "week_start": "Week",
            "market_close_usd": "Close Price (USD)",
            "brand_name": "Brand",
        },
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    fig_market.update_layout(
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(t=40, b=20),
        hovermode="x unified",
    )
    st.plotly_chart(fig_market, use_container_width=True)

# ── Row 2: WoW heatmap ────────────────────────────────────────────────────────
st.subheader("Wikipedia Week-over-Week Change (%)")
st.caption("Red = attention drop · Green = attention spike · White = no change")

# Pivot to brand × week matrix for heatmap
heatmap_data = (
    filtered[["brand_name", "week_start", "wikipedia_wow_pct"]]
    .dropna(subset=["wikipedia_wow_pct"])
    .assign(week_label=lambda x: x["week_start"].dt.strftime("%b %d"))
)

pivot = heatmap_data.pivot(
    index="brand_name", columns="week_label", values="wikipedia_wow_pct"
)

# Preserve chronological column order
week_order = (
    heatmap_data[["week_start", "week_label"]]
    .drop_duplicates()
    .sort_values("week_start")["week_label"]
    .tolist()
)
pivot = pivot[[c for c in week_order if c in pivot.columns]]

fig_heat = go.Figure(
    data=go.Heatmap(
        z=pivot.values,
        x=pivot.columns.tolist(),
        y=pivot.index.tolist(),
        colorscale="RdYlGn",
        zmid=0,
        colorbar=dict(title="WoW %"),
        hovertemplate="Brand: %{y}<br>Week: %{x}<br>WoW: %{z:.1f}%<extra></extra>",
    )
)
fig_heat.update_layout(
    margin=dict(t=20, b=40),
    xaxis=dict(tickangle=-45),
    height=250,
)
st.plotly_chart(fig_heat, use_container_width=True)

# ── Row 3: Latest week snapshot + Insight panel ───────────────────────────────
col3, col4 = st.columns([1.2, 1])

with col3:
    st.subheader("Latest Week Snapshot")

    latest_week = filtered["week_start"].max()
    latest = (
        filtered[filtered["week_start"] == latest_week]
        .set_index("brand_name")[
            [
                "wikipedia_views",
                "wikipedia_wow_pct",
                "wikipedia_4w_avg",
                "market_close_usd",
                "market_wow_pct",
            ]
        ]
        .rename(
            columns={
                "wikipedia_views": "Wiki Views",
                "wikipedia_wow_pct": "Wiki WoW %",
                "wikipedia_4w_avg": "4w Avg Views",
                "market_close_usd": "Market (USD)",
                "market_wow_pct": "Market WoW %",
            }
        )
    )

    st.caption(f"Week of {latest_week.strftime('%B %d, %Y')}")
    latest.index.name = None  # removes "brand_name" label from index column
    st.dataframe(
        latest.style.format(
            {
                "Wiki Views": "{:,.0f}",
                "Wiki WoW %": "{:+.1f}%",
                "4w Avg Views": "{:,.0f}",
                "Market (USD)": "${:.2f}",
                "Market WoW %": "{:+.1f}%",
            }
        ).background_gradient(
            subset=["Wiki WoW %", "Market WoW %"], cmap="RdYlGn", vmin=-20, vmax=20
        ),
        use_container_width=True,
    )

with col4:
    st.subheader("Analytical Finding")
    st.caption("From lead-lag correlation analysis · Phase 5")

    st.info(
        """
        **Wikipedia attention → market movement is brand-specific, not universal.**

        Correlating weekly Wikipedia views with next-week market price change (n=26 weeks):

        - **Nike +0.41** — strongest positive signal. Higher attention correlates with market gains same week, weakly leads next week.
        - **Samsung +0.47** — strong same-week correlation but no lead relationship. Attention and market move together (shared news driver), not sequentially.
        - **Netflix −0.28** — consistent negative correlation. Wikipedia spikes track controversy, not positive momentum.
        - **Apple +0.11, Coca-Cola ~0** — weak to no relationship.

        **Pooled across all brands: R²=0.003, p=0.535 — no significant signal.** Brand-specific effects cancel out when aggregated. The signal only appears when you disaggregate.

        This mirrors the input diagnostic phase of Marketing Mix Modelling — understanding which variables move together before building the full model.
        """
    )

# ── Footer ─────────────────────────────────────────────────────────────────────
st.markdown("---")
st.caption(
    "Media Pulse Pipeline · Built with Airflow, dbt, PostgreSQL, Streamlit · "
    "[GitHub](https://github.com/mseijse01/media-pulse-pipeline)"
)
