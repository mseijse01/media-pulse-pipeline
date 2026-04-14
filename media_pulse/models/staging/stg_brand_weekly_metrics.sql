with source as (
    select
        m.id,
        b.brand_name,
        m.week_start,
        m.wikipedia_views,
        m.trends_score,
        m.market_close,
        m.market_close_usd,
        m.market_currency,
        m.loaded_at
    from public.brand_weekly_metrics m
    join public.brands b on b.brand_id = m.brand_id
)

select * from source