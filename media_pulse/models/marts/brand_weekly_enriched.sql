with base as (
    select * from {{ ref('stg_brand_weekly_metrics') }}
),

enriched as (
    select
        brand_name,
        week_start,
        wikipedia_views,
        trends_score,
        market_close_usd,
        market_currency,

        lag(wikipedia_views) over (
            partition by brand_name order by week_start
        ) as prev_wikipedia_views,

        round(
            100.0 * (wikipedia_views - lag(wikipedia_views) over (
                partition by brand_name order by week_start
            )) / nullif(lag(wikipedia_views) over (
                partition by brand_name order by week_start
            ), 0),
        2) as wikipedia_wow_pct,

        round(avg(wikipedia_views) over (
            partition by brand_name
            order by week_start
            rows between 3 preceding and current row
        ), 0) as wikipedia_4w_avg,

        lag(market_close_usd) over (
            partition by brand_name order by week_start
        ) as prev_market_close_usd,

        round(
            100.0 * (market_close_usd - lag(market_close_usd) over (
                partition by brand_name order by week_start
            )) / nullif(lag(market_close_usd) over (
                partition by brand_name order by week_start
            ), 0),
        2) as market_wow_pct,

        loaded_at

    from base
)

select * from enriched