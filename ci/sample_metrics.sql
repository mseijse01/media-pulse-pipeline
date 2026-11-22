-- Synthetic fixture rows for CI only: three weeks for each brand, so the
-- dbt models (including the week-over-week and rolling-average logic) and
-- their tests run against real rows rather than an empty table.
-- These are NOT real pipeline numbers.
INSERT INTO brand_weekly_metrics
    (brand_id, week_start, wikipedia_views, trends_score, market_close, market_close_usd, market_currency)
SELECT b.brand_id, w.week_start,
       10000 + 1000 * b.brand_id + 250 * w.n,
       NULL,
       CASE WHEN b.brand_name = 'Samsung' THEN 75000 + 500 * w.n ELSE 100 + 5 * w.n END,
       CASE WHEN b.brand_name = 'Samsung' THEN 52.5 + 0.35 * w.n ELSE 100 + 5 * w.n END,
       CASE WHEN b.brand_name = 'Samsung' THEN 'KRW' ELSE 'USD' END
FROM brands b
CROSS JOIN (VALUES (0, DATE '2026-01-05'), (1, DATE '2026-01-12'), (2, DATE '2026-01-19')) AS w(n, week_start)
ON CONFLICT (brand_id, week_start) DO NOTHING;
