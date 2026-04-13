CREATE TABLE IF NOT EXISTS brands (
    brand_id    SERIAL PRIMARY KEY,
    brand_name  VARCHAR(100) NOT NULL UNIQUE,
    created_at  TIMESTAMP DEFAULT NOW()
);

INSERT INTO brands (brand_name) VALUES
    ('Nike'),
    ('Coca-Cola'),
    ('Netflix'),
    ('Apple'),
    ('Samsung')
ON CONFLICT (brand_name) DO NOTHING;

CREATE TABLE IF NOT EXISTS brand_weekly_metrics (
    id                      SERIAL PRIMARY KEY,
    brand_id                INTEGER NOT NULL REFERENCES brands(brand_id),
    week_start              DATE NOT NULL,
    wikipedia_views         BIGINT,
    trends_score            NUMERIC(5,2),
    market_close            NUMERIC(10,4),
    market_close_usd        NUMERIC(10,4),
    market_currency         VARCHAR(3) DEFAULT 'USD',
    loaded_at               TIMESTAMP DEFAULT NOW(),
    UNIQUE (brand_id, week_start)
);

CREATE INDEX IF NOT EXISTS idx_metrics_brand_week
    ON brand_weekly_metrics(brand_id, week_start);