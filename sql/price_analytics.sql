DROP MATERIALIZED VIEW IF EXISTS price_analytics CASCADE;

CREATE MATERIALIZED VIEW price_analytics AS
SELECT 
    area_code,
    item_code,
    year,
    month,
    value as price,
    
    -- Period-over-period changes
    LAG(value, 1) OVER w as prev_month_price,
    LAG(value, 12) OVER w as prev_year_price,
    
    -- Percentage changes
    ((value - LAG(value, 1) OVER w) / NULLIF(LAG(value, 1) OVER w, 0)) * 100 as mom_change,
    ((value - LAG(value, 12) OVER w) / NULLIF(LAG(value, 12) OVER w, 0)) * 100 as yoy_change,
    
    -- Moving averages
    AVG(value) OVER (PARTITION BY area_code, item_code 
                     ORDER BY year, month 
                     ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as ma3,
    AVG(value) OVER (PARTITION BY area_code, item_code 
                     ORDER BY year, month 
                     ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) as ma12,
    
    -- Volatility (rolling std dev)
    STDDEV(value) OVER (PARTITION BY area_code, item_code 
                        ORDER BY year, month 
                        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) as volatility_12m,
    
    -- Z-score for anomaly detection
    (value - AVG(value) OVER (PARTITION BY area_code, item_code)) / 
    NULLIF(STDDEV(value) OVER (PARTITION BY area_code, item_code), 0) as zscore

FROM prices
WINDOW w AS (PARTITION BY area_code, item_code ORDER BY year, month);

-- Add indexes for common queries
CREATE INDEX idx_analytics_item ON price_analytics(item_code);
CREATE INDEX idx_analytics_area ON price_analytics(area_code);
CREATE INDEX idx_analytics_anomaly ON price_analytics(zscore) WHERE ABS(zscore) > 2;