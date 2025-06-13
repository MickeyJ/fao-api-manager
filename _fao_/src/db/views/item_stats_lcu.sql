DROP MATERIALIZED VIEW IF EXISTS item_stats_lcu CASCADE;
CREATE MATERIALIZED VIEW item_stats_lcu AS
SELECT 
    ic.id,
    ic.item as name,
    ic.item_code,
    ic.item_code_cpc as cpc_code,
    COUNT(DISTINCT p.id) as price_points,
    COUNT(DISTINCT p.area_code_id) as countries_with_data,
    COUNT(DISTINCT p.year) as years_with_data,
    MIN(p.year) as earliest_year,
    MAX(p.year) as latest_year,
    -- Average data points per country (indicates data density)
    (COUNT(p.id)::numeric / COUNT(DISTINCT p.area_code_id)) as avg_points_per_country
FROM item_codes ic
JOIN prices p ON ic.id = p.item_code_id
JOIN elements e ON e.id = p.element_code_id
JOIN flags f ON f.id = p.flag_id
WHERE 
    e.element_code = '5530'  -- LCU prices
    AND ic.source_dataset = 'prices'
    AND f.flag != 'I'
    AND p.year >= 1990
GROUP BY ic.id, ic.item, ic.item_code, ic.item_code_cpc
HAVING 
    COUNT(DISTINCT p.area_code_id) >= 10  -- At least 10 countries
    AND COUNT(DISTINCT p.year) >= 10       -- At least 10 years of data
ORDER BY 
    COUNT(DISTINCT p.area_code_id) DESC,  -- Most countries first
    COUNT(DISTINCT p.year) DESC,          -- Then most years
    COUNT(p.id) DESC
WITH NO DATA;