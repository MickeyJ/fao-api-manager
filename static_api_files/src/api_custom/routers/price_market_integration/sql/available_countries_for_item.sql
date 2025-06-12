WITH country_price_stats AS (
    SELECT 
        ac.id as area_id,
        ac.area_code,
        ac.area as area_name,
        COUNT(DISTINCT p.year) as years_with_data,
        -- Count only annual records for fair comparison
        COUNT(CASE WHEN p.months = 'Annual value' OR p.months_code = '7021' THEN 1 END) as annual_records,
        MIN(p.year) as first_year,
        MAX(p.year) as last_year,
        AVG(CASE WHEN p.months = 'Annual value' OR p.months_code = '7021' THEN p.value END) as avg_price
    FROM prices p
    JOIN area_codes ac ON p.area_code_id = ac.id
    JOIN item_codes ic ON p.item_code_id = ic.id
    JOIN elements e ON p.element_code_id = e.id
    WHERE 
        ic.item_code = :item_code
        AND e.element_code IN ('5532')
        AND p.year >= 2010
    GROUP BY ac.id, ac.area_code, ac.area
),
ranked_countries AS (
    SELECT 
        *,
        DENSE_RANK() OVER (ORDER BY annual_records DESC, years_with_data DESC) as data_rank
    FROM country_price_stats
    WHERE annual_records >= 5  -- At least 5 years of annual data
)
SELECT 
    area_id,
    area_code,
    area_name,
    years_with_data,
    annual_records,
    first_year || '-' || last_year as year_range,
    ROUND(avg_price::numeric, 2) as avg_price
FROM ranked_countries
WHERE data_rank <= 40
ORDER BY data_rank, area_name;