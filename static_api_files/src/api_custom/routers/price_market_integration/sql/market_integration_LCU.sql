WITH annual_prices AS (
    SELECT
        ac.id as area_id,
        ac.area_code,
        ac.area as country_name,
        p.year,
        AVG(p.value) as price_lcu
    FROM prices p
    JOIN area_codes ac ON p.area_code_id = ac.id
    JOIN item_codes ic ON p.item_code_id = ic.id
    JOIN elements e ON p.element_code_id = e.id
    JOIN flags f ON p.flag_id = f.id
    WHERE
        f.flag = 'A'
        AND e.element_code = :element_code
        AND ic.item_code = :item_code
        AND p.year >= :start_year
        AND ac.area_code = ANY(:selected_area_codes)
        AND p.months_code = '7021'
    GROUP BY ac.id, ac.area_code, ac.area, p.year
),
normalized_prices AS (
    SELECT 
        ap.*,
        ap.price_lcu / er.value as price
    FROM annual_prices ap
    INNER JOIN exchange_rate er ON 
        er.year = ap.year
        AND er.element_code_id = (SELECT id FROM elements WHERE element_code = 'LCU')
        AND er.months_code = '7021'
    INNER JOIN area_codes ac_er ON  -- Join to get the exchange rate's area_code
        er.area_code_id = ac_er.id 
        AND ac_er.area_code = ap.area_code  -- Match on the string code, not ID
    WHERE er.value > 0
),
price_ratios AS (
    SELECT
        p1.area_id as country1_id,
        p1.area_code as country1_code,
        p1.country_name as country1,
        p2.area_id as country2_id,
        p2.area_code as country2_code,
        p2.country_name as country2,
        p1.year,
        p1.price as price1,  -- Now in USD
        p2.price as price2,  -- Now in USD
        p1.price / NULLIF(p2.price, 0) as price_ratio
    FROM normalized_prices p1
    JOIN normalized_prices p2
        ON p1.year = p2.year
        AND p1.area_code < p2.area_code
)
-- Rest remains exactly the same
SELECT
    country1,
    country2,
    country1_id,
    country2_id,
    country1_code,
    country2_code,
    JSON_AGG(
        JSON_BUILD_OBJECT(
            'year', year,
            'price1', price1,
            'price2', price2,
            'ratio', ROUND(price_ratio::numeric, 3)
        ) ORDER BY year
    ) as time_series,
    COUNT(*) as years_compared,
    ROUND(AVG(price_ratio)::numeric, 3) as avg_ratio,
    ROUND(STDDEV(price_ratio)::numeric, 3) as ratio_volatility,
    ROUND(MIN(price_ratio)::numeric, 3) as min_ratio,
    ROUND(MAX(price_ratio)::numeric, 3) as max_ratio,
    CASE 
        WHEN STDDEV(price_ratio) < 0.1 THEN 'high'
        WHEN STDDEV(price_ratio) < 0.2 THEN 'moderate'
        WHEN STDDEV(price_ratio) < 0.3 THEN 'low'
        ELSE 'none'
    END as integration_level
FROM price_ratios
GROUP BY country1, country2, country1_id, country2_id, country1_code, country2_code;