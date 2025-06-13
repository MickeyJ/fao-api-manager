DROP MATERIALIZED VIEW IF EXISTS price_ratios_lcu CASCADE;
CREATE MATERIALIZED VIEW price_ratios_lcu AS
WITH annual_prices AS (
    SELECT
        ac.id as area_id,
        ac.area_code,
        ac.area as country_name,
        ic.item_code,
        ic.item as item_name,
        p.year,
        AVG(p.value) as price_lcu
    FROM prices p
    JOIN area_codes ac ON p.area_code_id = ac.id
    JOIN item_codes ic ON p.item_code_id = ic.id
    JOIN elements e ON p.element_code_id = e.id
    JOIN flags f ON p.flag_id = f.id
    WHERE
        f.flag != 'I'
        AND e.element_code = '5530'  -- LCU prices only
        AND p.months_code = '7021'   -- Annual average
        -- Exclude Euro countries during transition period
        AND NOT (
            ac.area_code IN ('11', '15', '67', '68', '79', '134', '106', '174', '203', '255', '256')
            AND p.year BETWEEN 1999 AND 2001
        )
    GROUP BY ac.id, ac.area_code, ac.area, ic.item_code, ic.item, p.year
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
    INNER JOIN area_codes ac_er ON
        er.area_code_id = ac_er.id 
        AND ac_er.area_code = ap.area_code
    WHERE er.value > 0
)
SELECT
    p1.area_id as country1_id,
    p1.area_code as country1_code,
    p1.country_name as country1,
    p2.area_id as country2_id,
    p2.area_code as country2_code,
    p2.country_name as country2,
    p1.item_code,
    p1.item_name,
    p1.year,
    p1.price as price1,
    p2.price as price2,
    ROUND((p1.price / NULLIF(p2.price, 0))::numeric, 3) as price_ratio
FROM normalized_prices p1
JOIN normalized_prices p2
    ON p1.year = p2.year
    AND p1.item_code = p2.item_code
    AND p1.area_code < p2.area_code
WHERE p2.price > 0
WITH NO DATA;