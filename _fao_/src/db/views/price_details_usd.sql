DROP MATERIALIZED VIEW IF EXISTS price_details_usd CASCADE;
CREATE MATERIALIZED VIEW price_details_usd AS
SELECT 
    ac.id as area_id,
    ac.area as area_name,
    ac.area_code,
    p.year,
    p.value as price,
    p.unit,
    ic.item as item_name,
    ic.item_code,
    ic.id as item_id
FROM prices p
JOIN item_codes ic ON ic.id = p.item_code_id
JOIN area_codes ac ON ac.id = p.area_code_id
JOIN elements e ON e.id = p.element_code_id
JOIN flags f ON f.id = p.flag_id
WHERE 
    e.element_code = '5532'  -- USD prices
    AND f.flag != 'I'
    AND p.months_code = '7021'  -- Annual prices only
ORDER BY ac.area_code, p.year
WITH NO DATA;