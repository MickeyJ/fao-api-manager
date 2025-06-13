SELECT 
    ac.id as area_id,
    ac.area as area_name,
    ac.area_code,
    p.year,
    p.value / er.value as price,  -- Convert LCU to USD
    'USD' as unit,                 -- Converted to USD
    ic.item as item_name,
    ic.item_code,
    ic.id as item_id
FROM prices p
JOIN item_codes ic ON ic.id = p.item_code_id
JOIN area_codes ac ON ac.id = p.area_code_id
JOIN elements e ON e.id = p.element_code_id
JOIN flags f ON f.id = p.flag_id
-- Join exchange rate via area_code string matching
JOIN area_codes ac_er ON ac_er.area_code = ac.area_code
JOIN exchange_rate er ON 
    er.area_code_id = ac_er.id
    AND er.year = p.year
    AND er.months_code = '7021'  -- Annual rates
    AND er.element_code_id = (SELECT id FROM elements WHERE element_code = 'LCU')
WHERE 
    e.element_code = '5530'  -- LCU prices
    AND f.flag = 'A'         -- Official figures only
    AND p.months_code = '7021'  -- Annual prices only
    AND er.value > 0         -- Valid exchange rates
    -- Exclude Euro countries during transition period
    AND NOT (
        ac.area_code IN ('11', '15', '67', '68', '79', '134', '106', '174', '203', '255', '256')
        AND p.year BETWEEN 1999 AND 2001
    )
ORDER BY ac.area_code, p.year;