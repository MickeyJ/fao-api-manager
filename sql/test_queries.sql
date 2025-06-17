-- -- Which countries have the most annual LCU price records?
-- SELECT 
--     ac.area_code,
--     ac.area as country_name,
--     COUNT(DISTINCT ic.item_code) as unique_commodities,
--     COUNT(*) as total_price_records,
--     MIN(p.year) as earliest_year,
--     MAX(p.year) as latest_year,
--     MAX(p.year) - MIN(p.year) + 1 as year_span
-- FROM prices p
-- JOIN area_codes ac ON p.area_code_id = ac.id
-- JOIN item_codes ic ON p.item_code_id = ic.id
-- JOIN elements e ON p.element_code_id = e.id
-- WHERE e.element_code = '5530'  -- Producer Prices (LCU)
--   AND p.months_code = '7021'    -- Annual average
-- GROUP BY ac.area_code, ac.area
-- HAVING COUNT(*) > 1000          -- Countries with substantial data
-- ORDER BY unique_commodities DESC, total_price_records DESC
-- LIMIT 20;


-- -- Check which countries have good coverage in both tables
-- WITH modern_prices AS (
--     SELECT 
--         ac.area_code,
--         ac.area as country_name,
--         COUNT(DISTINCT ic.item_code) as commodities_modern,
--         MIN(p.year) as min_year_modern,
--         MAX(p.year) as max_year_modern
--     FROM prices p
--     JOIN area_codes ac ON p.area_code_id = ac.id
--     JOIN item_codes ic ON p.item_code_id = ic.id
--     JOIN elements e ON p.element_code_id = e.id
--     WHERE e.element_code = '5530'
--       AND p.months_code = '7021'
--     GROUP BY ac.area_code, ac.area
-- ),
-- archive_prices AS (
--     SELECT 
--         ac.area_code,
--         ac.area as country_name,
--         COUNT(DISTINCT ic.item_code) as commodities_archive,
--         MIN(pa.year) as min_year_archive,
--         MAX(pa.year) as max_year_archive
--     FROM prices_archive pa
--     JOIN area_codes ac ON pa.area_code_id = ac.id
--     JOIN item_codes ic ON pa.item_code_id = ic.id
--     JOIN elements e ON pa.element_code_id = e.id
--     WHERE e.element_code = '5530'
--     GROUP BY ac.area_code, ac.area
-- )
-- SELECT 
--     COALESCE(m.area_code, a.area_code) as area_code,
--     COALESCE(m.country_name, a.country_name) as country_name,
--     COALESCE(m.commodities_modern, 0) as commodities_1991_2023,
--     COALESCE(a.commodities_archive, 0) as commodities_archive,
--     COALESCE(a.min_year_archive, m.min_year_modern) as earliest_data,
--     COALESCE(m.max_year_modern, a.max_year_archive) as latest_data,
--     CASE 
--         WHEN a.commodities_archive > 0 AND m.commodities_modern > 0 THEN 'Both'
--         WHEN a.commodities_archive > 0 THEN 'Archive only'
--         WHEN m.commodities_modern > 0 THEN 'Modern only'
--     END as data_source
-- FROM modern_prices m
-- FULL OUTER JOIN archive_prices a 
--     ON m.area_code = a.area_code
-- WHERE COALESCE(m.commodities_modern, 0) > 50 
--    OR COALESCE(a.commodities_archive, 0) > 50
-- ORDER BY 
--     COALESCE(m.commodities_modern, 0) + COALESCE(a.commodities_archive, 0) DESC,
--     commodities_1991_2023 DESC
-- LIMIT 20;

-- -- Check modern prices coverage
-- SELECT 
--     ac.area_code,
--     ac.area as country_name,
--     'Modern (1991-2023)' as dataset,
--     COUNT(DISTINCT p.item_code_id) as commodities,
--     COUNT(DISTINCT p.year) as years_with_data,
--     MIN(p.year) || '-' || MAX(p.year) as year_range
-- FROM prices p
-- JOIN area_codes ac ON p.area_code_id = ac.id
-- JOIN elements e ON p.element_code_id = e.id
-- WHERE e.element_code = '5530'
--   AND p.months_code = '7021'
--   AND ac.area_code IN ('203', '138', '223', '231')
-- GROUP BY ac.area_code, ac.area
-- UNION ALL
-- -- Check archive prices coverage
-- SELECT 
--     ac.area_code,
--     ac.area as country_name,
--     'Archive (1966-1990)' as dataset,
--     COUNT(DISTINCT pa.item_code_id) as commodities,
--     COUNT(DISTINCT pa.year) as years_with_data,
--     MIN(pa.year) || '-' || MAX(pa.year) as year_range
-- FROM prices_archive pa
-- JOIN area_codes ac ON pa.area_code_id = ac.id
-- JOIN elements e ON pa.element_code_id = e.id
-- WHERE e.element_code = '5530'
--   AND ac.area_code IN ('203', '138', '223', '231')
-- GROUP BY ac.area_code, ac.area
-- ORDER BY area_code, dataset DESC;



WITH commodity_coverage AS (
    -- Modern period coverage
    SELECT 
        ac.area,
        ic.item_code,
        ic.item,
        COUNT(DISTINCT p.year) as modern_years,
        0 as archive_years
    FROM prices p
    JOIN item_codes ic ON p.item_code_id = ic.id
    JOIN elements e ON p.element_code_id = e.id
    JOIN area_codes ac ON p.area_code_id = ac.id
    WHERE e.element_code = '5530'
      AND p.months_code = '7021'
      AND ac.area_code = '231'  -- Change this for different countries
    GROUP BY ac.area, ic.item_code, ic.item
    
    UNION ALL
    
    -- Archive period coverage
    SELECT 
        ac.area,
        ic.item_code,
        ic.item,
        0 as modern_years,
        COUNT(DISTINCT pa.year) as archive_years
    FROM prices_archive pa
    JOIN item_codes ic ON pa.item_code_id = ic.id
    JOIN elements e ON pa.element_code_id = e.id
    JOIN area_codes ac ON pa.area_code_id = ac.id
    WHERE e.element_code = '5530'
      AND ac.area_code = '231'  -- Change this for different countries
    GROUP BY ac.area, ic.item_code, ic.item
)
SELECT 
    area,
    item_code,
    item,
    SUM(modern_years) as modern_years,
    SUM(archive_years) as archive_years,
    SUM(modern_years) + SUM(archive_years) as total_years,
    CASE 
        WHEN SUM(modern_years) > 0 AND SUM(archive_years) > 0 THEN 'Both periods'
        WHEN SUM(modern_years) > 0 THEN 'Modern only'
        ELSE 'Archive only'
    END as data_coverage
FROM commodity_coverage
GROUP BY area, item_code, item
ORDER BY total_years DESC, item;