
-- list all materialized views in the public schema
SELECT 
    schemaname,
    matviewname,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||matviewname)) as size,
    hasindexes,
    ispopulated
FROM pg_matviews
WHERE schemaname = 'public'  -- or your schema name
ORDER BY matviewname;


-- Check if these are tables or views
SELECT 
    c.relname,
    CASE c.relkind 
        WHEN 'r' THEN 'table'
        WHEN 'm' THEN 'materialized view'
    END as type,
    pg_size_pretty(pg_total_relation_size(c.oid)) as size
FROM pg_class c
WHERE c.relname IN (
  'price_ratios_usd', 
  'price_ratios_lcu', 
  'price_details_usd', 
  'price_details_lcu',
  'item_stats_usd',
  'item_stats_lcu'
);










-- -- Force a checkpoint to process WAL files
-- CHECKPOINT;

-- -- Check current WAL settings
-- SHOW wal_keep_size;
-- SHOW max_wal_size;
-- SHOW checkpoint_timeout;


-- -- See current WAL position and activity
-- SELECT 
--     pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0'::pg_lsn)) as total_wal_generated,
--     pg_is_in_recovery() as is_replica,
--     pg_last_wal_receive_lsn() as last_received,
--     pg_last_wal_replay_lsn() as last_replayed;

-- -- Check replication slots (these can prevent WAL cleanup)
-- SELECT slot_name, active, restart_lsn 
-- FROM pg_replication_slots;

-- SELECT 'price_ratios_usd' as view_name, COUNT(*) as row_count FROM price_ratios_usd
-- UNION ALL
-- SELECT 'price_ratios_lcu', COUNT(*) FROM price_ratios_lcu
-- UNION ALL
-- SELECT 'price_details_usd', COUNT(*) FROM price_details_usd
-- UNION ALL
-- SELECT 'price_details_lcu', COUNT(*) FROM price_details_lcu
-- UNION ALL
-- SELECT 'item_stats_usd', COUNT(*) FROM item_stats_usd
-- UNION ALL
-- SELECT 'item_stats_lcu', COUNT(*) FROM item_stats_lcu;


-- SET statement_timeout = '30min';

-- REFRESH MATERIALIZED VIEW item_stats_lcu;
-- REFRESH MATERIALIZED VIEW item_stats_usd;
-- REFRESH MATERIALIZED VIEW price_details_lcu;
-- REFRESH MATERIALIZED VIEW price_details_usd;
-- REFRESH MATERIALIZED VIEW price_ratios_lcu;
-- REFRESH MATERIALIZED VIEW price_ratios_usd;


-- DROP MATERIALIZED VIEW IF EXISTS price_ratios_usd CASCADE;
-- CREATE MATERIALIZED VIEW price_ratios_usd AS
-- WITH annual_prices AS (
--     SELECT
--         ac.id as area_id,
--         ac.area_code,
--         ac.area as country_name,
--         ic.item_code,
--         ic.item as item_name,
--         p.year,
--         AVG(p.value) as price
--     FROM prices p
--     JOIN area_codes ac ON p.area_code_id = ac.id
--     JOIN item_codes ic ON p.item_code_id = ic.id
--     JOIN elements e ON p.element_code_id = e.id
--     JOIN flags f ON p.flag_id = f.id
--     WHERE
--         e.element_code = '5532'  -- USD prices only
--         AND f.flag != 'I'
--         AND p.months_code = '7021'  -- Annual average
--     GROUP BY ac.id, ac.area_code, ac.area, ic.item_code, ic.item, p.year
-- )
-- SELECT
--     p1.area_id as country1_id,
--     p1.area_code as country1_code,
--     p1.country_name as country1,
--     p2.area_id as country2_id,
--     p2.area_code as country2_code,
--     p2.country_name as country2,
--     p1.item_code,
--     p1.item_name,
--     p1.year,
--     p1.price as price1,
--     p2.price as price2,
--     ROUND((p1.price / NULLIF(p2.price, 0))::numeric, 3) as price_ratio
-- FROM annual_prices p1
-- JOIN annual_prices p2
--     ON p1.year = p2.year
--     AND p1.item_code = p2.item_code
--     AND p1.area_code < p2.area_code
-- WHERE p2.price > 0
-- WITH NO DATA;