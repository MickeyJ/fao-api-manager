-- Sum up all your materialized views
SELECT 
    'All Materialized Views' as category,
    pg_size_pretty(SUM(pg_total_relation_size(c.oid))) as total_size
FROM pg_matviews mv
JOIN pg_class c ON c.relname = mv.matviewname
WHERE mv.schemaname = 'public'

UNION ALL

-- WAL size
SELECT 
    'WAL/Transaction Logs' as category,
    pg_size_pretty(sum(size)) as total_size
FROM pg_ls_waldir()

UNION ALL

-- Database total
SELECT 
    'Entire Database' as category,
    pg_size_pretty(pg_database_size(current_database())) as total_size;


-- Top 10 space users (tables + matviews + indexes)
SELECT 
    nspname as schema,
    relname as object_name,
    CASE relkind
        WHEN 'r' THEN 'table'
        WHEN 'i' THEN 'index'
        WHEN 'm' THEN 'matview'
        WHEN 't' THEN 'toast'
    END as type,
    pg_size_pretty(pg_total_relation_size(C.oid)) as total_size,
    CASE 
        WHEN relkind = 'r' THEN pg_size_pretty(pg_total_relation_size(C.oid) - pg_relation_size(C.oid))
        ELSE '-'
    END as non_table_size
FROM pg_class C
JOIN pg_namespace N ON N.oid = C.relnamespace
WHERE nspname NOT IN ('pg_catalog', 'information_schema')
AND pg_total_relation_size(C.oid) > 10000000  -- > 10MB
ORDER BY pg_total_relation_size(C.oid) DESC
LIMIT 20;

-- transaction log size
SELECT 
    pg_size_pretty(sum(size)) as total_size
FROM pg_ls_waldir();


-- See current temp file usage
SELECT 
    name,
    size,
    pg_size_pretty(size) as size_pretty,
    modification
FROM pg_ls_tmpdir()
ORDER BY size DESC;


-- See if any tables grew recently (if you have pg_stat_user_tables)
SELECT 
    schemaname,
    relname as table_name,
    n_tup_ins as rows_inserted,
    n_tup_upd as rows_updated,
    n_tup_del as rows_deleted,
    last_vacuum,
    last_autovacuum
FROM pg_stat_user_tables
WHERE n_tup_ins > 100000  -- tables with lots of inserts
ORDER BY n_tup_ins DESC;


-- Check index sizes
SELECT 
    schemaname,
    relname as table_name,
    indexrelname,
    pg_size_pretty(pg_relation_size(indexrelid)) as size
FROM pg_stat_user_indexes
ORDER BY pg_relation_size(indexrelid) DESC
LIMIT 20;