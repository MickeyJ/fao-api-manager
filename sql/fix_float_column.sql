
-- which tables need value column fixed
-- SELECT 
--     table_name,
--     data_type,
--     CASE 
--         WHEN data_type LIKE '%char%' THEN 'Needs conversion'
--         WHEN data_type IN ('double precision', 'real', 'numeric') THEN 'Already numeric'
--         ELSE 'Other type'
--     END as status
-- FROM information_schema.columns
-- WHERE column_name = 'value'
--   AND table_schema = 'public'
-- ORDER BY status, table_name;

-- See ALL tables that still have value as character varying
SELECT 
    table_name,
    data_type
FROM information_schema.columns
WHERE column_name = 'value'
  AND table_schema = 'public'
  AND data_type LIKE '%char%'
ORDER BY table_name;


-- check what needs to be fixed
SELECT DISTINCT value, COUNT(*) as count
FROM cost_affordability_healthy_diet_co_ahd
WHERE value !~ '^[0-9]+\.?[0-9]*$'
  AND value IS NOT NULL
GROUP BY value
ORDER BY count DESC;


-- Step 1: Add new float column
ALTER TABLE cost_affordability_healthy_diet_co_ahd 
ADD COLUMN value_numeric FLOAT;

-- Step 2: Populate with cleaned data
UPDATE cost_affordability_healthy_diet_co_ahd
SET value_numeric = 
    CASE 
        WHEN value = 'nan' OR value IS NULL THEN NULL
        WHEN value = '<0.1' THEN 0.05
        WHEN value LIKE '<%' THEN CAST(REPLACE(value, '<', '') AS FLOAT) * 0.5
        WHEN value LIKE '>%' THEN CAST(REPLACE(value, '>', '') AS FLOAT) * 1.5
        ELSE CAST(value AS FLOAT)
    END;

-- Step 3: Drop old column and rename new one
ALTER TABLE cost_affordability_healthy_diet_co_ahd DROP COLUMN value;
ALTER TABLE cost_affordability_healthy_diet_co_ahd RENAME COLUMN value_numeric TO value;

-- a script to fix all of them at once
DO $$
DECLARE
    tbl RECORD;
BEGIN
    FOR tbl IN 
        SELECT table_name 
        FROM information_schema.columns 
        WHERE column_name = 'value' 
        AND table_schema = 'public'
        AND data_type LIKE '%char%'
    LOOP
        EXECUTE format('ALTER TABLE %I ADD COLUMN value_numeric FLOAT', tbl.table_name);
        
        EXECUTE format('UPDATE %I SET value_numeric = 
            CASE 
                WHEN value = ''nan'' OR value IS NULL THEN NULL
                WHEN value = ''<0.1'' THEN 0.05
                ELSE value::FLOAT
            END', tbl.table_name);
            
        EXECUTE format('ALTER TABLE %I DROP COLUMN value', tbl.table_name);
        EXECUTE format('ALTER TABLE %I RENAME COLUMN value_numeric TO value', tbl.table_name);
        
        RAISE NOTICE 'Fixed table: %', tbl.table_name;
    END LOOP;
END $$;