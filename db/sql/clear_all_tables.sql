DO $$ 
DECLARE
    r RECORD;
    tables TEXT;
BEGIN
    -- Build comma-separated list of all table names
    SELECT string_agg(quote_ident(tablename), ', ') INTO tables
    FROM pg_tables 
    WHERE schemaname = 'public';
    
    -- Truncate all tables at once with CASCADE
    IF tables IS NOT NULL THEN
        EXECUTE 'TRUNCATE TABLE ' || tables || ' RESTART IDENTITY CASCADE';
    END IF;
END $$;