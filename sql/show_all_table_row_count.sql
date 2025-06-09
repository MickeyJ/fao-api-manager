DO $$
DECLARE
    tbl TEXT;
    row_count BIGINT;
BEGIN
    FOR tbl IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
    LOOP
        EXECUTE format('SELECT count(*) FROM public.%I', tbl) INTO row_count;
        RAISE NOTICE 'Table: %, Row Count: %', tbl, row_count;
    END LOOP;
END $$;