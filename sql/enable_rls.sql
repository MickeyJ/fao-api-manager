DO $$ 
DECLARE
    tbl RECORD;
BEGIN
    FOR tbl IN 
        SELECT c.relname as tablename
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' 
        AND c.relkind = 'r'  -- 'r' for regular tables
    LOOP
        EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', tbl.tablename);
        EXECUTE format('CREATE POLICY "Public read access" ON %I FOR SELECT USING (true)', tbl.tablename);
        RAISE NOTICE 'Enabled RLS on %', tbl.tablename;
    END LOOP;
END $$;