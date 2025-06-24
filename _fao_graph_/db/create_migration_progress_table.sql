-- fao_graph/db/create_migration_progress_table.sql
CREATE TABLE IF NOT EXISTS migration_progress (
    id SERIAL PRIMARY KEY,
    migration_type VARCHAR(50) NOT NULL, -- 'node' or 'relationship'
    table_name VARCHAR(100) NOT NULL,
    relationship_type VARCHAR(50), -- 'TRADES', 'PRODUCES', etc
    batch_number INTEGER NOT NULL DEFAULT 0,
    batch_size INTEGER NOT NULL,
    records_processed BIGINT NOT NULL DEFAULT 0,
    total_records BIGINT, -- Total records to process
    -- 'pending', 'in_progress', 'completed', 'failed'
    status TEXT NOT NULL DEFAULT 'pending',
    error_message TEXT,
    -- Last successfully processed offset for resume
    last_offset BIGINT DEFAULT 0,
    select_duration_ms INTEGER, -- Average SELECT duration
    insert_duration_ms INTEGER, -- Average INSERT duration
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    completed BOOLEAN NOT NULL DEFAULT FALSE,
    indexes_created BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (table_name, relationship_type)
);

-- Create update trigger for updated_at
CREATE OR REPLACE FUNCTION UPDATE_UPDATED_AT_COLUMN()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_migration_progress_updated_at
BEFORE UPDATE ON migration_progress
FOR EACH ROW
EXECUTE FUNCTION UPDATE_UPDATED_AT_COLUMN();
