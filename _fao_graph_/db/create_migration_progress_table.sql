CREATE TABLE IF NOT EXISTS migration_progress (
    id SERIAL PRIMARY KEY,
    migration_type VARCHAR(50) NOT NULL, -- 'node' or 'relationship'
    table_name VARCHAR(100) NOT NULL,
    relationship_type VARCHAR(50), -- 'TRADES', 'PRODUCES', etc
    batch_number INTEGER NOT NULL,
    batch_size INTEGER NOT NULL,
    records_processed INTEGER NOT NULL,
    select_duration_ms INTEGER, -- Time to SELECT batch
    insert_duration_ms INTEGER, -- Time to INSERT into graph
    total_duration_ms INTEGER,
    started_at TIMESTAMP NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMP,
    error_message TEXT,
    cumulative_records BIGINT, -- Total processed so far
    memory_usage_mb INTEGER, -- Optional: track memory
    UNIQUE(table_name, relationship_type, batch_number)
);