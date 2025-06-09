CREATE TABLE IF NOT EXISTS pipeline_progress (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) UNIQUE NOT NULL,
    last_row_processed INTEGER NOT NULL,
    total_rows INTEGER,
    last_chunk_time TIMESTAMP DEFAULT NOW(),
    status VARCHAR(20) DEFAULT 'in_progress',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

INSERT INTO pipeline_progress (
  table_name, 
  last_row_processed, 
  total_rows, 
  status
)
VALUES 
(
  'food_and_diet_individual_quantitative_dietary_data', 
  874965, 
  1164769, 
  'in_progress'
);