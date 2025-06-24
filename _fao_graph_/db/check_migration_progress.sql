SELECT
    batch_number,
    batch_size,
    select_duration_ms,
    insert_duration_ms,
    total_duration_ms,
    cumulative_records,
    memory_usage_mb,
    insert_duration_ms::float / batch_size AS ms_per_record
FROM migration_progress
WHERE
    table_name =: table_name
    AND error_message IS NULL
ORDER BY batch_number;

SELECT
    batch_number,
    AVG(insert_duration_ms)
        OVER (ORDER BY batch_number ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)
    AS rolling_avg_insert_ms
FROM migration_progress
WHERE table_name =: table_name;
