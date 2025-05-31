-- Check column structure for all your tables
SELECT 
    table_name,
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns 
WHERE table_name IN ('items', 'areas', 'item_prices', 'food_price_index', 'food_price_inflation')
  AND table_schema = 'public'
ORDER BY table_name, ordinal_position;