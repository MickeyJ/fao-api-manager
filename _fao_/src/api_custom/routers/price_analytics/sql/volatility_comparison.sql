-- Compare price volatility between two time periods
-- Parameters: :p1_start, :p1_end, :p2_start, :p2_end, :min_year, :max_year, 
--            :element, :area_filter, :item_filter, :min_obs, :limit

WITH price_volatility AS (
    SELECT 
        i.item,
        i.item_code,
        a.area,
        a.area_code,
        CASE 
            WHEN p.year BETWEEN :p1_start AND :p1_end THEN 'period1'
            WHEN p.year BETWEEN :p2_start AND :p2_end THEN 'period2'
        END as period,
        STDDEV(p.value) as std_dev,
        AVG(p.value) as avg_price,
        MIN(p.value) as min_price,
        MAX(p.value) as max_price,
        ROUND((STDDEV(p.value) / NULLIF(AVG(p.value), 0) * 100)::numeric, 2) as cv_percent,
        COUNT(*) as observations
    FROM prices p
    JOIN item_codes i ON p.item_code_id = i.id
    JOIN area_codes a ON p.area_code_id = a.id
    JOIN elements e ON p.element_code_id = e.id
    JOIN flags f ON p.flag_id = f.id
    WHERE p.year BETWEEN :min_year AND :max_year
        AND p.value IS NOT NULL
        AND p.value > 0
        AND e.element = :element
        AND f.flag = 'A'
        AND (:area_filter IS NULL OR a.area_code = :area_filter)
        AND (:item_filter IS NULL OR i.item_code = :item_filter)
    GROUP BY i.item, i.item_code, a.area, a.area_code, period
    HAVING COUNT(*) >= :min_obs
)
SELECT 
    item,
    item_code,
    area,
    area_code,
    -- Period 1 stats
    MAX(CASE WHEN period = 'period1' THEN observations END) as period1_observations,
    MAX(CASE WHEN period = 'period1' THEN ROUND(avg_price::numeric, 2) END) as period1_avg_price,
    MAX(CASE WHEN period = 'period1' THEN cv_percent END) as period1_volatility_cv,
    
    -- Period 2 stats  
    MAX(CASE WHEN period = 'period2' THEN observations END) as period2_observations,
    MAX(CASE WHEN period = 'period2' THEN ROUND(avg_price::numeric, 2) END) as period2_avg_price,
    MAX(CASE WHEN period = 'period2' THEN cv_percent END) as period2_volatility_cv,
    
    -- Changes
    ROUND((MAX(CASE WHEN period = 'period2' THEN avg_price END) - 
           MAX(CASE WHEN period = 'period1' THEN avg_price END))::numeric, 2) as price_change,
    ROUND(((MAX(CASE WHEN period = 'period2' THEN avg_price END) - 
            MAX(CASE WHEN period = 'period1' THEN avg_price END)) / 
           NULLIF(MAX(CASE WHEN period = 'period1' THEN avg_price END), 0) * 100)::numeric, 2) as price_change_pct,
    ROUND((MAX(CASE WHEN period = 'period2' THEN cv_percent END) - 
           MAX(CASE WHEN period = 'period1' THEN cv_percent END))::numeric, 2) as volatility_change_pts
FROM price_volatility
GROUP BY item, item_code, area, area_code
HAVING MAX(CASE WHEN period = 'period1' THEN cv_percent END) IS NOT NULL
    AND MAX(CASE WHEN period = 'period2' THEN cv_percent END) IS NOT NULL
ORDER BY volatility_change_pts DESC NULLS LAST
LIMIT :limit