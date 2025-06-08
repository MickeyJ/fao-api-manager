WITH price_volatility AS (
  SELECT 
    i.item,
    i.item_code,
    a.area,
    CASE 
      WHEN p.year BETWEEN 2017 AND 2019 THEN 'pre_covid'
      WHEN p.year BETWEEN 2020 AND 2023 THEN 'covid_ukraine_era'
    END as period,
    STDDEV(p.value) as price_volatility,
    AVG(p.value) as avg_price,
    COUNT(*) as observations
  FROM prices p
  JOIN item_codes i ON p.item_code_id = i.id
  JOIN area_codes a ON p.area_code_id = a.id
  WHERE p.year IN (2010,2011,2012,2013,2014,2019,2020,2021,2022,2023)
    AND p.value IS NOT NULL
  GROUP BY i.item, i.item_code, a.area, period
  HAVING COUNT(*) > 10  -- Need enough data points
)
SELECT 
  item,
  area,
  MAX(CASE WHEN period = 'pre_covid' THEN price_volatility END) as volatility_2010_2014,
  MAX(CASE WHEN period = 'covid_ukraine_era' THEN price_volatility END) as volatility_2019_2023,
  ROUND(
    ((MAX(CASE WHEN period = 'covid_ukraine_era' THEN price_volatility END) - 
      MAX(CASE WHEN period = 'pre_covid' THEN price_volatility END)) / 
     NULLIF(MAX(CASE WHEN period = 'pre_covid' THEN price_volatility END), 0) * 100)::numeric, 
    2
  ) as volatility_increase_pct
FROM price_volatility
GROUP BY item, area
HAVING MAX(CASE WHEN period = 'pre_covid' THEN price_volatility END) IS NOT NULL
   AND MAX(CASE WHEN period = 'covid_ukraine_era' THEN price_volatility END) IS NOT NULL
ORDER BY volatility_increase_pct DESC NULLS LAST
;