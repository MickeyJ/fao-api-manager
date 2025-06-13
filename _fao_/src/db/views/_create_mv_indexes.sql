-- Run all index creations (will take a few minutes)

-- Price ratios indexes
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_price_ratios_usd_lookup ON price_ratios_usd(item_code, country1_code, country2_code);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_price_ratios_usd_countries ON price_ratios_usd(country1_code, country2_code);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_price_ratios_lcu_lookup ON price_ratios_lcu(item_code, country1_code, country2_code);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_price_ratios_lcu_countries ON price_ratios_lcu(country1_code, country2_code);

-- Price details indexes
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_price_details_usd_lookup ON price_details_usd(area_code, item_code, year);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_price_details_usd_item ON price_details_usd(item_code, year);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_price_details_lcu_lookup ON price_details_lcu(area_code, item_code, year);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_price_details_lcu_item ON price_details_lcu(item_code, year);

-- Item stats indexes
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_item_stats_usd_item_code ON item_stats_usd(item_code);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_item_stats_usd_countries ON item_stats_usd(countries_with_data DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_item_stats_lcu_item_code ON item_stats_lcu(item_code);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_item_stats_lcu_countries ON item_stats_lcu(countries_with_data DESC);

