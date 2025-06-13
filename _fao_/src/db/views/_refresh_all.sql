SET statement_timeout = '30min';

REFRESH MATERIALIZED VIEW item_stats_lcu;
REFRESH MATERIALIZED VIEW item_stats_usd;
REFRESH MATERIALIZED VIEW price_details_lcu;
REFRESH MATERIALIZED VIEW price_details_usd;
REFRESH MATERIALIZED VIEW price_ratios_lcu;
REFRESH MATERIALIZED VIEW price_ratios_usd;