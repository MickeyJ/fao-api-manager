"""Database views for the agricultural data analysis project."""

from pathlib import Path
from _fao_.src.core.utils import load_sql

refresh_views_sql = load_sql("_refresh_all.sql", Path(__file__).parent)
create_view_indexes_sql = load_sql("_create_mv_indexes.sql", Path(__file__).parent)

# List all views for registration
ALL_VIEWS = {
    "price_ratios_usd": load_sql("price_ratios_usd.sql", Path(__file__).parent),
    "price_ratios_lcu": load_sql("price_ratios_lcu.sql", Path(__file__).parent),
    "price_details_usd": load_sql("price_details_usd.sql", Path(__file__).parent),
    "price_details_lcu": load_sql("price_details_lcu.sql", Path(__file__).parent),
    "item_stats_lcu": load_sql("item_stats_lcu.sql", Path(__file__).parent),
    "item_stats_usd": load_sql("item_stats_usd.sql", Path(__file__).parent),
}

ALL_DROP_VIEWS = {
    "price_ratios_usd": "DROP MATERIALIZED VIEW IF EXISTS price_ratios_usd CASCADE",
    "price_ratios_lcu": "DROP MATERIALIZED VIEW IF EXISTS price_ratios_lcu CASCADE",
    "price_details_usd": "DROP MATERIALIZED VIEW IF EXISTS price_details_usd CASCADE",
    "price_details_lcu": "DROP MATERIALIZED VIEW IF EXISTS price_details_lcu CASCADE",
    "item_stats_lcu": "DROP MATERIALIZED VIEW IF EXISTS item_stats_lcu CASCADE",
    "item_stats_usd": "DROP MATERIALIZED VIEW IF EXISTS item_stats_usd CASCADE",
}
