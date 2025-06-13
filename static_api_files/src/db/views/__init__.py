"""Database views for the agricultural data analysis project."""

from pathlib import Path
from alembic_utils.pg_materialized_view import PGMaterializedView
from fao.src.core.utils import load_sql


price_ratios_usd_sql = load_sql("price_ratios_usd.sql", Path(__file__).parent)
PRICE_RATIOS_USD_VIEW = PGMaterializedView(
    schema="public", signature="price_ratios_usd", definition=price_ratios_usd_sql, with_data=True
)

price_ratios_lcu_sql = load_sql("price_ratios_lcu.sql", Path(__file__).parent)
PRICE_RATIOS_LCU_VIEW = PGMaterializedView(
    schema="public", signature="price_ratios_lcu", definition=price_ratios_lcu_sql, with_data=True
)

price_details_usd_sql = load_sql("price_details_usd.sql", Path(__file__).parent)
PRICE_DETAILS_USD_VIEW = PGMaterializedView(
    schema="public", signature="price_details_usd", definition=price_details_usd_sql, with_data=True
)

price_details_lcu_sql = load_sql("price_details_lcu.sql", Path(__file__).parent)
PRICE_DETAILS_LCU_VIEW = PGMaterializedView(
    schema="public", signature="price_details_lcu", definition=price_details_lcu_sql, with_data=True
)

item_stats_lcu_sql = load_sql("item_stats_lcu.sql", Path(__file__).parent)
ITEM_STATS_LCU_VIEW = PGMaterializedView(
    schema="public", signature="item_stats_lcu", definition=item_stats_lcu_sql, with_data=True
)

item_stats_usd_sql = load_sql("item_stats_usd.sql", Path(__file__).parent)
ITEM_STATS_USD_VIEW = PGMaterializedView(
    schema="public", signature="item_stats_usd", definition=item_stats_usd_sql, with_data=True
)

# List all views for registration
ALL_VIEWS = [
    PRICE_RATIOS_USD_VIEW,
    PRICE_RATIOS_LCU_VIEW,
    PRICE_DETAILS_USD_VIEW,
    PRICE_DETAILS_LCU_VIEW,
    ITEM_STATS_LCU_VIEW,
    ITEM_STATS_USD_VIEW,
]
