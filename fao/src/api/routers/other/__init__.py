from fastapi import APIRouter
from ... import current_version_prefix

from .area_codes import router as area_codes
from .item_codes import router as item_codes
from .elements import router as elements
from .sexs import router as sexs
from .flags import router as flags
from .currencies import router as currencies
from .sources import router as sources
from .surveys import router as surveys
from .releases import router as releases
from .purposes import router as purposes
from .donors import router as donors
from .geographic_levels import router as geographic_levels
from .aquastat import router as aquastat
from .climate_change_emissions_indicators import router as climate_change_emissions_indicators
from .consumer_price_indices import router as consumer_price_indices
from .cost_affordability_healthy_diet_co_ahd import router as cost_affordability_healthy_diet_co_ahd
from .deflators import router as deflators
from .development_assistance_to_agriculture import router as development_assistance_to_agriculture
from .exchange_rate import router as exchange_rate
from .fertilizers_detailed_trade_matrix import router as fertilizers_detailed_trade_matrix
from .household_consumption_and_expenditure_surveys_food_and_diet import router as household_consumption_and_expenditure_surveys_food_and_diet
from .individual_quantitative_dietary_data_food_and_diet import router as individual_quantitative_dietary_data_food_and_diet
from .macro_statistics_key_indicators import router as macro_statistics_key_indicators
from .minimum_dietary_diversity_for_women_mdd_w_food_and_diet import router as minimum_dietary_diversity_for_women_mdd_w_food_and_diet
from .sdg_bulk_downloads import router as sdg_bulk_downloads
from .sua_crops_livestock import router as sua_crops_livestock
from .supply_utilization_accounts_food_and_diet import router as supply_utilization_accounts_food_and_diet

other_api = APIRouter(
  prefix=f"/{current_version_prefix}", 
  tags=["other"],
)

other_api.include_router(
  area_codes, 
  prefix=f"/other", 
  tags=["other", "area_codes"],
)
other_api.include_router(
  item_codes, 
  prefix=f"/other", 
  tags=["other", "item_codes"],
)
other_api.include_router(
  elements, 
  prefix=f"/other", 
  tags=["other", "elements"],
)
other_api.include_router(
  sexs, 
  prefix=f"/other", 
  tags=["other", "sexs"],
)
other_api.include_router(
  flags, 
  prefix=f"/other", 
  tags=["other", "flags"],
)
other_api.include_router(
  currencies, 
  prefix=f"/other", 
  tags=["other", "currencies"],
)
other_api.include_router(
  sources, 
  prefix=f"/other", 
  tags=["other", "sources"],
)
other_api.include_router(
  surveys, 
  prefix=f"/other", 
  tags=["other", "surveys"],
)
other_api.include_router(
  releases, 
  prefix=f"/other", 
  tags=["other", "releases"],
)
other_api.include_router(
  purposes, 
  prefix=f"/other", 
  tags=["other", "purposes"],
)
other_api.include_router(
  donors, 
  prefix=f"/other", 
  tags=["other", "donors"],
)
other_api.include_router(
  geographic_levels, 
  prefix=f"/other", 
  tags=["other", "geographic_levels"],
)
other_api.include_router(
  aquastat, 
  prefix=f"/other", 
  tags=["other", "aquastat"],
)
other_api.include_router(
  climate_change_emissions_indicators, 
  prefix=f"/other", 
  tags=["other", "climate_change_emissions_indicators"],
)
other_api.include_router(
  consumer_price_indices, 
  prefix=f"/other", 
  tags=["other", "consumer_price_indices"],
)
other_api.include_router(
  cost_affordability_healthy_diet_co_ahd, 
  prefix=f"/other", 
  tags=["other", "cost_affordability_healthy_diet_co_ahd"],
)
other_api.include_router(
  deflators, 
  prefix=f"/other", 
  tags=["other", "deflators"],
)
other_api.include_router(
  development_assistance_to_agriculture, 
  prefix=f"/other", 
  tags=["other", "development_assistance_to_agriculture"],
)
other_api.include_router(
  exchange_rate, 
  prefix=f"/other", 
  tags=["other", "exchange_rate"],
)
other_api.include_router(
  fertilizers_detailed_trade_matrix, 
  prefix=f"/other", 
  tags=["other", "fertilizers_detailed_trade_matrix"],
)
other_api.include_router(
  household_consumption_and_expenditure_surveys_food_and_diet, 
  prefix=f"/other", 
  tags=["other", "household_consumption_and_expenditure_surveys_food_and_diet"],
)
other_api.include_router(
  individual_quantitative_dietary_data_food_and_diet, 
  prefix=f"/other", 
  tags=["other", "individual_quantitative_dietary_data_food_and_diet"],
)
other_api.include_router(
  macro_statistics_key_indicators, 
  prefix=f"/other", 
  tags=["other", "macro_statistics_key_indicators"],
)
other_api.include_router(
  minimum_dietary_diversity_for_women_mdd_w_food_and_diet, 
  prefix=f"/other", 
  tags=["other", "minimum_dietary_diversity_for_women_mdd_w_food_and_diet"],
)
other_api.include_router(
  sdg_bulk_downloads, 
  prefix=f"/other", 
  tags=["other", "sdg_bulk_downloads"],
)
other_api.include_router(
  sua_crops_livestock, 
  prefix=f"/other", 
  tags=["other", "sua_crops_livestock"],
)
other_api.include_router(
  supply_utilization_accounts_food_and_diet, 
  prefix=f"/other", 
  tags=["other", "supply_utilization_accounts_food_and_diet"],
)

other_group_map = {
    "description": "other",
    "routes": [
        {
            "name": "area_codes",
            "description": "",
            "path": f"/{ current_version_prefix }/other/area_codes",
        },
        {
            "name": "item_codes",
            "description": "",
            "path": f"/{ current_version_prefix }/other/item_codes",
        },
        {
            "name": "elements",
            "description": "",
            "path": f"/{ current_version_prefix }/other/elements",
        },
        {
            "name": "sexs",
            "description": "",
            "path": f"/{ current_version_prefix }/other/sexs",
        },
        {
            "name": "flags",
            "description": "",
            "path": f"/{ current_version_prefix }/other/flags",
        },
        {
            "name": "currencies",
            "description": "",
            "path": f"/{ current_version_prefix }/other/currencies",
        },
        {
            "name": "sources",
            "description": "",
            "path": f"/{ current_version_prefix }/other/sources",
        },
        {
            "name": "surveys",
            "description": "",
            "path": f"/{ current_version_prefix }/other/surveys",
        },
        {
            "name": "releases",
            "description": "",
            "path": f"/{ current_version_prefix }/other/releases",
        },
        {
            "name": "purposes",
            "description": "",
            "path": f"/{ current_version_prefix }/other/purposes",
        },
        {
            "name": "donors",
            "description": "",
            "path": f"/{ current_version_prefix }/other/donors",
        },
        {
            "name": "geographic_levels",
            "description": "",
            "path": f"/{ current_version_prefix }/other/geographic_levels",
        },
        {
            "name": "aquastat",
            "description": "",
            "path": f"/{ current_version_prefix }/other/aquastat",
        },
        {
            "name": "climate_change_emissions_indicators",
            "description": "",
            "path": f"/{ current_version_prefix }/other/climate_change_emissions_indicators",
        },
        {
            "name": "consumer_price_indices",
            "description": "",
            "path": f"/{ current_version_prefix }/other/consumer_price_indices",
        },
        {
            "name": "cost_affordability_healthy_diet_co_ahd",
            "description": "",
            "path": f"/{ current_version_prefix }/other/cost_affordability_healthy_diet_co_ahd",
        },
        {
            "name": "deflators",
            "description": "",
            "path": f"/{ current_version_prefix }/other/deflators",
        },
        {
            "name": "development_assistance_to_agriculture",
            "description": "",
            "path": f"/{ current_version_prefix }/other/development_assistance_to_agriculture",
        },
        {
            "name": "exchange_rate",
            "description": "",
            "path": f"/{ current_version_prefix }/other/exchange_rate",
        },
        {
            "name": "fertilizers_detailed_trade_matrix",
            "description": "",
            "path": f"/{ current_version_prefix }/other/fertilizers_detailed_trade_matrix",
        },
        {
            "name": "household_consumption_and_expenditure_surveys_food_and_diet",
            "description": "",
            "path": f"/{ current_version_prefix }/other/household_consumption_and_expenditure_surveys_food_and_diet",
        },
        {
            "name": "individual_quantitative_dietary_data_food_and_diet",
            "description": "",
            "path": f"/{ current_version_prefix }/other/individual_quantitative_dietary_data_food_and_diet",
        },
        {
            "name": "macro_statistics_key_indicators",
            "description": "",
            "path": f"/{ current_version_prefix }/other/macro_statistics_key_indicators",
        },
        {
            "name": "minimum_dietary_diversity_for_women_mdd_w_food_and_diet",
            "description": "",
            "path": f"/{ current_version_prefix }/other/minimum_dietary_diversity_for_women_mdd_w_food_and_diet",
        },
        {
            "name": "sdg_bulk_downloads",
            "description": "",
            "path": f"/{ current_version_prefix }/other/sdg_bulk_downloads",
        },
        {
            "name": "sua_crops_livestock",
            "description": "",
            "path": f"/{ current_version_prefix }/other/sua_crops_livestock",
        },
        {
            "name": "supply_utilization_accounts_food_and_diet",
            "description": "",
            "path": f"/{ current_version_prefix }/other/supply_utilization_accounts_food_and_diet",
        },
    ],
}

# Export the sub-API
__all__ = ["other_api", "other_group_map"]