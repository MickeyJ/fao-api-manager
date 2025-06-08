from fastapi import APIRouter
from ... import current_version_prefix

from .environment_bioenergy import router as environment_bioenergy
from .environment_cropland_nutrient_budget import router as environment_cropland_nutrient_budget
from .environment_emissions_by_sector import router as environment_emissions_by_sector
from .environment_emissions_intensities import router as environment_emissions_intensities
from .environment_food_waste_disposal import router as environment_food_waste_disposal
from .environment_land_cover import router as environment_land_cover
from .environment_land_use import router as environment_land_use
from .environment_livestock_manure import router as environment_livestock_manure
from .environment_livestock_patterns import router as environment_livestock_patterns
from .environment_pesticides import router as environment_pesticides
from .environment_soil_nutrient_budget import router as environment_soil_nutrient_budget
from .environment_temperature_change import router as environment_temperature_change

environment_api = APIRouter(
  prefix=f"/{current_version_prefix}", 
  tags=["environment"],
)

environment_api.include_router(
  environment_bioenergy, 
  prefix=f"/environment", 
  tags=["environment", "environment_bioenergy"],
)
environment_api.include_router(
  environment_cropland_nutrient_budget, 
  prefix=f"/environment", 
  tags=["environment", "environment_cropland_nutrient_budget"],
)
environment_api.include_router(
  environment_emissions_by_sector, 
  prefix=f"/environment", 
  tags=["environment", "environment_emissions_by_sector"],
)
environment_api.include_router(
  environment_emissions_intensities, 
  prefix=f"/environment", 
  tags=["environment", "environment_emissions_intensities"],
)
environment_api.include_router(
  environment_food_waste_disposal, 
  prefix=f"/environment", 
  tags=["environment", "environment_food_waste_disposal"],
)
environment_api.include_router(
  environment_land_cover, 
  prefix=f"/environment", 
  tags=["environment", "environment_land_cover"],
)
environment_api.include_router(
  environment_land_use, 
  prefix=f"/environment", 
  tags=["environment", "environment_land_use"],
)
environment_api.include_router(
  environment_livestock_manure, 
  prefix=f"/environment", 
  tags=["environment", "environment_livestock_manure"],
)
environment_api.include_router(
  environment_livestock_patterns, 
  prefix=f"/environment", 
  tags=["environment", "environment_livestock_patterns"],
)
environment_api.include_router(
  environment_pesticides, 
  prefix=f"/environment", 
  tags=["environment", "environment_pesticides"],
)
environment_api.include_router(
  environment_soil_nutrient_budget, 
  prefix=f"/environment", 
  tags=["environment", "environment_soil_nutrient_budget"],
)
environment_api.include_router(
  environment_temperature_change, 
  prefix=f"/environment", 
  tags=["environment", "environment_temperature_change"],
)

environment_group_map = {
    "description": "environment",
    "routes": [
        {
            "name": "environment_bioenergy",
            "description": "",
            "path": f"/{ current_version_prefix }/environment/environment_bioenergy",
        },
        {
            "name": "environment_cropland_nutrient_budget",
            "description": "",
            "path": f"/{ current_version_prefix }/environment/environment_cropland_nutrient_budget",
        },
        {
            "name": "environment_emissions_by_sector",
            "description": "",
            "path": f"/{ current_version_prefix }/environment/environment_emissions_by_sector",
        },
        {
            "name": "environment_emissions_intensities",
            "description": "",
            "path": f"/{ current_version_prefix }/environment/environment_emissions_intensities",
        },
        {
            "name": "environment_food_waste_disposal",
            "description": "",
            "path": f"/{ current_version_prefix }/environment/environment_food_waste_disposal",
        },
        {
            "name": "environment_land_cover",
            "description": "",
            "path": f"/{ current_version_prefix }/environment/environment_land_cover",
        },
        {
            "name": "environment_land_use",
            "description": "",
            "path": f"/{ current_version_prefix }/environment/environment_land_use",
        },
        {
            "name": "environment_livestock_manure",
            "description": "",
            "path": f"/{ current_version_prefix }/environment/environment_livestock_manure",
        },
        {
            "name": "environment_livestock_patterns",
            "description": "",
            "path": f"/{ current_version_prefix }/environment/environment_livestock_patterns",
        },
        {
            "name": "environment_pesticides",
            "description": "",
            "path": f"/{ current_version_prefix }/environment/environment_pesticides",
        },
        {
            "name": "environment_soil_nutrient_budget",
            "description": "",
            "path": f"/{ current_version_prefix }/environment/environment_soil_nutrient_budget",
        },
        {
            "name": "environment_temperature_change",
            "description": "",
            "path": f"/{ current_version_prefix }/environment/environment_temperature_change",
        },
    ],
}

# Export the sub-API
__all__ = ["environment_api", "environment_group_map"]