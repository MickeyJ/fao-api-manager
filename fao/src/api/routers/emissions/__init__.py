from fastapi import APIRouter
from ... import current_version_prefix

from .emissions_agriculture_energy import router as emissions_agriculture_energy
from .emissions_crops import router as emissions_crops
from .emissions_drained_organic_soils import router as emissions_drained_organic_soils
from .emissions_land_use_fires import router as emissions_land_use_fires
from .emissions_land_use_forests import router as emissions_land_use_forests
from .emissions_livestock import router as emissions_livestock
from .emissions_pre_post_production import router as emissions_pre_post_production
from .emissions_totals import router as emissions_totals

emissions_api = APIRouter(
  prefix=f"/{current_version_prefix}", 
  tags=["emissions"],
)

emissions_api.include_router(
  emissions_agriculture_energy, 
  prefix=f"/emissions", 
  tags=["emissions", "emissions_agriculture_energy"],
)
emissions_api.include_router(
  emissions_crops, 
  prefix=f"/emissions", 
  tags=["emissions", "emissions_crops"],
)
emissions_api.include_router(
  emissions_drained_organic_soils, 
  prefix=f"/emissions", 
  tags=["emissions", "emissions_drained_organic_soils"],
)
emissions_api.include_router(
  emissions_land_use_fires, 
  prefix=f"/emissions", 
  tags=["emissions", "emissions_land_use_fires"],
)
emissions_api.include_router(
  emissions_land_use_forests, 
  prefix=f"/emissions", 
  tags=["emissions", "emissions_land_use_forests"],
)
emissions_api.include_router(
  emissions_livestock, 
  prefix=f"/emissions", 
  tags=["emissions", "emissions_livestock"],
)
emissions_api.include_router(
  emissions_pre_post_production, 
  prefix=f"/emissions", 
  tags=["emissions", "emissions_pre_post_production"],
)
emissions_api.include_router(
  emissions_totals, 
  prefix=f"/emissions", 
  tags=["emissions", "emissions_totals"],
)

emissions_group_map = {
    "description": "emissions",
    "routes": [
        {
            "name": "emissions_agriculture_energy",
            "description": "",
            "path": f"/{ current_version_prefix }/emissions/emissions_agriculture_energy",
        },
        {
            "name": "emissions_crops",
            "description": "",
            "path": f"/{ current_version_prefix }/emissions/emissions_crops",
        },
        {
            "name": "emissions_drained_organic_soils",
            "description": "",
            "path": f"/{ current_version_prefix }/emissions/emissions_drained_organic_soils",
        },
        {
            "name": "emissions_land_use_fires",
            "description": "",
            "path": f"/{ current_version_prefix }/emissions/emissions_land_use_fires",
        },
        {
            "name": "emissions_land_use_forests",
            "description": "",
            "path": f"/{ current_version_prefix }/emissions/emissions_land_use_forests",
        },
        {
            "name": "emissions_livestock",
            "description": "",
            "path": f"/{ current_version_prefix }/emissions/emissions_livestock",
        },
        {
            "name": "emissions_pre_post_production",
            "description": "",
            "path": f"/{ current_version_prefix }/emissions/emissions_pre_post_production",
        },
        {
            "name": "emissions_totals",
            "description": "",
            "path": f"/{ current_version_prefix }/emissions/emissions_totals",
        },
    ],
}

# Export the sub-API
__all__ = ["emissions_api", "emissions_group_map"]