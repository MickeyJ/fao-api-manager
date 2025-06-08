from fastapi import APIRouter
from ... import current_version_prefix

from .indicators import router as indicators
from .indicators_from_household_surveys import router as indicators_from_household_surveys

indicators_api = APIRouter(
  prefix=f"/{current_version_prefix}", 
  tags=["indicators"],
)

indicators_api.include_router(
  indicators, 
  prefix=f"/indicators", 
  tags=["indicators", "indicators"],
)
indicators_api.include_router(
  indicators_from_household_surveys, 
  prefix=f"/indicators", 
  tags=["indicators", "indicators_from_household_surveys"],
)

indicators_group_map = {
    "description": "indicators",
    "routes": [
        {
            "name": "indicators",
            "description": "",
            "path": f"/{ current_version_prefix }/indicators/indicators",
        },
        {
            "name": "indicators_from_household_surveys",
            "description": "",
            "path": f"/{ current_version_prefix }/indicators/indicators_from_household_surveys",
        },
    ],
}

# Export the sub-API
__all__ = ["indicators_api", "indicators_group_map"]