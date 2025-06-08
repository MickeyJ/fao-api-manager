from fastapi import APIRouter
from ... import current_version_prefix

from .employment_indicators_agriculture import router as employment_indicators_agriculture
from .employment_indicators_rural import router as employment_indicators_rural

employment_api = APIRouter(
  prefix=f"/{current_version_prefix}", 
  tags=["employment"],
)

employment_api.include_router(
  employment_indicators_agriculture, 
  prefix=f"/employment", 
  tags=["employment", "employment_indicators_agriculture"],
)
employment_api.include_router(
  employment_indicators_rural, 
  prefix=f"/employment", 
  tags=["employment", "employment_indicators_rural"],
)

employment_group_map = {
    "description": "employment",
    "routes": [
        {
            "name": "employment_indicators_agriculture",
            "description": "",
            "path": f"/{ current_version_prefix }/employment/employment_indicators_agriculture",
        },
        {
            "name": "employment_indicators_rural",
            "description": "",
            "path": f"/{ current_version_prefix }/employment/employment_indicators_rural",
        },
    ],
}

# Export the sub-API
__all__ = ["employment_api", "employment_group_map"]