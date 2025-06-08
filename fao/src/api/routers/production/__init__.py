from fastapi import APIRouter
from ... import current_version_prefix

from .production_crops_livestock import router as production_crops_livestock
from .production_indices import router as production_indices

production_api = APIRouter(
  prefix=f"/{current_version_prefix}", 
  tags=["production"],
)

production_api.include_router(
  production_crops_livestock, 
  prefix=f"/production", 
  tags=["production", "production_crops_livestock"],
)
production_api.include_router(
  production_indices, 
  prefix=f"/production", 
  tags=["production", "production_indices"],
)

production_group_map = {
    "description": "production",
    "routes": [
        {
            "name": "production_crops_livestock",
            "description": "",
            "path": f"/{ current_version_prefix }/production/production_crops_livestock",
        },
        {
            "name": "production_indices",
            "description": "",
            "path": f"/{ current_version_prefix }/production/production_indices",
        },
    ],
}

# Export the sub-API
__all__ = ["production_api", "production_group_map"]