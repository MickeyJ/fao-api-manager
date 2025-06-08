from fastapi import APIRouter
from ... import current_version_prefix

from .population_age_groups import router as population_age_groups
from .population import router as population

population_api = APIRouter(
  prefix=f"/{current_version_prefix}", 
  tags=["population"],
)

population_api.include_router(
  population_age_groups, 
  prefix=f"/population", 
  tags=["population", "population_age_groups"],
)
population_api.include_router(
  population, 
  prefix=f"/population", 
  tags=["population", "population"],
)

population_group_map = {
    "description": "population",
    "routes": [
        {
            "name": "population_age_groups",
            "description": "",
            "path": f"/{ current_version_prefix }/population/population_age_groups",
        },
        {
            "name": "population",
            "description": "",
            "path": f"/{ current_version_prefix }/population/population",
        },
    ],
}

# Export the sub-API
__all__ = ["population_api", "population_group_map"]