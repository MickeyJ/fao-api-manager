from fastapi import APIRouter
from ... import current_version_prefix

from .commodity_balances_non_food_2013_old_methodology import router as commodity_balances_non_food_2013_old_methodology
from .commodity_balances_non_food_2010 import router as commodity_balances_non_food_2010
from .commodity_balances_non_food import router as commodity_balances_non_food

commodity_api = APIRouter(
  prefix=f"/{current_version_prefix}", 
  tags=["commodity"],
)

commodity_api.include_router(
  commodity_balances_non_food_2013_old_methodology, 
  prefix=f"/commodity", 
  tags=["commodity", "commodity_balances_non_food_2013_old_methodology"],
)
commodity_api.include_router(
  commodity_balances_non_food_2010, 
  prefix=f"/commodity", 
  tags=["commodity", "commodity_balances_non_food_2010"],
)
commodity_api.include_router(
  commodity_balances_non_food, 
  prefix=f"/commodity", 
  tags=["commodity", "commodity_balances_non_food"],
)

commodity_group_map = {
    "description": "commodity",
    "routes": [
        {
            "name": "commodity_balances_non_food_2013_old_methodology",
            "description": "",
            "path": f"/{ current_version_prefix }/commodity/commodity_balances_non_food_2013_old_methodology",
        },
        {
            "name": "commodity_balances_non_food_2010",
            "description": "",
            "path": f"/{ current_version_prefix }/commodity/commodity_balances_non_food_2010",
        },
        {
            "name": "commodity_balances_non_food",
            "description": "",
            "path": f"/{ current_version_prefix }/commodity/commodity_balances_non_food",
        },
    ],
}

# Export the sub-API
__all__ = ["commodity_api", "commodity_group_map"]