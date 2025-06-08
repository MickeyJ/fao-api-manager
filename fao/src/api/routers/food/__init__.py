from fastapi import APIRouter
from ... import current_version_prefix

from .food_groups import router as food_groups
from .food_balance_sheets_historic import router as food_balance_sheets_historic
from .food_balance_sheets import router as food_balance_sheets
from .food_aid_shipments_wfp import router as food_aid_shipments_wfp
from .food_and_diet_individual_quantitative_dietary_data import router as food_and_diet_individual_quantitative_dietary_data
from .food_security_data import router as food_security_data

food_api = APIRouter(
  prefix=f"/{current_version_prefix}", 
  tags=["food"],
)

food_api.include_router(
  food_groups, 
  prefix=f"/food", 
  tags=["food", "food_groups"],
)
food_api.include_router(
  food_balance_sheets_historic, 
  prefix=f"/food", 
  tags=["food", "food_balance_sheets_historic"],
)
food_api.include_router(
  food_balance_sheets, 
  prefix=f"/food", 
  tags=["food", "food_balance_sheets"],
)
food_api.include_router(
  food_aid_shipments_wfp, 
  prefix=f"/food", 
  tags=["food", "food_aid_shipments_wfp"],
)
food_api.include_router(
  food_and_diet_individual_quantitative_dietary_data, 
  prefix=f"/food", 
  tags=["food", "food_and_diet_individual_quantitative_dietary_data"],
)
food_api.include_router(
  food_security_data, 
  prefix=f"/food", 
  tags=["food", "food_security_data"],
)

food_group_map = {
    "description": "food",
    "routes": [
        {
            "name": "food_groups",
            "description": "",
            "path": f"/{ current_version_prefix }/food/food_groups",
        },
        {
            "name": "food_balance_sheets_historic",
            "description": "",
            "path": f"/{ current_version_prefix }/food/food_balance_sheets_historic",
        },
        {
            "name": "food_balance_sheets",
            "description": "",
            "path": f"/{ current_version_prefix }/food/food_balance_sheets",
        },
        {
            "name": "food_aid_shipments_wfp",
            "description": "",
            "path": f"/{ current_version_prefix }/food/food_aid_shipments_wfp",
        },
        {
            "name": "food_and_diet_individual_quantitative_dietary_data",
            "description": "",
            "path": f"/{ current_version_prefix }/food/food_and_diet_individual_quantitative_dietary_data",
        },
        {
            "name": "food_security_data",
            "description": "",
            "path": f"/{ current_version_prefix }/food/food_security_data",
        },
    ],
}

# Export the sub-API
__all__ = ["food_api", "food_group_map"]