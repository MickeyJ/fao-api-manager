from fastapi import APIRouter
from ... import current_version_prefix

from .inputs_fertilizers_archive import router as inputs_fertilizers_archive
from .inputs_fertilizers_nutrient import router as inputs_fertilizers_nutrient
from .inputs_fertilizers_product import router as inputs_fertilizers_product
from .inputs_land_use import router as inputs_land_use
from .inputs_pesticides_trade import router as inputs_pesticides_trade
from .inputs_pesticides_use import router as inputs_pesticides_use

inputs_api = APIRouter(
  prefix=f"/{current_version_prefix}", 
  tags=["inputs"],
)

inputs_api.include_router(
  inputs_fertilizers_archive, 
  prefix=f"/inputs", 
  tags=["inputs", "inputs_fertilizers_archive"],
)
inputs_api.include_router(
  inputs_fertilizers_nutrient, 
  prefix=f"/inputs", 
  tags=["inputs", "inputs_fertilizers_nutrient"],
)
inputs_api.include_router(
  inputs_fertilizers_product, 
  prefix=f"/inputs", 
  tags=["inputs", "inputs_fertilizers_product"],
)
inputs_api.include_router(
  inputs_land_use, 
  prefix=f"/inputs", 
  tags=["inputs", "inputs_land_use"],
)
inputs_api.include_router(
  inputs_pesticides_trade, 
  prefix=f"/inputs", 
  tags=["inputs", "inputs_pesticides_trade"],
)
inputs_api.include_router(
  inputs_pesticides_use, 
  prefix=f"/inputs", 
  tags=["inputs", "inputs_pesticides_use"],
)

inputs_group_map = {
    "description": "inputs",
    "routes": [
        {
            "name": "inputs_fertilizers_archive",
            "description": "",
            "path": f"/{ current_version_prefix }/inputs/inputs_fertilizers_archive",
        },
        {
            "name": "inputs_fertilizers_nutrient",
            "description": "",
            "path": f"/{ current_version_prefix }/inputs/inputs_fertilizers_nutrient",
        },
        {
            "name": "inputs_fertilizers_product",
            "description": "",
            "path": f"/{ current_version_prefix }/inputs/inputs_fertilizers_product",
        },
        {
            "name": "inputs_land_use",
            "description": "",
            "path": f"/{ current_version_prefix }/inputs/inputs_land_use",
        },
        {
            "name": "inputs_pesticides_trade",
            "description": "",
            "path": f"/{ current_version_prefix }/inputs/inputs_pesticides_trade",
        },
        {
            "name": "inputs_pesticides_use",
            "description": "",
            "path": f"/{ current_version_prefix }/inputs/inputs_pesticides_use",
        },
    ],
}

# Export the sub-API
__all__ = ["inputs_api", "inputs_group_map"]