from fastapi import APIRouter
from ... import current_version_prefix

from .prices_archive import router as prices_archive
from .prices import router as prices

prices_api = APIRouter(
  prefix=f"/{current_version_prefix}", 
  tags=["prices"],
)

prices_api.include_router(
  prices_archive, 
  prefix=f"/prices", 
  tags=["prices", "prices_archive"],
)
prices_api.include_router(
  prices, 
  prefix=f"/prices", 
  tags=["prices", "prices"],
)

prices_group_map = {
    "description": "prices",
    "routes": [
        {
            "name": "prices_archive",
            "description": "",
            "path": f"/{ current_version_prefix }/prices/prices_archive",
        },
        {
            "name": "prices",
            "description": "",
            "path": f"/{ current_version_prefix }/prices/prices",
        },
    ],
}

# Export the sub-API
__all__ = ["prices_api", "prices_group_map"]