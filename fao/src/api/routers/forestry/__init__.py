from fastapi import APIRouter
from ... import current_version_prefix

from .forestry import router as forestry
from .forestry_pulp_paper_survey import router as forestry_pulp_paper_survey
from .forestry_trade_flows import router as forestry_trade_flows

forestry_api = APIRouter(
  prefix=f"/{current_version_prefix}", 
  tags=["forestry"],
)

forestry_api.include_router(
  forestry, 
  prefix=f"/forestry", 
  tags=["forestry", "forestry"],
)
forestry_api.include_router(
  forestry_pulp_paper_survey, 
  prefix=f"/forestry", 
  tags=["forestry", "forestry_pulp_paper_survey"],
)
forestry_api.include_router(
  forestry_trade_flows, 
  prefix=f"/forestry", 
  tags=["forestry", "forestry_trade_flows"],
)

forestry_group_map = {
    "description": "forestry",
    "routes": [
        {
            "name": "forestry",
            "description": "",
            "path": f"/{ current_version_prefix }/forestry/forestry",
        },
        {
            "name": "forestry_pulp_paper_survey",
            "description": "",
            "path": f"/{ current_version_prefix }/forestry/forestry_pulp_paper_survey",
        },
        {
            "name": "forestry_trade_flows",
            "description": "",
            "path": f"/{ current_version_prefix }/forestry/forestry_trade_flows",
        },
    ],
}

# Export the sub-API
__all__ = ["forestry_api", "forestry_group_map"]