from fastapi import APIRouter
from ... import current_version_prefix

from .investment_capital_stock import router as investment_capital_stock
from .investment_country_investment_statistics_profile import router as investment_country_investment_statistics_profile
from .investment_credit_agriculture import router as investment_credit_agriculture
from .investment_foreign_direct_investment import router as investment_foreign_direct_investment
from .investment_government_expenditure import router as investment_government_expenditure
from .investment_machinery_archive import router as investment_machinery_archive
from .investment_machinery import router as investment_machinery

investment_api = APIRouter(
  prefix=f"/{current_version_prefix}", 
  tags=["investment"],
)

investment_api.include_router(
  investment_capital_stock, 
  prefix=f"/investment", 
  tags=["investment", "investment_capital_stock"],
)
investment_api.include_router(
  investment_country_investment_statistics_profile, 
  prefix=f"/investment", 
  tags=["investment", "investment_country_investment_statistics_profile"],
)
investment_api.include_router(
  investment_credit_agriculture, 
  prefix=f"/investment", 
  tags=["investment", "investment_credit_agriculture"],
)
investment_api.include_router(
  investment_foreign_direct_investment, 
  prefix=f"/investment", 
  tags=["investment", "investment_foreign_direct_investment"],
)
investment_api.include_router(
  investment_government_expenditure, 
  prefix=f"/investment", 
  tags=["investment", "investment_government_expenditure"],
)
investment_api.include_router(
  investment_machinery_archive, 
  prefix=f"/investment", 
  tags=["investment", "investment_machinery_archive"],
)
investment_api.include_router(
  investment_machinery, 
  prefix=f"/investment", 
  tags=["investment", "investment_machinery"],
)

investment_group_map = {
    "description": "investment",
    "routes": [
        {
            "name": "investment_capital_stock",
            "description": "",
            "path": f"/{ current_version_prefix }/investment/investment_capital_stock",
        },
        {
            "name": "investment_country_investment_statistics_profile",
            "description": "",
            "path": f"/{ current_version_prefix }/investment/investment_country_investment_statistics_profile",
        },
        {
            "name": "investment_credit_agriculture",
            "description": "",
            "path": f"/{ current_version_prefix }/investment/investment_credit_agriculture",
        },
        {
            "name": "investment_foreign_direct_investment",
            "description": "",
            "path": f"/{ current_version_prefix }/investment/investment_foreign_direct_investment",
        },
        {
            "name": "investment_government_expenditure",
            "description": "",
            "path": f"/{ current_version_prefix }/investment/investment_government_expenditure",
        },
        {
            "name": "investment_machinery_archive",
            "description": "",
            "path": f"/{ current_version_prefix }/investment/investment_machinery_archive",
        },
        {
            "name": "investment_machinery",
            "description": "",
            "path": f"/{ current_version_prefix }/investment/investment_machinery",
        },
    ],
}

# Export the sub-API
__all__ = ["investment_api", "investment_group_map"]