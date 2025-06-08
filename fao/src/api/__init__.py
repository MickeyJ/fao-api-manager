current_version_prefix = "v1"

from .routers.other import other_group_map      
from .routers.population import population_group_map      
from .routers.indicators import indicators_group_map      
from .routers.food import food_group_map      
from .routers.asti import asti_group_map      
from .routers.commodity import commodity_group_map      
from .routers.emissions import emissions_group_map      
from .routers.employment import employment_group_map      
from .routers.environment import environment_group_map      
from .routers.forestry import forestry_group_map      
from .routers.inputs import inputs_group_map      
from .routers.investment import investment_group_map      
from .routers.prices import prices_group_map      
from .routers.production import production_group_map      

api_map = {
    "api_name": "FAO API",
    "api_description": "API for accessing FAO datasets",
    "version": "1.0.0",
    "docs": "/docs",
    "redoc": "/redoc",
    "endpoints": {
        "other": other_group_map,
        "population": population_group_map,
        "indicators": indicators_group_map,
        "food": food_group_map,
        "asti": asti_group_map,
        "commodity": commodity_group_map,
        "emissions": emissions_group_map,
        "employment": employment_group_map,
        "environment": environment_group_map,
        "forestry": forestry_group_map,
        "inputs": inputs_group_map,
        "investment": investment_group_map,
        "prices": prices_group_map,
        "production": production_group_map,
    },
}