from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from . import current_version_prefix, api_map

# Import all the routers
from .routers.other import other_api
from .routers.population import population_api
from .routers.indicators import indicators_api
from .routers.food import food_api
from .routers.asti import asti_api
from .routers.commodity import commodity_api
from .routers.emissions import emissions_api
from .routers.employment import employment_api
from .routers.environment import environment_api
from .routers.forestry import forestry_api
from .routers.inputs import inputs_api
from .routers.investment import investment_api
from .routers.prices import prices_api
from .routers.production import production_api


# Create main app
app = FastAPI(
    title="Food Price Analysis API",
    description="API for analyzing global food commodity prices",
    version="1.0.0",
    docs_url="/docs",  # Put docs at root instead of /docs
    redoc_url="/redoc",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "https://app.mickeymalotte.com",
    ],  # Next.js dev server
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(other_api)
app.include_router(population_api)
app.include_router(indicators_api)
app.include_router(food_api)
app.include_router(asti_api)
app.include_router(commodity_api)
app.include_router(emissions_api)
app.include_router(employment_api)
app.include_router(environment_api)
app.include_router(forestry_api)
app.include_router(inputs_api)
app.include_router(investment_api)
app.include_router(prices_api)
app.include_router(production_api)


# Import custom routers (this section preserved during regeneration)
try:
    from fao.src.api_custom.routers import custom_routers
    for custom_router in custom_routers:
        app.include_router(custom_router)
    print(f"✅ Loaded {len(custom_routers)} custom routers")
except ImportError as e:
    print("ℹ️  No custom routers found")
except Exception as e:
    print(f"⚠️  Error loading custom routers: {e}")

# Root endpoint
@app.get("/")
def root():
    return api_map

if __name__ == "__main__":
    import uvicorn
    import signal
    import sys

    def signal_handler(sig, frame):
        print("\nShutting down gracefully...")
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    uvicorn.run("fao.src.api.__main__:app", host="localhost", port=8000, reload=True)