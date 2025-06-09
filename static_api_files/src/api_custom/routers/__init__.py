from .price_analytics import price_analytics_router
from .versions import versions_router

custom_routers = [
    price_analytics_router,
    versions_router,
]

__all__ = ["custom_routers"]
