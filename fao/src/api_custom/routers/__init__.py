from .price_analytics import price_analytics_router

# Export all custom routers as a list
custom_routers = [
    price_analytics_router,
    # Add more custom routers here as you create them
]

__all__ = ["custom_routers"]
