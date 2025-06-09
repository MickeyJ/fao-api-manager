# static_api_files/src/core/middleware.py
from fastapi import Request
from datetime import datetime
from ..core import settings
from ..core.versioning import VERSIONS


async def add_version_headers(request: Request, call_next):
    """Add API version information to response headers"""
    response = await call_next(request)

    # Add version headers
    response.headers["X-API-Version"] = settings.api_version
    response.headers["X-API-Version-Major"] = settings.api_version_prefix

    # Check version status from registry
    current_version = VERSIONS.get(settings.api_version_prefix)

    if current_version and current_version.is_deprecated:
        response.headers["X-API-Deprecation"] = "true"

        if current_version.sunset_date:
            response.headers["X-API-Deprecation-Date"] = current_version.sunset_date.isoformat()

            days_left = current_version.days_until_sunset
            if days_left:
                response.headers["X-API-Deprecation-Info"] = (
                    f"This version will be sunset in {days_left} days. Please migrate to the latest version."
                )

    return response
