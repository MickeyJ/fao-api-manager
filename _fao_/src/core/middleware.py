# static_api_files/src/core/middleware.py
from fastapi import Request
from starlette.types import ASGIApp, Scope, Receive, Send
from urllib.parse import parse_qs as parse_query_string
from urllib.parse import urlencode as encode_query_string

from _fao_.src.core import settings
from _fao_.src.core.versioning import VERSIONS


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


class QueryStringFlatteningMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        query_string = scope.get("query_string", None).decode()
        if scope["type"] == "http" and query_string:
            parsed = parse_query_string(query_string)
            flattened = {}
            for name, values in parsed.items():
                all_values = []
                for value in values:
                    all_values.extend(value.split(","))

                flattened[name] = all_values

            # doseq: Turn lists into repeated parameters, which is better for FastAPI
            scope["query_string"] = encode_query_string(flattened, doseq=True).encode("utf-8")

            await self.app(scope, receive, send)
        else:
            await self.app(scope, receive, send)
