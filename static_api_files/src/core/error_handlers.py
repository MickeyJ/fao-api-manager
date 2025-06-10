# fao/src/core/error_handlers.py
"""
FAO API Error Handlers

Provides consistent error handling and formatting for all API exceptions.
"""
from fastapi import Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime, timezone
from typing import Any
import uuid
import logging

# Import custom exceptions and error codes
from .exceptions import FAOAPIError, ExternalServiceError, ServerError
from .error_codes import ErrorCode

logger = logging.getLogger(__name__)


def add_request_id_header(response: JSONResponse, request_id: str) -> JSONResponse:
    """Add request ID to response headers for easier debugging"""
    response.headers["X-Request-ID"] = request_id
    return response


def get_timestamp() -> str:
    """Get current UTC timestamp in ISO format"""
    return datetime.now(timezone.utc).isoformat()


def sanitize_error_detail(detail: Any) -> str:
    """Sanitize error detail to ensure it's safe to return"""
    if detail is None:
        return "Request failed"

    # Convert to string and limit length
    detail_str = str(detail)
    if len(detail_str) > 500:
        return detail_str[:497] + "..."

    return detail_str


async def fao_exception_handler(request: Request, exc: FAOAPIError) -> JSONResponse:
    """Handle our custom FAO API exceptions"""
    request_id = str(uuid.uuid4())

    # Log the error with full context
    logger.error(
        f"API Error: {exc.error_code}",
        extra={
            "request_id": request_id,
            "path": str(request.url),
            "method": request.method,
            "error_code": exc.error_code,
            "status_code": exc.status_code,
            "error_type": exc.error_type,
            "param": exc.param,
            "client_host": request.client.host if request.client else None,
        },
    )

    response = JSONResponse(status_code=exc.status_code, content=exc.to_dict(request_id))

    return add_request_id_header(response, request_id)


async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    """Handle FastAPI's HTTPException"""
    request_id = str(uuid.uuid4())

    # Map common HTTP exceptions to our format
    error_map = {
        400: ("BAD_REQUEST", "bad_request"),
        401: ("UNAUTHORIZED", "authentication_error"),
        403: ("FORBIDDEN", "authorization_error"),
        404: ("RESOURCE_NOT_FOUND", "resource_not_found"),
        405: ("METHOD_NOT_ALLOWED", "method_not_allowed"),
        409: ("CONFLICT", "conflict_error"),
        422: ("INVALID_REQUEST", "validation_error"),
        429: ("TOO_MANY_REQUESTS", "rate_limit_error"),
        500: ("INTERNAL_ERROR", "server_error"),
        503: ("SERVICE_UNAVAILABLE", "service_unavailable"),
    }

    error_code, error_type = error_map.get(exc.status_code, ("HTTP_ERROR", "http_error"))

    # Sanitize the detail message
    message = sanitize_error_detail(exc.detail)

    # Log HTTP exceptions
    logger.warning(
        f"HTTP Exception: {exc.status_code}",
        extra={
            "request_id": request_id,
            "path": str(request.url),
            "method": request.method,
            "status_code": exc.status_code,
            "detail": message,
        },
    )

    response = JSONResponse(
        status_code=exc.status_code,
        content={
            "error": {
                "type": error_type,
                "code": error_code,
                "message": message,
                "doc_url": f"https://api.fao.org/docs/errors#{error_code}",
            },
            "request_id": request_id,
            "timestamp": get_timestamp(),
        },
    )

    return add_request_id_header(response, request_id)


async def validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    """Handle Pydantic validation errors"""
    request_id = str(uuid.uuid4())

    # Extract validation errors
    errors = exc.errors()

    # Process errors to make them more user-friendly
    processed_errors = []
    for error in errors:
        # Build parameter path (e.g., body.year or query.limit)
        loc = error.get("loc", [])
        param_path = ".".join(str(loc_part) for loc_part in loc[1:] if loc_part != "__root__")

        processed_errors.append(
            {
                "param": param_path or "unknown",
                "message": error.get("msg", "validation failed"),
                "type": error.get("type", "value_error"),
            }
        )

    # Get first error for main message
    first_error = processed_errors[0] if processed_errors else {"param": "unknown", "message": "validation failed"}

    # Log validation errors
    logger.info(
        f"Validation error: {len(errors)} field(s)",
        extra={
            "request_id": request_id,
            "path": str(request.url),
            "method": request.method,
            "validation_errors": processed_errors,
        },
    )

    response = JSONResponse(
        status_code=422,
        content={
            "error": {
                "type": "validation_error",
                "code": "VALIDATION_ERROR",
                "message": f"Invalid value for parameter '{first_error['param']}': {first_error['message']}",
                "param": first_error["param"],
                "detail": f"Validation failed for {len(errors)} field(s)",
                "doc_url": "https://api.fao.org/docs/errors#VALIDATION_ERROR",
                "validation_errors": processed_errors,
            },
            "request_id": request_id,
            "timestamp": get_timestamp(),
        },
    )

    return add_request_id_header(response, request_id)


async def sqlalchemy_exception_handler(request: Request, exc: SQLAlchemyError) -> JSONResponse:
    """Handle database errors"""
    request_id = str(uuid.uuid4())

    # Determine error type based on exception
    error_detail = str(exc.__class__.__name__)

    # Log the actual error for debugging (but don't expose details)
    logger.error(
        f"Database error: {error_detail}",
        exc_info=exc,
        extra={
            "request_id": request_id,
            "path": str(request.url),
            "method": request.method,
            "error_type": error_detail,
        },
    )

    # Check if it's a connection error
    if "connect" in str(exc).lower():
        db_error = ExternalServiceError(service="database", message="Unable to connect to database service")
    else:
        db_error = ExternalServiceError(service="database", message="Database operation failed")

    response = JSONResponse(status_code=db_error.status_code, content=db_error.to_dict(request_id))

    return add_request_id_header(response, request_id)


async def generic_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Catch-all for unexpected exceptions"""
    request_id = str(uuid.uuid4())

    # Log the full exception with context
    logger.error(
        f"Unexpected error: {type(exc).__name__}",
        exc_info=exc,
        extra={
            "request_id": request_id,
            "path": str(request.url),
            "method": request.method,
            "query_params": dict(request.query_params),
            "client_host": request.client.host if request.client else None,
        },
    )

    # Create generic error response
    error = ServerError(
        message="An unexpected error occurred. Please try again later.",
        error_code=ErrorCode.INTERNAL_ERROR,
        metadata={"request_id": request_id},
    )

    response = JSONResponse(status_code=500, content=error.to_dict(request_id))

    return add_request_id_header(response, request_id)


# Optional: Add a health check exception handler
async def health_check_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Special handler for health check endpoint failures"""
    request_id = str(uuid.uuid4())

    logger.error("Health check failed", exc_info=exc, extra={"request_id": request_id})

    response = JSONResponse(
        status_code=503,
        content={
            "status": "unhealthy",
            "error": {
                "type": "health_check_failed",
                "code": "HEALTH_CHECK_FAILED",
                "message": "Service health check failed",
            },
            "request_id": request_id,
            "timestamp": get_timestamp(),
        },
    )

    return add_request_id_header(response, request_id)
