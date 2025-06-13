# fao/src/core/exceptions.py
"""
FAO API Exception Classes

Provides a comprehensive error handling system following professional API standards.
Based on Stripe's error model, adapted for agricultural data needs.
"""
from typing import Optional, Dict, Any, List
from datetime import datetime
from .error_codes import ErrorCode, get_error_message


class FAOAPIError(Exception):
    """
    Base exception for all FAO API errors.

    Provides consistent error structure across the entire API.
    """

    def __init__(
        self,
        message: str,
        error_type: str,
        error_code: ErrorCode | str,  # Accept both enum and string
        status_code: int = 400,
        param: Optional[str] = None,
        detail: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize FAO API Error.

        Args:
            message: Human-readable error message
            error_type: Category of error (validation_error, data_not_found, etc.)
            error_code: Machine-readable error code (ErrorCode enum or string)
            status_code: HTTP status code
            param: Parameter that caused the error (if applicable)
            detail: Extended explanation of the error
            metadata: Additional context-specific information
        """
        self.message = message
        self.error_type = error_type
        # Convert enum to string value
        self.error_code = error_code.value if isinstance(error_code, ErrorCode) else error_code
        self.status_code = status_code
        self.param = param
        self.detail = detail
        self.metadata = metadata or {}
        super().__init__(self.message)

    def to_dict(self, request_id: str) -> Dict[str, Any]:
        """Convert exception to API response format."""
        error_dict = {
            "error": {
                "type": self.error_type,
                "code": self.error_code,
                "message": self.message,
                "doc_url": f"https://api.fao.org/docs/errors#{self.error_code}",
            },
            "request_id": request_id,
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }

        # Add optional fields if present
        if self.param:
            error_dict["error"]["param"] = self.param
        if self.detail:
            error_dict["error"]["detail"] = self.detail
        if self.metadata:
            error_dict["error"]["metadata"] = self.metadata

        return error_dict


class ValidationError(FAOAPIError):
    """Raised when input parameters fail validation."""

    def __init__(self, message: str, error_code: str, param: str, **kwargs):
        super().__init__(
            message=message,
            error_type="validation_error",
            error_code=error_code,
            status_code=400,
            param=param,
            **kwargs,
        )


class DataNotFoundError(FAOAPIError):
    """Raised when requested data doesn't exist."""

    def __init__(self, message: str, error_code: str = ErrorCode.NO_DATA_FOR_QUERY, **kwargs):
        super().__init__(message=message, error_type="data_not_found", error_code=error_code, status_code=404, **kwargs)


class BusinessLogicError(FAOAPIError):
    """Raised when request is valid but cannot be processed due to business rules."""

    def __init__(self, message: str, error_code: str, **kwargs):
        super().__init__(
            message=message, error_type="business_logic_error", error_code=error_code, status_code=422, **kwargs
        )


class AuthenticationError(FAOAPIError):
    """Raised when authentication fails."""

    def __init__(
        self, message: Optional[str] = None, error_code: ErrorCode = ErrorCode.AUTHENTICATION_REQUIRED, **kwargs
    ):
        if message is None:
            message = get_error_message(error_code)
        super().__init__(
            message=message, error_type="authentication_error", error_code=error_code, status_code=401, **kwargs
        )


class AuthorizationError(FAOAPIError):
    """Raised when user lacks permission for requested resource."""

    def __init__(
        self, message: Optional[str] = None, error_code: ErrorCode = ErrorCode.INSUFFICIENT_PERMISSIONS, **kwargs
    ):
        if message is None:
            message = get_error_message(error_code)
        super().__init__(
            message=message, error_type="authorization_error", error_code=error_code, status_code=403, **kwargs
        )


class RateLimitError(FAOAPIError):
    """Raised when rate limit is exceeded."""

    def __init__(
        self,
        message: Optional[str] = None,
        error_code: ErrorCode = ErrorCode.RATE_LIMIT_EXCEEDED,
        reset_time: Optional[datetime] = None,
        limit: Optional[int] = None,
        period: Optional[str] = None,
        **kwargs,
    ):
        metadata = kwargs.get("metadata", {})
        if reset_time:
            metadata["reset_time"] = reset_time.isoformat() + "Z"
        if limit:
            metadata["limit"] = limit
        if period:
            metadata["period"] = period

        kwargs["metadata"] = metadata

        # Generate message from template if not provided
        if message is None:
            message = get_error_message(error_code, limit=limit, period=period or "hour")

        super().__init__(
            message=message, error_type="rate_limit_error", error_code=error_code, status_code=429, **kwargs
        )


class ServerError(FAOAPIError):
    """Raised when server encounters an error."""

    def __init__(self, message: Optional[str] = None, error_code: ErrorCode = ErrorCode.INTERNAL_ERROR, **kwargs):
        # Always use safe message for server errors
        if message is None:
            message = get_error_message(error_code)

        super().__init__(message=message, error_type="server_error", error_code=error_code, status_code=500, **kwargs)


class ExternalServiceError(FAOAPIError):
    """Raised when external service (database, cache, etc.) is unavailable."""

    def __init__(self, service: str, message: Optional[str] = None, **kwargs):
        if message is None:
            message = get_error_message(ErrorCode.EXTERNAL_SERVICE_UNAVAILABLE, service=service)

        super().__init__(
            message=message,
            error_type="external_service_error",
            error_code=ErrorCode.EXTERNAL_SERVICE_UNAVAILABLE,
            status_code=503,
            metadata={"service": service},
            **kwargs,
        )


class DataQualityError(FAOAPIError):
    """Raised when data quality issues prevent processing."""

    def __init__(
        self,
        message: str,
        error_code: ErrorCode = ErrorCode.DATA_QUALITY_THRESHOLD,
        quality_flags: Optional[List[str]] = None,
        **kwargs,
    ):
        metadata = kwargs.get("metadata", {})
        if quality_flags:
            metadata["quality_flags"] = quality_flags

        kwargs["metadata"] = metadata

        super().__init__(
            message=message, error_type="data_quality_error", error_code=error_code, status_code=422, **kwargs
        )


class ConfigurationError(FAOAPIError):
    """Raised when system configuration is invalid."""

    def __init__(self, message: Optional[str] = None, error_code: ErrorCode = ErrorCode.CONFIGURATION_ERROR, **kwargs):
        if message is None:
            message = get_error_message(error_code)

        super().__init__(
            message=message, error_type="configuration_error", error_code=error_code, status_code=500, **kwargs
        )


# Convenience functions for common errors


def invalid_parameter(param: str, value: Any, reason: str) -> ValidationError:
    """Create a validation error for invalid parameter."""
    return ValidationError(
        message=get_error_message(ErrorCode.INVALID_PARAMETER, param=param, reason=reason),
        error_code=ErrorCode.INVALID_PARAMETER,
        param=param,
        detail=f"Received value: {value}",
    )


def missing_parameter(param: str) -> ValidationError:
    """Create a validation error for missing required parameter."""
    return ValidationError(
        message=get_error_message(ErrorCode.MISSING_REQUIRED_PARAMETER, param=param),
        error_code=ErrorCode.MISSING_REQUIRED_PARAMETER,
        param=param,
    )


def no_data_found(dataset: str, filters: Dict[str, Any]) -> DataNotFoundError:
    """Create a data not found error with context."""
    filter_str = ", ".join([f"{k}={v}" for k, v in filters.items()])
    return DataNotFoundError(
        message=f"No data found in {dataset} for filters: {filter_str}",
        error_code=ErrorCode.NO_DATA_FOR_QUERY,
        metadata={"dataset": dataset, "filters": filters},
    )


def incompatible_parameters(param1: str, param2: str, reason: str = "") -> ValidationError:
    """Create an error for incompatible parameter combination."""
    message = get_error_message(ErrorCode.INCOMPATIBLE_PARAMETERS, param1=param1, param2=param2)
    if reason:
        message += f": {reason}"

    return ValidationError(
        message=message,
        error_code=ErrorCode.INCOMPATIBLE_PARAMETERS,
        param=f"{param1},{param2}",
        metadata={"param1": param1, "param2": param2},
    )


def invalid_area_code(value: str) -> ValidationError:
    """Create an error for invalid area code."""
    return ValidationError(
        message=get_error_message(ErrorCode.INVALID_AREA_CODE, value=value),
        error_code=ErrorCode.INVALID_AREA_CODE,
        param="area_code",
        detail=f"Received: '{value}'. Use /area_codes endpoint to see valid codes.",
    )


def invalid_item_code(value: str) -> ValidationError:
    """Create an error for invalid item code."""
    return ValidationError(
        message=get_error_message(ErrorCode.INVALID_ITEM_CODE, value=value),
        error_code=ErrorCode.INVALID_ITEM_CODE,
        param="item_code",
        detail=f"Received: '{value}'. Use /item_codes endpoint to see valid codes.",
    )


def invalid_element_code(value: str) -> ValidationError:
    """Create an error for invalid element code."""
    return ValidationError(
        message=get_error_message(ErrorCode.INVALID_ELEMENT_CODE, value=value),
        error_code=ErrorCode.INVALID_ELEMENT_CODE,
        param="element_code",
        detail=f"Received: '{value}'. Use /elements endpoint to see valid codes.",
    )


def invalid_year_range(value: int) -> ValidationError:
    """Create an error for year outside valid range."""
    return ValidationError(
        message=get_error_message(ErrorCode.INVALID_YEAR_RANGE, value=value),
        error_code=ErrorCode.INVALID_YEAR_RANGE,
        param="year",
        detail=f"FAO data is available from 1961 to 2024. Requested year: {value}",
    )
