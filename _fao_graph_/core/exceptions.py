# fao/src/core/exceptions.py
"""
FAO API Exception Classes

Provides a comprehensive error handling system following professional API standards.
Based on Stripe's error model, adapted for agricultural data needs.
"""
from typing import Optional, Dict, Any, List
from datetime import datetime
from .error_codes import ErrorCode, get_error_message


class MigrationError(Exception):
    """Base exception for migration errors."""

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


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


class CacheError(FAOAPIError):
    """Base exception for cache-related errors."""

    def __init__(self, message: Optional[str] = None, error_code: ErrorCode = ErrorCode.CACHE_ERROR, **kwargs):
        if message is None:
            message = get_error_message(error_code)

        super().__init__(
            message=message,
            error_type="cache_error",
            error_code=error_code,
            status_code=503,  # Service Unavailable
            **kwargs,
        )


class CacheConnectionError(CacheError):
    """Raised when cache service connection fails."""

    def __init__(self, service: str = "Redis", message: Optional[str] = None, **kwargs):
        if message is None:
            message = get_error_message(ErrorCode.CACHE_CONNECTION_FAILED, service=service)

        super().__init__(
            message=message, error_code=ErrorCode.CACHE_CONNECTION_FAILED, metadata={"service": service}, **kwargs
        )


class CacheOperationError(CacheError):
    """Raised when cache read/write operation fails."""

    def __init__(self, operation: str, key: Optional[str] = None, message: Optional[str] = None, **kwargs):
        if message is None:
            message = get_error_message(ErrorCode.CACHE_OPERATION_FAILED, operation=operation)
            if key:
                message += f" for key: {key[:50]}"  # Truncate long keys

        metadata = kwargs.get("metadata", {})
        metadata["operation"] = operation
        if key:
            metadata["key"] = key[:50]

        kwargs["metadata"] = metadata

        super().__init__(message=message, error_code=ErrorCode.CACHE_OPERATION_FAILED, **kwargs)


class CacheSerializationError(CacheError):
    """Raised when cache data serialization/deserialization fails."""

    def __init__(
        self,
        action: str = "serialize",  # "serialize" or "deserialize"
        data_type: Optional[str] = None,
        message: Optional[str] = None,
        **kwargs,
    ):
        if message is None:
            message = get_error_message(ErrorCode.CACHE_SERIALIZATION_ERROR, action=action)
            if data_type:
                message += f" for type: {data_type}"

        super().__init__(
            message=message,
            error_code=ErrorCode.CACHE_SERIALIZATION_ERROR,
            metadata={"action": action, "data_type": data_type},
            **kwargs,
        )


# Convenience functions for common errors


def cache_connection_failed(service: str = "Redis", error: Optional[Exception] = None) -> CacheConnectionError:
    """Create a cache connection error with context."""
    return CacheConnectionError(
        service=service,
        message=get_error_message(ErrorCode.CACHE_CONNECTION_FAILED, service=service),
        detail=str(error) if error else None,
    )


def cache_read_failed(key: str, error: Optional[Exception] = None) -> CacheOperationError:
    """Create a cache read error."""
    return CacheOperationError(
        operation="read",
        key=key,
        message=get_error_message(ErrorCode.CACHE_OPERATION_FAILED, operation="read"),
        detail=str(error) if error else None,
    )


def cache_write_failed(key: str, error: Optional[Exception] = None) -> CacheOperationError:
    """Create a cache write error."""
    return CacheOperationError(
        operation="write",
        key=key,
        message=get_error_message(ErrorCode.CACHE_OPERATION_FAILED, operation="write"),
        detail=str(error) if error else None,
    )


def cache_delete_failed(pattern: str, error: Optional[Exception] = None) -> CacheOperationError:
    """Create a cache delete error."""
    return CacheOperationError(
        operation="delete",
        key=pattern,
        message=get_error_message(ErrorCode.CACHE_OPERATION_FAILED, operation="delete"),
        detail=str(error) if error else None,
    )


def cache_serialization_failed(data_type: type, error: Optional[Exception] = None) -> CacheSerializationError:
    """Create a serialization error."""
    return CacheSerializationError(
        action="serialize",
        data_type=data_type.__name__,
        message=get_error_message(ErrorCode.CACHE_SERIALIZATION_ERROR, action="serialize"),
        detail=str(error) if error else None,
    )


def cache_deserialization_failed(error: Optional[Exception] = None) -> CacheSerializationError:
    """Create a deserialization error."""
    return CacheSerializationError(
        action="deserialize",
        message=get_error_message(ErrorCode.CACHE_SERIALIZATION_ERROR, action="deserialize"),
        detail=str(error) if error else None,
    )


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


def invalid_flag(value: str) -> ValidationError:
    """Create an error for invalid flag."""
    return ValidationError(
        message=get_error_message(ErrorCode.INVALID_FLAG_CODE, value=value),
        error_code=ErrorCode.INVALID_FLAG_CODE,
        param="flag",
        detail=f"Received: '{value}'. Use /flags endpoint to see valid codes.",
    )


def invalid_donor_code(value: str) -> ValidationError:
    """Create an error for invalid donor code."""
    return ValidationError(
        message=get_error_message(ErrorCode.INVALID_DONOR_CODE, value=value),
        error_code=ErrorCode.INVALID_DONOR_CODE,
        param="donor_code",
        detail=f"Received: '{value}'. Use /donors endpoint to see valid codes.",
    )


def invalid_source_code(value: str) -> ValidationError:
    """Create an error for invalid source code."""
    return ValidationError(
        message=get_error_message(ErrorCode.INVALID_SOURCE_CODE, value=value),
        error_code=ErrorCode.INVALID_SOURCE_CODE,
        param="source_code",
        detail=f"Received: '{value}'. Use /sources endpoint to see valid codes.",
    )


def invalid_purpose_code(value: str) -> ValidationError:
    """Create an error for invalid purpose code."""
    return ValidationError(
        message=get_error_message(ErrorCode.INVALID_PURPOSE_CODE, value=value),
        error_code=ErrorCode.INVALID_PURPOSE_CODE,
        param="purpose_code",
        detail=f"Received: '{value}'. Use /purposes endpoint to see valid codes.",
    )


def invalid_sex_code(value: str) -> ValidationError:
    """Create an error for invalid sex code."""
    return ValidationError(
        message=get_error_message(ErrorCode.INVALID_SEX_CODE, value=value),
        error_code=ErrorCode.INVALID_SEX_CODE,
        param="sex_code",
        detail=f"Received: '{value}'. Use /sexs endpoint to see valid codes.",
    )


def invalid_release_code(value: str) -> ValidationError:
    """Create an error for invalid release code."""
    return ValidationError(
        message=get_error_message(ErrorCode.INVALID_RELEASE_CODE, value=value),
        error_code=ErrorCode.INVALID_RELEASE_CODE,
        param="release_code",
        detail=f"Received: '{value}'. Use /releases endpoint to see valid codes.",
    )


def invalid_reporter_country_code(value: str) -> ValidationError:
    """Create an error for invalid reporter_country code."""
    return ValidationError(
        message=get_error_message(ErrorCode.INVALID_REPORTER_COUNTRY_CODE, value=value),
        error_code=ErrorCode.INVALID_REPORTER_COUNTRY_CODE,
        param="reporter_country_code",
        detail=f"Received: '{value}'. Use /reporter_country_codes endpoint to see valid codes.",
    )


def invalid_partner_country_code(value: str) -> ValidationError:
    """Create an error for invalid partner_country code."""
    return ValidationError(
        message=get_error_message(ErrorCode.INVALID_PARTNER_COUNTRY_CODE, value=value),
        error_code=ErrorCode.INVALID_PARTNER_COUNTRY_CODE,
        param="partner_country_code",
        detail=f"Received: '{value}'. Use /partner_country_codes endpoint to see valid codes.",
    )


def invalid_recipient_country_code(value: str) -> ValidationError:
    """Create an error for invalid recipient_country code."""
    return ValidationError(
        message=get_error_message(ErrorCode.INVALID_RECIPIENT_COUNTRY_CODE, value=value),
        error_code=ErrorCode.INVALID_RECIPIENT_COUNTRY_CODE,
        param="recipient_country_code",
        detail=f"Received: '{value}'. Use /recipient_country_codes endpoint to see valid codes.",
    )


def invalid_currency_code(value: str) -> ValidationError:
    """Create an error for invalid currency code."""
    return ValidationError(
        message=get_error_message(ErrorCode.INVALID_CURRENCY_CODE, value=value),
        error_code=ErrorCode.INVALID_CURRENCY_CODE,
        param="currency_code",
        detail=f"Received: '{value}'. Use /currencies endpoint to see valid codes.",
    )


def invalid_survey_code(value: str) -> ValidationError:
    """Create an error for invalid survey code."""
    return ValidationError(
        message=get_error_message(ErrorCode.INVALID_SURVEY_CODE, value=value),
        error_code=ErrorCode.INVALID_SURVEY_CODE,
        param="survey_code",
        detail=f"Received: '{value}'. Use /surveys endpoint to see valid codes.",
    )


def invalid_population_age_group_code(value: str) -> ValidationError:
    """Create an error for invalid population age group code."""
    return ValidationError(
        message=get_error_message(ErrorCode.INVALID_POPULATION_AGE_GROUP_CODE, value=value),
        error_code=ErrorCode.INVALID_POPULATION_AGE_GROUP_CODE,
        param="population_age_group_code",
        detail=f"Received: '{value}'. Use /population_age_groups endpoint to see valid codes.",
    )


def invalid_indicator_code(value: str) -> ValidationError:
    """Create an error for invalid indicator code."""
    return ValidationError(
        message=get_error_message(ErrorCode.INVALID_INDICATOR_CODE, value=value),
        error_code=ErrorCode.INVALID_INDICATOR_CODE,
        param="indicator_code",
        detail=f"Received: '{value}'. Use /indicators endpoint to see valid codes.",
    )


def invalid_food_group_code(value: str) -> ValidationError:
    """Create an error for invalid food group code."""
    return ValidationError(
        message=get_error_message(ErrorCode.INVALID_FOOD_GROUP_CODE, value=value),
        error_code=ErrorCode.INVALID_FOOD_GROUP_CODE,
        param="food_group_code",
        detail=f"Received: '{value}'. Use /food_groups endpoint to see valid codes.",
    )


def invalid_geographic_level_code(value: str) -> ValidationError:
    """Create an error for invalid geographic level code."""
    return ValidationError(
        message=get_error_message(ErrorCode.INVALID_GEOGRAPHIC_LEVEL_CODE, value=value),
        error_code=ErrorCode.INVALID_GEOGRAPHIC_LEVEL_CODE,
        param="geographic_level_code",
        detail=f"Received: '{value}'. Use /geographic_levels endpoint to see valid codes.",
    )


def invalid_year_range(value: int) -> ValidationError:
    """Create an error for year outside valid range."""
    return ValidationError(
        message=get_error_message(ErrorCode.INVALID_YEAR_RANGE, value=value),
        error_code=ErrorCode.INVALID_YEAR_RANGE,
        param="year",
        detail=f"FAO data is available from 1961 to 2024. Requested year: {value}",
    )
