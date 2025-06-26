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
        error_code: ErrorCode | str,
        status_code: int = 400,
        params: Optional[str | list[str]] = None,
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
            params: Parameter(s) that caused the error (if applicable)
            detail: Extended explanation of the error
            metadata: Additional context-specific information
        """
        self.message = message
        self.error_type = error_type
        # Convert enum to string value
        self.error_code = error_code.value if isinstance(error_code, ErrorCode) else error_code
        self.status_code = status_code
        self.params = params
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
        if self.params:
            error_dict["error"]["params"] = self.params
        if self.detail:
            error_dict["error"]["detail"] = self.detail
        if self.metadata:
            error_dict["error"]["metadata"] = self.metadata

        return error_dict


class ValidationError(FAOAPIError):
    """Raised when input parameters fail validation."""

    def __init__(
        self,
        message: str,
        error_code: str,
        params: str | list[str],
        detail: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            message=message,
            error_type="validation_error",
            error_code=error_code,
            status_code=400,
            params=params,
            detail=detail,
            metadata=metadata,
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
        detail: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        if metadata is None:
            metadata = {}

        if reset_time:
            metadata["reset_time"] = reset_time.isoformat() + "Z"
        if limit:
            metadata["limit"] = limit
        if period:
            metadata["period"] = period

        # Generate message from template if not provided
        if message is None:
            message = get_error_message(error_code, limit=limit, period=period or "hour")

        super().__init__(
            message=message,
            error_type="rate_limit_error",
            error_code=error_code,
            status_code=429,
            detail=detail,
            metadata=metadata,
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

    def __init__(self, service: str, message: Optional[str] = None, detail: Optional[str] = None):
        if message is None:
            message = get_error_message(ErrorCode.EXTERNAL_SERVICE_UNAVAILABLE, service=service)

        super().__init__(
            message=message,
            error_type="external_service_error",
            error_code=ErrorCode.EXTERNAL_SERVICE_UNAVAILABLE,
            status_code=503,
            detail=detail,
            metadata={"service": service},
        )


class DataQualityError(FAOAPIError):
    """Raised when data quality issues prevent processing."""

    def __init__(
        self,
        message: str,
        error_code: ErrorCode = ErrorCode.DATA_QUALITY_THRESHOLD,
        quality_flags: Optional[List[str]] = None,
        detail: Optional[str] = None,
    ):
        metadata = {}
        if quality_flags:
            metadata["quality_flags"] = quality_flags

        super().__init__(
            message=message,
            error_type="data_quality_error",
            error_code=error_code,
            status_code=422,
            detail=detail,
            metadata=metadata,
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

    def __init__(
        self,
        operation: str,
        key: Optional[str] = None,
        message: Optional[str] = None,
        detail: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):

        if message is None:
            message = get_error_message(ErrorCode.CACHE_OPERATION_FAILED, operation=operation)
            if key:
                message += f" for key: {key[:50]}"  # Truncate long keys

        if metadata is None:
            metadata = {}

        metadata["operation"] = operation
        if key:
            metadata["key"] = key[:50]

        super().__init__(
            message=message,
            error_code=ErrorCode.CACHE_OPERATION_FAILED,
            detail=detail,
            metadata=metadata,
        )


class CacheSerializationError(CacheError):
    """Raised when cache data serialization/deserialization fails."""

    def __init__(
        self,
        action: str = "serialize",  # "serialize" or "deserialize"
        data_type: Optional[str] = None,
        message: Optional[str] = None,
        detail: Optional[str] = None,
    ):
        if message is None:
            message = get_error_message(ErrorCode.CACHE_SERIALIZATION_ERROR, action=action)
            if data_type:
                message += f" for type: {data_type}"

        super().__init__(
            message=message,
            error_code=ErrorCode.CACHE_SERIALIZATION_ERROR,
            detail=detail,
            metadata={"action": action, "data_type": data_type},
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


def invalid_parameter(params: str, value: Any, reason: str) -> ValidationError:
    """Create a validation error for invalid parameter."""
    return ValidationError(
        message=get_error_message(ErrorCode.INVALID_PARAMETER, params=params, reason=reason),
        error_code=ErrorCode.INVALID_PARAMETER,
        params=params,
        detail=f"Received value: {value}",
    )


def missing_parameter(params: str) -> ValidationError:
    """Create a validation error for missing required parameter."""
    return ValidationError(
        message=get_error_message(ErrorCode.MISSING_REQUIRED_PARAMETER, params=params),
        error_code=ErrorCode.MISSING_REQUIRED_PARAMETER,
        params=params,
    )


def no_data_found(dataset: str, filters: Dict[str, Any]) -> DataNotFoundError:
    """Create a data not found error with context."""
    filter_str = ", ".join([f"{k}={v}" for k, v in filters.items()])
    return DataNotFoundError(
        message=f"No data found in {dataset} for filters: {filter_str}",
        error_code=ErrorCode.NO_DATA_FOR_QUERY,
        metadata={"dataset": dataset, "filters": filters},
    )


def incompatible_parameters(params: List[str], values: List[Any], reason: str = "") -> ValidationError:
    """Create an error for incompatible parameter combination."""
    message = get_error_message(ErrorCode.INCOMPATIBLE_PARAMETERS, param1=params[0], param2=params[1])
    if reason:
        message += f": {reason}"

    return ValidationError(
        message=message,
        error_code=ErrorCode.INCOMPATIBLE_PARAMETERS,
        params=params,
        metadata={"params": params, "values": values},
    )


def invalid_area_code(value: str) -> ValidationError:
    """Create an error for invalid area code."""
    return ValidationError(
        message=get_error_message(ErrorCode.INVALID_AREA_CODE, value=value),
        error_code=ErrorCode.INVALID_AREA_CODE,
        params="area_code",
        detail=f"Received: '{value}'. Use /area_codes endpoint to see valid codes.",
    )


def invalid_reporter_country_code(value: str) -> ValidationError:
    """Create an error for invalid reporter country code."""
    return ValidationError(
        message=get_error_message(ErrorCode.INVALID_REPORTER_COUNTRY_CODE, value=value),
        error_code=ErrorCode.INVALID_REPORTER_COUNTRY_CODE,
        params="reporter_country_code",
        detail=f"Received: '{value}'. Use /reporter_country_codes endpoint to see valid codes.",
    )


def invalid_partner_country_code(value: str) -> ValidationError:
    """Create an error for invalid partner country code."""
    return ValidationError(
        message=get_error_message(ErrorCode.INVALID_PARTNER_COUNTRY_CODE, value=value),
        error_code=ErrorCode.INVALID_PARTNER_COUNTRY_CODE,
        params="partner_country_code",
        detail=f"Received: '{value}'. Use /partner_country_codes endpoint to see valid codes.",
    )


def invalid_recipient_country_code(value: str) -> ValidationError:
    """Create an error for invalid recipient country code."""
    return ValidationError(
        message=get_error_message(ErrorCode.INVALID_RECIPIENT_COUNTRY_CODE, value=value),
        error_code=ErrorCode.INVALID_RECIPIENT_COUNTRY_CODE,
        params="recipient_country_code",
        detail=f"Received: '{value}'. Use /recipient_country_codes endpoint to see valid codes.",
    )


def invalid_item_code(value: str) -> ValidationError:
    """Create an error for invalid item code."""
    return ValidationError(
        message=get_error_message(ErrorCode.INVALID_ITEM_CODE, value=value),
        error_code=ErrorCode.INVALID_ITEM_CODE,
        params="item_code",
        detail=f"Received: '{value}'. Use /item_codes endpoint to see valid codes.",
    )


def invalid_element_code(value: str) -> ValidationError:
    """Create an error for invalid element code."""
    return ValidationError(
        message=get_error_message(ErrorCode.INVALID_ELEMENT_CODE, value=value),
        error_code=ErrorCode.INVALID_ELEMENT_CODE,
        params="element_code",
        detail=f"Received: '{value}'. Use /elements endpoint to see valid codes.",
    )


def invalid_flag(value: str) -> ValidationError:
    """Create an error for invalid flag."""
    return ValidationError(
        message=get_error_message(ErrorCode.INVALID_FLAG, value=value),
        error_code=ErrorCode.INVALID_FLAG,
        params="flag",
        detail=f"Received: '{value}'. Use /flags endpoint to see valid codes.",
    )


def invalid_iso_currency_code(value: str) -> ValidationError:
    """Create an error for invalid iso currency code."""
    return ValidationError(
        message=get_error_message(ErrorCode.INVALID_ISO_CURRENCY_CODE, value=value),
        error_code=ErrorCode.INVALID_ISO_CURRENCY_CODE,
        params="iso_currency_code",
        detail=f"Received: '{value}'. Use /currencies endpoint to see valid codes.",
    )


def invalid_source_code(value: str) -> ValidationError:
    """Create an error for invalid source code."""
    return ValidationError(
        message=get_error_message(ErrorCode.INVALID_SOURCE_CODE, value=value),
        error_code=ErrorCode.INVALID_SOURCE_CODE,
        params="source_code",
        detail=f"Received: '{value}'. Use /sources endpoint to see valid codes.",
    )


def invalid_release_code(value: str) -> ValidationError:
    """Create an error for invalid release code."""
    return ValidationError(
        message=get_error_message(ErrorCode.INVALID_RELEASE_CODE, value=value),
        error_code=ErrorCode.INVALID_RELEASE_CODE,
        params="release_code",
        detail=f"Received: '{value}'. Use /releases endpoint to see valid codes.",
    )


def invalid_sex_code(value: str) -> ValidationError:
    """Create an error for invalid sex code."""
    return ValidationError(
        message=get_error_message(ErrorCode.INVALID_SEX_CODE, value=value),
        error_code=ErrorCode.INVALID_SEX_CODE,
        params="sex_code",
        detail=f"Received: '{value}'. Use /sexs endpoint to see valid codes.",
    )


def invalid_indicator_code(value: str) -> ValidationError:
    """Create an error for invalid indicator code."""
    return ValidationError(
        message=get_error_message(ErrorCode.INVALID_INDICATOR_CODE, value=value),
        error_code=ErrorCode.INVALID_INDICATOR_CODE,
        params="indicator_code",
        detail=f"Received: '{value}'. Use /indicators endpoint to see valid codes.",
    )


def invalid_population_age_group_code(value: str) -> ValidationError:
    """Create an error for invalid population age group code."""
    return ValidationError(
        message=get_error_message(ErrorCode.INVALID_POPULATION_AGE_GROUP_CODE, value=value),
        error_code=ErrorCode.INVALID_POPULATION_AGE_GROUP_CODE,
        params="population_age_group_code",
        detail=f"Received: '{value}'. Use /population_age_groups endpoint to see valid codes.",
    )


def invalid_survey_code(value: str) -> ValidationError:
    """Create an error for invalid survey code."""
    return ValidationError(
        message=get_error_message(ErrorCode.INVALID_SURVEY_CODE, value=value),
        error_code=ErrorCode.INVALID_SURVEY_CODE,
        params="survey_code",
        detail=f"Received: '{value}'. Use /surveys endpoint to see valid codes.",
    )


def invalid_purpose_code(value: str) -> ValidationError:
    """Create an error for invalid purpose code."""
    return ValidationError(
        message=get_error_message(ErrorCode.INVALID_PURPOSE_CODE, value=value),
        error_code=ErrorCode.INVALID_PURPOSE_CODE,
        params="purpose_code",
        detail=f"Received: '{value}'. Use /purposes endpoint to see valid codes.",
    )


def invalid_donor_code(value: str) -> ValidationError:
    """Create an error for invalid donor code."""
    return ValidationError(
        message=get_error_message(ErrorCode.INVALID_DONOR_CODE, value=value),
        error_code=ErrorCode.INVALID_DONOR_CODE,
        params="donor_code",
        detail=f"Received: '{value}'. Use /donors endpoint to see valid codes.",
    )


def invalid_food_group_code(value: str) -> ValidationError:
    """Create an error for invalid food group code."""
    return ValidationError(
        message=get_error_message(ErrorCode.INVALID_FOOD_GROUP_CODE, value=value),
        error_code=ErrorCode.INVALID_FOOD_GROUP_CODE,
        params="food_group_code",
        detail=f"Received: '{value}'. Use /food_groups endpoint to see valid codes.",
    )


def invalid_geographic_level_code(value: str) -> ValidationError:
    """Create an error for invalid geographic level code."""
    return ValidationError(
        message=get_error_message(ErrorCode.INVALID_GEOGRAPHIC_LEVEL_CODE, value=value),
        error_code=ErrorCode.INVALID_GEOGRAPHIC_LEVEL_CODE,
        params="geographic_level_code",
        detail=f"Received: '{value}'. Use /geographic_levels endpoint to see valid codes.",
    )


def invalid_food_value_code(value: str) -> ValidationError:
    """Create an error for invalid food value code."""
    return ValidationError(
        message=get_error_message(ErrorCode.INVALID_FOOD_VALUE_CODE, value=value),
        error_code=ErrorCode.INVALID_FOOD_VALUE_CODE,
        params="food_value_code",
        detail=f"Received: '{value}'. Use /food_values endpoint to see valid codes.",
    )


def invalid_industry_code(value: str) -> ValidationError:
    """Create an error for invalid industry code."""
    return ValidationError(
        message=get_error_message(ErrorCode.INVALID_INDUSTRY_CODE, value=value),
        error_code=ErrorCode.INVALID_INDUSTRY_CODE,
        params="industry_code",
        detail=f"Received: '{value}'. Use /industries endpoint to see valid codes.",
    )


def invalid_factor_code(value: str) -> ValidationError:
    """Create an error for invalid factor code."""
    return ValidationError(
        message=get_error_message(ErrorCode.INVALID_FACTOR_CODE, value=value),
        error_code=ErrorCode.INVALID_FACTOR_CODE,
        params="factor_code",
        detail=f"Received: '{value}'. Use /factors endpoint to see valid codes.",
    )


def invalid_range(params: List[str], values: List[int]) -> ValidationError:
    """Create an error for value outside valid range."""
    return ValidationError(
        message=get_error_message(ErrorCode.INVALID_RANGE, value=f"{params}: {values}"),
        error_code=ErrorCode.INVALID_RANGE,
        params=params,
        detail=f"minimum value must be less than maximum value",
    )
