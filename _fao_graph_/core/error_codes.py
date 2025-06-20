# fao/src/core/error_codes.py
"""
FAO API Error Code Constants

Centralized definition of all error codes used throughout the API.
Using enums ensures consistency and prevents typos.
"""
from enum import Enum
from typing import Dict


class ErrorCode(str, Enum):
    """
    Enumeration of all FAO API error codes.

    Organized by category for easier maintenance.
    """

    # Validation Errors (400)
    INVALID_PARAMETER = "INVALID_PARAMETER"
    INVALID_AREA_CODE = "INVALID_AREA_CODE"
    INVALID_ITEM_CODE = "INVALID_ITEM_CODE"
    INVALID_YEAR_RANGE = "INVALID_YEAR_RANGE"
    INVALID_ELEMENT_CODE = "INVALID_ELEMENT_CODE"
    INVALID_FLAG_CODE = "INVALID_FLAG_CODE"
    INVALID_DONOR_CODE = "INVALID_DONOR_CODE"
    INVALID_SOURCE_CODE = "INVALID_SOURCE_CODE"
    INVALID_PURPOSE_CODE = "INVALID_PURPOSE_CODE"
    INVALID_SEX_CODE = "INVALID_SEX_CODE"
    INVALID_RELEASE_CODE = "INVALID_RELEASE_CODE"
    INVALID_REPORTER_COUNTRY_CODE = "INVALID_REPORTER_COUNTRY_CODE"
    INVALID_PARTNER_COUNTRY_CODE = "INVALID_PARTNER_COUNTRY_CODE"
    INVALID_RECIPIENT_COUNTRY_CODE = "INVALID_RECIPIENT_COUNTRY_CODE"
    INVALID_CURRENCY_CODE = "INVALID_CURRENCY_CODE"
    INVALID_SURVEY_CODE = "INVALID_SURVEY_CODE"
    INVALID_POPULATION_AGE_GROUP_CODE = "INVALID_POPULATION_AGE_GROUP_CODE"
    INVALID_INDICATOR_CODE = "INVALID_INDICATOR_CODE"
    INVALID_FOOD_GROUP_CODE = "INVALID_FOOD_GROUP_CODE"
    INVALID_GEOGRAPHIC_LEVEL_CODE = "INVALID_GEOGRAPHIC_LEVEL_CODE"
    INVALID_DATE_FORMAT = "INVALID_DATE_FORMAT"
    INVALID_LIMIT = "INVALID_LIMIT"
    INVALID_OFFSET = "INVALID_OFFSET"
    MISSING_REQUIRED_PARAMETER = "MISSING_REQUIRED_PARAMETER"
    INCOMPATIBLE_PARAMETERS = "INCOMPATIBLE_PARAMETERS"
    PARAMETER_OUT_OF_RANGE = "PARAMETER_OUT_OF_RANGE"
    TOO_MANY_VALUES = "TOO_MANY_VALUES"

    # Data Availability Errors (404)
    NO_DATA_FOR_QUERY = "NO_DATA_FOR_QUERY"
    DATASET_NOT_FOUND = "DATASET_NOT_FOUND"
    RESOURCE_NOT_FOUND = "RESOURCE_NOT_FOUND"
    NO_DATA_FOR_PERIOD = "NO_DATA_FOR_PERIOD"
    NO_DATA_FOR_COMBINATION = "NO_DATA_FOR_COMBINATION"

    # Business Logic Errors (422)
    INCOMPATIBLE_AGGREGATION = "INCOMPATIBLE_AGGREGATION"
    INSUFFICIENT_DATA_POINTS = "INSUFFICIENT_DATA_POINTS"
    TIME_SERIES_GAP = "TIME_SERIES_GAP"
    CALCULATION_ERROR = "CALCULATION_ERROR"
    UNSUPPORTED_OPERATION = "UNSUPPORTED_OPERATION"
    DATA_QUALITY_THRESHOLD = "DATA_QUALITY_THRESHOLD"
    CONFLICTING_UNITS = "CONFLICTING_UNITS"

    # Authentication Errors (401)
    AUTHENTICATION_REQUIRED = "AUTHENTICATION_REQUIRED"
    INVALID_API_KEY = "INVALID_API_KEY"
    EXPIRED_API_KEY = "EXPIRED_API_KEY"
    INVALID_TOKEN = "INVALID_TOKEN"
    EXPIRED_TOKEN = "EXPIRED_TOKEN"

    # Authorization Errors (403)
    INSUFFICIENT_PERMISSIONS = "INSUFFICIENT_PERMISSIONS"
    TIER_LIMIT_EXCEEDED = "TIER_LIMIT_EXCEEDED"
    RESOURCE_ACCESS_DENIED = "RESOURCE_ACCESS_DENIED"
    OPERATION_NOT_ALLOWED = "OPERATION_NOT_ALLOWED"

    # Rate Limiting Errors (429)
    RATE_LIMIT_EXCEEDED = "RATE_LIMIT_EXCEEDED"
    DAILY_QUOTA_EXCEEDED = "DAILY_QUOTA_EXCEEDED"
    CONCURRENT_REQUEST_LIMIT = "CONCURRENT_REQUEST_LIMIT"

    # Server Errors (500)
    INTERNAL_ERROR = "INTERNAL_ERROR"
    DATABASE_ERROR = "DATABASE_ERROR"
    CONFIGURATION_ERROR = "CONFIGURATION_ERROR"
    UNEXPECTED_ERROR = "UNEXPECTED_ERROR"

    # Service Availability Errors (503)
    SERVICE_UNAVAILABLE = "SERVICE_UNAVAILABLE"
    DATABASE_UNAVAILABLE = "DATABASE_UNAVAILABLE"
    EXTERNAL_SERVICE_UNAVAILABLE = "EXTERNAL_SERVICE_UNAVAILABLE"
    MAINTENANCE_MODE = "MAINTENANCE_MODE"
    # Cache errors
    CACHE_ERROR = "CACHE_ERROR"
    CACHE_CONNECTION_FAILED = "CACHE_CONNECTION_FAILED"
    CACHE_OPERATION_FAILED = "CACHE_OPERATION_FAILED"
    CACHE_SERIALIZATION_ERROR = "CACHE_SERIALIZATION_ERROR"


# Human-readable error messages with placeholders
ERROR_MESSAGES: Dict[ErrorCode, str] = {
    # Validation errors
    ErrorCode.INVALID_PARAMETER: "Invalid value for parameter '{param}': {reason}",
    ErrorCode.INVALID_AREA_CODE: "Invalid area code '{value}'. Must be a valid ISO3 country code or FAO area code. Use /other/area_codes endpoint to see valid codes.",
    ErrorCode.INVALID_ITEM_CODE: "Invalid item code '{value}'. Use /other/item_codes endpoint to see valid codes.",
    ErrorCode.INVALID_YEAR_RANGE: "Year {value} is outside valid range (1961-2024)",
    ErrorCode.INVALID_ELEMENT_CODE: "Invalid element code '{value}'. Use /other/elements endpoint to see valid codes.",
    ErrorCode.INVALID_FLAG_CODE: "Invalid flag code '{value}'. Use /other/flags endpoint to see valid codes",
    ErrorCode.INVALID_DONOR_CODE: "Invalid donor code '{value}'. Use /other/donors endpoint to see valid codes.",
    ErrorCode.INVALID_SOURCE_CODE: "Invalid source code '{value}'. Use /other/sources endpoint to see valid codes.",
    ErrorCode.INVALID_PURPOSE_CODE: "Invalid purpose code '{value}'. Use /other/purposes endpoint to see valid codes.",
    ErrorCode.INVALID_SEX_CODE: "Invalid sex code '{value}'. Use /other/sexs endpoint to see valid codes.",
    ErrorCode.INVALID_RELEASE_CODE: "Invalid release code '{value}'. Use /other/releases endpoint to see valid codes.",
    ErrorCode.INVALID_REPORTER_COUNTRY_CODE: "Invalid reporter country code '{value}'. Use /other/reporter_country_codes endpoint to see valid codes.",
    ErrorCode.INVALID_PARTNER_COUNTRY_CODE: "Invalid partner country code '{value}'. Use /other/partner_country_codes endpoint to see valid codes.",
    ErrorCode.INVALID_RECIPIENT_COUNTRY_CODE: "Invalid recipient country code '{value}'. Use /other/recipient_country_codes endpoint to see valid codes.",
    ErrorCode.INVALID_CURRENCY_CODE: "Invalid currency code '{value}'. Use /other/currencies endpoint to see valid codes.",
    ErrorCode.INVALID_SURVEY_CODE: "Invalid survey code '{value}'. Use /other/surveys endpoint to see valid codes.",
    ErrorCode.INVALID_POPULATION_AGE_GROUP_CODE: "Invalid population age group code '{value}'. Use /population/population_age_groups endpoint to see valid codes.",
    ErrorCode.INVALID_INDICATOR_CODE: "Invalid indicator code '{value}'. Use /indicators/indicators endpoint to see valid codes.",
    ErrorCode.INVALID_FOOD_GROUP_CODE: "Invalid food group code '{value}'. Use /food/food_groups endpoint to see valid codes.",
    ErrorCode.INVALID_GEOGRAPHIC_LEVEL_CODE: "Invalid geographic level code '{value}'. Use /other/geographic_levels endpoint to see valid codes.",
    ErrorCode.INVALID_DATE_FORMAT: "Invalid date format. Expected: {expected_format}",
    ErrorCode.INVALID_LIMIT: "Limit must be between 1 and {max_limit}. Received: {value}",
    ErrorCode.INVALID_OFFSET: "Offset must be non-negative. Received: {value}",
    ErrorCode.MISSING_REQUIRED_PARAMETER: "Missing required parameter: '{param}'",
    ErrorCode.INCOMPATIBLE_PARAMETERS: "Parameters '{param1}' and '{param2}' cannot be used together",
    ErrorCode.PARAMETER_OUT_OF_RANGE: "Parameter '{param}' value {value} is outside allowed range [{min}, {max}]",
    ErrorCode.TOO_MANY_VALUES: "Too many values for parameter '{param}'. Maximum allowed: {max}",
    # Data availability errors
    ErrorCode.NO_DATA_FOR_QUERY: "No data found matching your query parameters",
    ErrorCode.DATASET_NOT_FOUND: "Dataset '{dataset}' not found",
    ErrorCode.RESOURCE_NOT_FOUND: "The requested resource does not exist",
    ErrorCode.NO_DATA_FOR_PERIOD: "No data available for the specified time period",
    ErrorCode.NO_DATA_FOR_COMBINATION: "No data available for {item} in {area}",
    # Business logic errors
    ErrorCode.INCOMPATIBLE_AGGREGATION: "Cannot aggregate {element} across different units",
    ErrorCode.INSUFFICIENT_DATA_POINTS: "Insufficient data points for {operation}. Minimum required: {required}",
    ErrorCode.TIME_SERIES_GAP: "Time series contains gaps that prevent {operation}",
    ErrorCode.CALCULATION_ERROR: "Unable to perform calculation: {reason}",
    ErrorCode.UNSUPPORTED_OPERATION: "Operation '{operation}' is not supported for this dataset",
    ErrorCode.DATA_QUALITY_THRESHOLD: "Data quality below threshold ({quality}%) for reliable analysis",
    ErrorCode.CONFLICTING_UNITS: "Cannot combine data with different units: {units}",
    # Authentication errors
    ErrorCode.AUTHENTICATION_REQUIRED: "Authentication required. Please provide a valid API key.",
    ErrorCode.INVALID_API_KEY: "The provided API key is invalid",
    ErrorCode.EXPIRED_API_KEY: "Your API key has expired. Please renew at https://api.fao.org/account",
    ErrorCode.INVALID_TOKEN: "Invalid authentication token",
    ErrorCode.EXPIRED_TOKEN: "Authentication token has expired",
    # Authorization errors
    ErrorCode.INSUFFICIENT_PERMISSIONS: "Your API key lacks permission for this operation",
    ErrorCode.TIER_LIMIT_EXCEEDED: "This endpoint requires {required_tier} tier access. Your tier: {current_tier}",
    ErrorCode.RESOURCE_ACCESS_DENIED: "Access denied to resource '{resource}'",
    ErrorCode.OPERATION_NOT_ALLOWED: "Operation '{operation}' is not allowed for your account",
    # Rate limiting errors
    ErrorCode.RATE_LIMIT_EXCEEDED: "Rate limit exceeded. Limit: {limit} requests per {period}",
    ErrorCode.DAILY_QUOTA_EXCEEDED: "Daily request quota exceeded. Resets at: {reset_time}",
    ErrorCode.CONCURRENT_REQUEST_LIMIT: "Too many concurrent requests. Maximum allowed: {max}",
    # Server errors
    ErrorCode.INTERNAL_ERROR: "An unexpected error occurred. Please try again later.",
    ErrorCode.DATABASE_ERROR: "Database operation failed. Please try again later.",
    ErrorCode.CONFIGURATION_ERROR: "System configuration error. Please contact support.",
    ErrorCode.UNEXPECTED_ERROR: "An unexpected error occurred. Request ID: {request_id}",
    # Service availability errors
    ErrorCode.SERVICE_UNAVAILABLE: "Service temporarily unavailable. Please try again later.",
    ErrorCode.DATABASE_UNAVAILABLE: "Database service is temporarily unavailable",
    ErrorCode.EXTERNAL_SERVICE_UNAVAILABLE: "External service '{service}' is temporarily unavailable",
    ErrorCode.MAINTENANCE_MODE: "API is under maintenance. Expected completion: {end_time}",
    # Cache errors
    ErrorCode.CACHE_ERROR: "Cache service error occurred",
    ErrorCode.CACHE_CONNECTION_FAILED: "Failed to connect to cache service: {service}",
    ErrorCode.CACHE_OPERATION_FAILED: "Cache {operation} operation failed",
    ErrorCode.CACHE_SERIALIZATION_ERROR: "Failed to {action} cache data",
}


def get_error_message(code: ErrorCode, **kwargs) -> str:
    """
    Get formatted error message for a given error code.

    Args:
        code: Error code enum value
        **kwargs: Values to format into the message template

    Returns:
        Formatted error message

    Example:
        >>> get_error_message(ErrorCode.INVALID_YEAR_RANGE, value=2025)
        "Year 2025 is outside valid range (1961-2024)"
    """
    template = ERROR_MESSAGES.get(code, "An error occurred")
    try:
        return template.format(**kwargs)
    except KeyError:
        # If formatting fails, return template as-is
        return template
