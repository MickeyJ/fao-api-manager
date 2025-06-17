# src/core/cache.py
"""
Simple caching module for FAO API
Caches endpoint results in Redis with automatic fallback if Redis is unavailable

Note: Some type: ignore comments are needed due to redis-py type stubs
sometimes confusing sync and async clients in type checkers.

Requirements:
- redis>=4.0.0,<6.0.0 (install with: pip install redis==5.0.1)
"""
# Standard library
import asyncio
import hashlib
import json
import pickle
from functools import wraps
from typing import Any, Dict, List, Union

# Third-party
import redis
from redis import Redis
from _fao_.src.core import settings
from _fao_.logger import logger
from _fao_.src.core.exceptions import (
    CacheOperationError,
    cache_connection_failed,
    cache_read_failed,
    cache_write_failed,
    cache_delete_failed,
    cache_serialization_failed,
    cache_deserialization_failed,
)

# Global Redis client
_redis_client: Redis | None = None


def get_redis_client() -> Redis | None:
    """
    Get or create Redis client
    Returns None if Redis is disabled or unavailable
    """
    global _redis_client

    # Check if caching is enabled
    if not getattr(settings, "cache_enabled", True):
        logger.warning("Caching is disabled, skipping Redis client creation")
        return None

    # Try to create client if not exists
    if _redis_client is None:
        try:
            # Check if we're using Upstash (requires SSL)
            is_upstash = "upstash.io" in settings.redis_host.lower()

            _redis_client = Redis(
                host=settings.redis_host,
                port=settings.redis_port,
                password=settings.redis_password,
                decode_responses=False,  # We'll handle encoding/decoding
                socket_connect_timeout=5,
                socket_timeout=5,
                db=0,
                ssl=is_upstash,  # Enable SSL for Upstash
                ssl_cert_reqs="none" if is_upstash else "required",  # Upstash doesn't require cert validation
            )
            # Test connection
            _redis_client.ping()
            logger.success(f"Redis connected successfully at {settings.redis_host}:{settings.redis_port}")
        except (redis.ConnectionError, redis.TimeoutError) as e:
            # Log the proper exception but don't raise it
            exc = cache_connection_failed(error=e)
            logger.error(f"Cache connection failed: {exc.message} - {exc.detail}")
            _redis_client = None

    return _redis_client


def generate_cache_key(prefix: str, *, params: dict, exclude_params: List[str] | None = None) -> str:
    """
    Generate consistent cache key from endpoint and parameters

    Args:
        prefix: Cache key prefix (usually the endpoint name)
        params: Query parameters dictionary
        exclude_params: List of parameter names to exclude from cache key

    Returns:
        Cache key string
    """
    if exclude_params is None:
        exclude_params = []

    # Always exclude these from cache key
    default_excludes = ["db", "response", "request"]
    exclude_params.extend(default_excludes)

    # Filter and sort parameters for consistency
    cache_params = {}
    for key, value in params.items():
        if key not in exclude_params and value is not None:
            # Convert to string for consistent hashing
            if isinstance(value, bool):
                cache_params[key] = str(value).lower()
            else:
                cache_params[key] = str(value)

    # Sort for consistency
    sorted_params = sorted(cache_params.items())

    # Create hash of parameters
    if sorted_params:
        param_str = json.dumps(sorted_params, sort_keys=True)
        param_hash = hashlib.md5(param_str.encode()).hexdigest()[:16]  # Use first 16 chars
        return (
            f"{settings.cache_prefix}{settings.cache_key_separator}{prefix}{settings.cache_key_separator}{param_hash}"
        )
    else:
        return f"{settings.cache_prefix}{settings.cache_key_separator}{prefix}{settings.cache_key_separator}default"


def cache_result(prefix: str, *, ttl: int = 3600, exclude_params: List[str] | None = None):
    """Decorator to cache endpoint results in Redis.

    Caches function results based on input parameters with automatic
    fallback if Redis is unavailable.

    Args:
        prefix: Cache key prefix (typically the endpoint/dataset name)
        ttl: Time to live in seconds (default 1 hour)
        exclude_params: Parameters to exclude from cache key

    Returns:
        Decorated function that caches results

    Example:
        >>> @cache_result(prefix="prices", ttl=86400)
        >>> def get_prices(**kwargs):
        >>>     return expensive_database_query()
    """

    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            # Get Redis client
            redis_client = get_redis_client()
            if not redis_client:
                # Redis not available, execute without caching
                return await func(*args, **kwargs)

            cache_key = None
            try:
                # Generate cache key
                cache_key = generate_cache_key(prefix, params=kwargs, exclude_params=exclude_params)

                # Try to get from cache
                cached_data = redis_client.get(cache_key)
                if cached_data:
                    # Ensure cached_data is bytes
                    if isinstance(cached_data, bytes):
                        try:
                            return pickle.loads(cached_data)
                        except (pickle.PickleError, Exception) as e:
                            exc = cache_deserialization_failed(error=e)
                            logger.error(f"Cache deserialization failed: {exc.message} - {exc.detail}")
                            # Continue to fetch fresh data

                # Not in cache, execute function
                result = await func(*args, **kwargs)

                # Cache the result
                try:
                    pickled_data = pickle.dumps(result)
                    redis_client.setex(cache_key, ttl, pickled_data)
                except (pickle.PickleError, Exception) as e:
                    exc = cache_serialization_failed(type(result), error=e)
                    logger.error(f"Cache serialization failed: {exc.message} - {exc.detail}")
                    # Still return the result, just don't cache it

                return result

            except redis.RedisError as e:
                # Log error but don't fail the request
                if cache_key and "get" in str(e).lower():
                    exc = cache_read_failed(cache_key, error=e)
                elif cache_key:
                    exc = cache_write_failed(cache_key, error=e)
                else:
                    exc = CacheOperationError(operation="unknown", message=str(e))
                logger.error(f"Cache operation failed: {exc.message} - {exc.detail}")
                return await func(*args, **kwargs)

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            # Get Redis client
            redis_client = get_redis_client()
            if not redis_client:
                # Redis not available, execute without caching
                return func(*args, **kwargs)

            cache_key = None
            try:
                # Generate cache key
                cache_key = generate_cache_key(prefix, params=kwargs, exclude_params=exclude_params)

                # Try to get from cache
                cached_data = redis_client.get(cache_key)
                if cached_data:
                    # Ensure cached_data is bytes
                    if isinstance(cached_data, bytes):
                        try:
                            return pickle.loads(cached_data)
                        except (pickle.PickleError, Exception) as e:
                            exc = cache_deserialization_failed(error=e)
                            logger.error(f"Cache deserialization failed: {exc.message} - {exc.detail}")
                            # Continue to fetch fresh data

                # Not in cache, execute function
                result = func(*args, **kwargs)

                # Cache the result
                try:
                    pickled_data = pickle.dumps(result)
                    redis_client.setex(cache_key, ttl, pickled_data)
                except (pickle.PickleError, Exception) as e:
                    exc = cache_serialization_failed(type(result), error=e)
                    logger.error(f"Cache serialization failed: {exc.message} - {exc.detail}")
                    # Still return the result, just don't cache it

                return result

            except redis.RedisError as e:
                # Log error but don't fail the request
                if cache_key and "get" in str(e).lower():
                    exc = cache_read_failed(cache_key, error=e)
                elif cache_key:
                    exc = cache_write_failed(cache_key, error=e)
                else:
                    exc = CacheOperationError(operation="unknown", message=str(e))
                logger.error(f"Cache operation failed: {exc.message} - {exc.detail}")
                return func(*args, **kwargs)

        # Return appropriate wrapper based on function type
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator


def invalidate_cache(pattern: str = "*") -> int:
    """
    Invalidate cache entries matching pattern

    Args:
        pattern: Redis key pattern (e.g., "fao:prices:*" to clear all prices cache)

    Returns:
        Number of keys deleted
    """
    redis_client = get_redis_client()
    if not redis_client:
        return 0

    try:
        deleted_count = 0
        cursor: Union[int, bytes] = 0

        # Use SCAN to find matching keys (safer than KEYS for production)
        while True:
            # scan returns tuple of (cursor, keys)
            scan_result = redis_client.scan(
                cursor, match=f"{settings.cache_prefix}{settings.cache_key_separator}{pattern}", count=100
            )  # type: ignore
            cursor = scan_result[0]
            keys = scan_result[1]

            if keys:
                deleted_count += redis_client.delete(*keys)  # type: ignore
            if cursor == 0:
                break

        return deleted_count

    except redis.RedisError as e:
        exc = cache_delete_failed(pattern, error=e)
        logger.error(f"Cache invalidation failed: {exc.message} - {exc.detail}")
        return 0


def get_cache_info() -> Dict[str, Any]:
    """Get basic cache information and statistics"""
    redis_client = get_redis_client()
    if not redis_client:
        return {"status": "disabled", "reason": "Redis not available"}

    try:
        info: Dict[str, Any] = redis_client.info()  # type: ignore
        dbsize = redis_client.dbsize()

        # Count FAO-specific keys
        fao_keys = 0
        cursor: Union[int, bytes] = 0
        while True:
            scan_result = redis_client.scan(
                cursor, match=f"{settings.cache_prefix}{settings.cache_key_separator}*", count=100
            )  # type: ignore
            cursor = scan_result[0]
            keys = scan_result[1]
            fao_keys += len(keys)
            if cursor == 0:
                break

        return {
            "status": "active",
            "total_keys": dbsize,
            "fao_keys": fao_keys,
            "memory_used": info.get("used_memory_human", "unknown"),
            "connected_clients": info.get("connected_clients", 0),
            "redis_version": info.get("redis_version", "unknown"),
        }

    except redis.RedisError as e:
        exc = CacheOperationError(operation="info", message=str(e))
        logger.error(f"Error getting cache info: {exc.message}")
        return {"status": "error", "error": str(exc.message)}
