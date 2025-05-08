import asyncio
import functools
import hashlib
import inspect
import logging
import os
import pickle
from typing import Any, Callable, Coroutine, Optional, TypeVar

from redis.exceptions import RedisError

# Import the new connection helper
from src.cache.connection import get_redis, close_redis as close_redis_pool # Alias for clarity if needed

# Define namespace constant
NAMESPACE = "rcpe:"

logger = logging.getLogger(__name__)

# NAMESPACE_PREFIX is removed, use NAMESPACE internally

R = TypeVar('R')

def redis_cache(ttl: int = 3600, key_prefix: str = "") -> Callable[[Callable[..., Coroutine[Any, Any, R]]], Callable[..., Coroutine[Any, Any, R]]]:
    """
    Asynchronous caching decorator using Redis.

    Uses a lazy connection pool managed by src.cache.connection.

    Args:
        ttl: Time-to-live for the cache entry in seconds. If <= 0, caching is skipped.
        key_prefix: Optional prefix to add to the cache key namespace.
    """
    def decorator(func: Callable[..., Coroutine[Any, Any, R]]) -> Callable[..., Coroutine[Any, Any, R]]:
        if not inspect.iscoroutinefunction(func):
            raise TypeError("The decorated function must be an async function.")

        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> R:
            if ttl <= 0:
                logger.debug(f"TTL <= 0, skipping cache for {func.__qualname__}")
                return await func(*args, **kwargs)

            # fetch the live helper each call, so early‑decorated functions still work
            from src.cache import connection as _conn
            redis_conn = await _conn.get_redis()
            if not redis_conn:
                logger.warning(f"Redis unavailable, executing live function {func.__qualname__}")
                return await func(*args, **kwargs)

            # Generate cache key
            try:
                # Ensure args/kwargs are hashable/picklable before creating key
                arg_key_part = pickle.dumps((args, kwargs), protocol=5)
            except (pickle.PicklingError, TypeError) as e:
                 logger.warning(f"Failed to pickle arguments for {func.__qualname__}: {e}. Cannot generate cache key, executing live function.")
                 return await func(*args, **kwargs)

            hashed_args = hashlib.sha1(arg_key_part).hexdigest()
            # Ensure NAMESPACE is always prepended
            cache_key = (
                f"{NAMESPACE}{key_prefix}"
                f"{func.__module__}.{func.__qualname__}:{hashed_args}" # Use module and qualname as required
            )

            logger.debug(f"[{func.__qualname__}] Generated cache key: {cache_key}") # Use qualname in log
            try:
                # Check cache with timeout
                logger.debug(f"[{func.__qualname__}] Attempting GET for key: {cache_key}") # ADDED LOGGING
                cached_result = await asyncio.wait_for(redis_conn.get(cache_key), timeout=2.0)
                logger.debug(f"[{func.__qualname__}] GET result for key {cache_key}: {'HIT (data found)' if cached_result is not None else 'MISS (no data)'}") # ADDED LOGGING
                if cached_result is not None:
                    logger.debug(f"Cache hit for {func.__qualname__} with key {cache_key}") # Existing log
                    try:
                        return pickle.loads(cached_result) # type: ignore
                    except (pickle.UnpicklingError, TypeError, EOFError) as e:
                        logger.warning(f"Failed to unpickle cached data for key {cache_key}: {e}. Fetching live data.")
                        # Optionally delete the corrupted key: await redis_conn.unlink(cache_key) # Use unlink

                # Execute function if cache miss
                logger.debug(f"Cache miss for {func.__qualname__} with key {cache_key}")
                result = await func(*args, **kwargs)

                # Store result in cache
                try:
                    serialized_result = pickle.dumps(result, protocol=5)
                    # Use SETEX with timeout to enforce TTL
                    logger.debug(f"[{func.__qualname__}] Attempting SETEX for key: {cache_key} with TTL: {ttl}") # ADDED LOGGING
                    await asyncio.wait_for(redis_conn.setex(cache_key, ttl, serialized_result), timeout=2.0)
                    logger.debug(f"Stored result for {func.__qualname__} in cache with key {cache_key}, TTL={ttl}") # Existing log
                except (pickle.PicklingError, TypeError) as e:
                     logger.warning(f"Failed to pickle result for {func.__qualname__} key {cache_key}: {e}. Result not cached.")
                except RedisError as e:
                    logger.warning(f"Redis error during SET for key {cache_key}: {e}. Result not cached.")

                return result

            except asyncio.TimeoutError:
                logger.warning(f"Redis operation timed out for {func.__qualname__}. Executing live function.")
                # Fallback to live execution on timeout
                return await func(*args, **kwargs)
            except RedisError as e:
                logger.warning(f"Redis error during cache lookup/set for {func.__qualname__}: {e}. Executing live function.")
                # Attempt to return live result even if cache interaction fails
                try:
                    return await func(*args, **kwargs)
                except Exception as live_exc:
                    logger.error(f"Failed to execute live function {func.__qualname__} after Redis error: {live_exc}", exc_info=True)
                    raise # Re-raise the exception from the live function call
            except Exception as e:
                logger.error(f"Unexpected error in cache wrapper for {func.__qualname__}: {e}", exc_info=True)
                # Fallback to live execution on unexpected errors
                return await func(*args, **kwargs)

        return wrapper
    return decorator


# --- Invalidation Helper ---

async def _unlink_all(redis_conn, match_pattern: str) -> int:
    """Helper to scan and unlink keys matching a pattern."""
    deleted_count = 0
    logger.debug(f"[_unlink_all] Scanning with pattern: {match_pattern}")
    try:
        async for key in redis_conn.scan_iter(match=match_pattern, count=100): # count is a hint
            try:
                # UNLINK returns number of keys unlinked (0 or 1 here) - add timeout
                delete_result = await asyncio.wait_for(redis_conn.unlink(key), timeout=2.0)
                if delete_result > 0:
                    deleted_count += delete_result
                    logger.debug(f"[_unlink_all] Unlinked key: {key.decode(errors='ignore')}")
            except asyncio.TimeoutError:
                logger.warning(f"[_unlink_all] Redis unlink operation timed out for key {key.decode(errors='ignore')}. Stopping scan.")
                break # Stop processing on timeout
            except RedisError as e:
                 logger.error(f"[_unlink_all] Redis error during unlink for key {key.decode(errors='ignore')}: {e}. Stopping scan.")
                 break # Stop processing on error
    except RedisError as e:
        logger.error(f"[_unlink_all] Redis error during SCAN for pattern '{match_pattern}': {e}")
    except Exception as e:
        logger.error(f"[_unlink_all] Unexpected error during scan/unlink for pattern '{match_pattern}': {e}", exc_info=True)
    logger.debug(f"[_unlink_all] Finished scan for {match_pattern}. Unlinked {deleted_count} keys.")
    return deleted_count


# --- Public Invalidation Function ---

async def invalidate(pattern: str) -> None: # Changed return type annotation to None
    """
    Invalidates (deletes) cache keys matching the given pattern. Returns None.

    Uses SCAN and UNLINK for non-blocking deletion.

    Args:
        pattern: The pattern to match keys against (e.g., "module.Class.*").
                 The namespace prefix (NAMESPACE constant) will be added automatically.

    Returns:
        None. The number of deleted keys is logged internally.
    """
    # Get connection lazily
    redis_conn = await get_redis()
    if not redis_conn:
        logger.warning("Redis unavailable, cannot invalidate cache.")
        return 0

    # Construct the full pattern including namespace and wildcard
    # Ensure the user-provided pattern doesn't already include the namespace
    clean_pattern = pattern.removeprefix(NAMESPACE)
    full_pattern = f"{NAMESPACE}{clean_pattern}"
    if not full_pattern.endswith('*'):
        full_pattern += '*'

    logger.info(f"Starting cache invalidation for pattern: {full_pattern}")

    # Call the helper function
    deleted_count = await _unlink_all(redis_conn, full_pattern)

    logger.info(f"Invalidation complete for pattern '{full_pattern}'. Deleted {deleted_count} keys.")
    # No return value as per feedback

# Optional: Function to explicitly close the connection pool if needed during shutdown
# This now calls the helper module's close function.
async def close_redis_connection():
    """Closes the shared Redis connection pool."""
    await close_redis_pool()