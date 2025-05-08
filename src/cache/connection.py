import asyncio
import logging
import os
from functools import wraps
from typing import Optional

import redis.asyncio as aioredis
from redis.exceptions import RedisError, ConnectionError, TimeoutError # Ensure all needed exceptions are imported

_log = logging.getLogger(__name__)

# --- Lock-free _once decorator ---
def _once(fn):
    in_flight = None
    result = None
    @wraps(fn)
    async def wrapper():
        nonlocal in_flight, result
        if result is not None:
            # If we have a result, check if it's still valid (e.g., connected)
            # For simplicity here, we assume if it exists, it's usable,
            # relying on Redis operations to fail if the connection is truly dead.
            # A ping could be added here if needed.
            return result
        if in_flight is None:
            # No result and no task in flight, create one
            _log.debug(f"Creating task for {fn.__name__}")
            in_flight = asyncio.create_task(fn())

        try:
            # Await the task (either newly created or already in flight)
            result = await in_flight
            _log.debug(f"Task for {fn.__name__} completed, result: {type(result)}")
            return result
        except Exception as e:
            _log.error(f"Task for {fn.__name__} failed: {e}", exc_info=True)
            result = None # Ensure result is None on failure
            return None # Return None if the creation task fails
        finally:
            # Reset in_flight once the task is complete (success or failure)
            # so subsequent calls can retry if needed (e.g., if result is None)
            in_flight = None

    # Add a method to reset the cached result, needed by close_redis
    async def reset():
        nonlocal result, in_flight
        _log.debug(f"Resetting cache for {fn.__name__}")
        # Cancel any in-flight task if it exists
        if in_flight and not in_flight.done():
            in_flight.cancel()
            try:
                await in_flight # Allow cancellation to propagate
            except asyncio.CancelledError:
                _log.debug(f"Cancelled in-flight task for {fn.__name__}")
            except Exception as e:
                 _log.warning(f"Error awaiting cancelled task for {fn.__name__}: {e}")
        in_flight = None
        result_to_close = result
        result = None
        return result_to_close # Return the closed client if needed

    wrapper.reset = reset # type: ignore
    return wrapper

# --- Global variable to hold the result of the _once function ---
# This isn't strictly necessary with the decorator managing state,
# but can be useful for direct access if needed elsewhere (though discouraged).
_redis_client: aioredis.Redis | None = None

@_once
async def _create_redis_connection() -> aioredis.Redis | None:
    """Internal function to create the Redis connection, decorated by _once."""
    global _redis_client
    url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    _log.info(f"Attempting to create Redis connection to: {url}")
    try:
        client = aioredis.from_url(
            url,
            decode_responses=False,
            socket_connect_timeout=1,   # 1-second TCP connect cap
            socket_timeout=2,           # 2-second op cap
            # health_check_interval=30 # Let's rely on op timeouts for now
        )
        # REMOVED ping check - rely on operation timeouts
        # await asyncio.wait_for(client.ping(), timeout=2.0)
        _log.info(f"Successfully created Redis client for {url}") # Adjusted log message
        _redis_client = client # Store the successful client
        return client
    except (RedisError, ConnectionError, TimeoutError, asyncio.TimeoutError) as exc:
        _log.error(f"Failed to connect to Redis at {url} – cache disabled ({exc})")
        _redis_client = None
        return None
    except Exception as exc:
        _log.error(f"An unexpected error occurred during Redis connection: {exc}", exc_info=True)
        _redis_client = None
        return None

async def get_redis() -> aioredis.Redis | None:
    """
    Return a live Redis client connection pool instance using the lock-free
    _once pattern. Returns None if the connection fails.
    """
    # Call the wrapped _create_redis_connection function
    return await _create_redis_connection() # type: ignore

async def close_redis() -> None:
    """Close and discard the cached client."""
    global _redis_client
    # Use the reset method added to the wrapper by _once
    client_to_close = await _create_redis_connection.reset() # type: ignore

    if client_to_close:
        _log.info(f"Closing Redis connection pool...")
        try:
            # Use aclose() as close() is deprecated
            await client_to_close.aclose()
            _log.info("Redis connection pool closed.")
        except RedisError as e:
            _log.warning(f"Error closing Redis connection: {e}")
        except Exception as e:
             _log.error(f"Unexpected error closing Redis connection: {e}", exc_info=True)
    # Ensure the global variable is also cleared
    _redis_client = None