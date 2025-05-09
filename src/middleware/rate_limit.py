import asyncio
import logging
import time
from typing import Awaitable, Callable, Dict, Optional, Tuple

import redis.asyncio as redis
from fastapi import Request, Response
from fastapi.responses import JSONResponse
from redis.exceptions import RedisError
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.types import ASGIApp

# Import the new connection helper and constants
from src.cache.connection import get_redis, close_redis as close_redis_pool
from src.cache.decorators import NAMESPACE # Use the defined constant

logger = logging.getLogger(__name__)

# Rate limits per minute
RATE_LIMITS: Dict[str, int] = {
    "free": 40,
    "premium": 160,
    "default": 40, # Fallback if tier is unknown
}
WINDOW_SIZE_SECONDS = 60 # Per minute window

# Lua script for atomic sliding window increment
LUA_SCRIPT = """
local key = KEYS[1]
local expiry = tonumber(ARGV[1])
local count = redis.call("INCR", key)
if count == 1 then
    redis.call("EXPIRE", key, expiry)
end
return count
"""
# Cache the script SHA locally within the middleware instance or globally
_lua_sha: Optional[str] = None
_lua_sha_lock = asyncio.Lock()

async def get_lua_sha(redis_conn: redis.Redis) -> Optional[str]:
    """Loads the Lua script into Redis and returns its SHA."""
    global _lua_sha
    # Use lock to prevent multiple concurrent loads
    async with _lua_sha_lock:
        if _lua_sha is None:
            try:
                # Ensure connection is valid before loading script
                await redis_conn.ping()
                _lua_sha = await redis_conn.script_load(LUA_SCRIPT)
                logger.info(f"Loaded rate limiting Lua script with SHA: {_lua_sha}")
            except RedisError as e:
                logger.error(f"Failed to load Lua script into Redis: {e}")
                _lua_sha = None # Ensure it's None if loading fails
            except Exception as e:
                logger.error(f"Unexpected error loading Lua script: {e}")
                _lua_sha = None
        return _lua_sha


class RateLimitingMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: ASGIApp):
        super().__init__(app)
        # No script pre-loading here, do it lazily on first request

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        """Applies rate limiting logic before processing the request."""

        # --- 1. Check if rate limiting should apply ---
        # (Add path exclusions etc. if needed)

        # --- 2. Get User Identifier and Tier ---
        user_id: Optional[str] = None
        user_tier: str = "default"

        if hasattr(request.state, "user") and request.state.user:
            user_id = getattr(request.state.user, "id", None)
            tier = getattr(request.state.user, "tier", "free")
            user_tier = tier if tier in RATE_LIMITS else "default"
        else:
             logger.warning("Rate limiting skipped: request.state.user not found or has no ID.")
             return await call_next(request)

        if not user_id:
             logger.warning("Rate limiting skipped: User ID not found in request.state.user.")
             return await call_next(request)

        # --- 3. Connect to Redis (lazily) ---
        redis_conn = await get_redis()
        if not redis_conn:
            logger.warning("Redis unavailable, skipping rate limiting.")
            response = await call_next(request)
            return response

        # --- 4. Prepare Rate Limiting Key and Parameters ---
        limit = RATE_LIMITS.get(user_tier, RATE_LIMITS["default"])
        # Use integer division for epoch minute
        current_minute_epoch = int(time.time() // WINDOW_SIZE_SECONDS)
        key = f"{NAMESPACE}rl:{user_id}:{current_minute_epoch}" # Use NAMESPACE
        expiry_seconds = WINDOW_SIZE_SECONDS * 2 # Expire slightly longer

        # --- 5. Execute Lua Script ---
        current_count = 0
        try:
            # Ensure Lua script SHA is loaded
            sha = await get_lua_sha(redis_conn)
            if sha:
                current_count = await redis_conn.evalsha(sha, 1, key, str(expiry_seconds))
            else:
                # Fallback to EVAL if SHA loading failed
                logger.warning("Lua script SHA not available, using EVAL.")
                current_count = await redis_conn.eval(LUA_SCRIPT, 1, key, str(expiry_seconds))
            current_count = int(current_count)
        except RedisError as e:
            logger.error(f"Redis error during rate limiting for user {user_id}: {e}. Allowing request.")
            response = await call_next(request)
            return response
        except Exception as e:
            logger.error(f"Unexpected error during rate limiting execution for user {user_id}: {e}. Allowing request.", exc_info=True)
            response = await call_next(request)
            return response

        # --- 6. Calculate Remaining and Check Limit ---
        remaining = max(0, limit - current_count)

        # --- 7. Handle Limit Exceeded ---
        if current_count > limit:
            retry_after = WINDOW_SIZE_SECONDS # Simple retry after the window ends
            logger.warning(f"Rate limit exceeded for user {user_id} (tier: {user_tier}). Count: {current_count}, Limit: {limit}")
            # Use lowercase header names as specified in requirement
            headers = {
                "x-ratelimit-limit": str(limit),
                "x-ratelimit-remaining": "0",
                "retry-after": str(retry_after), # Corresponds to 'reset' concept
            }
            return JSONResponse(
                status_code=429,
                content={"detail": f"Rate limit exceeded. Limit: {limit} requests per minute."},
                headers=headers,
            )

        # --- 8. Process Request and Add Headers to Success Response ---
        response = await call_next(request)
        # Ensure headers are added even if response is not JSONResponse
        if hasattr(response, "headers"):
             # Use lowercase header names as specified in requirement
             headers = {
                 "x-ratelimit-limit": str(limit),
                 "x-ratelimit-remaining": str(remaining),
             }
             response.headers.update(headers)
             logger.debug(f"Rate limit check passed for user {user_id}. Count: {current_count}, Remaining: {remaining}")
        else:
             logger.warning(f"Could not add rate limit headers to response of type {type(response)}")


        return response

# Optional: Add shutdown hook if needed for cleanup using the connection helper
# async def shutdown_event():
#     await close_redis_pool()