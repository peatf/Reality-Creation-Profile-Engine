import asyncio
import logging
import os
import time
import uuid
from typing import Optional
from unittest.mock import patch, AsyncMock

import pytest
import pytest_asyncio
import redis.asyncio as redis
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient
from redis.exceptions import RedisError
# from testcontainers.redis import RedisContainer # Removed unused import

# Assuming src is importable
from src.middleware.rate_limit import RateLimitingMiddleware, RATE_LIMITS, WINDOW_SIZE_SECONDS
# Import connection logic from the new helper and constants
from src.cache.connection import get_redis, close_redis
from src.cache.decorators import NAMESPACE # Use the defined constant

# Configure logging for tests
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# --- Mock User State ---

class MockUser:
    def __init__(self, user_id: str, tier: str):
        self.id = user_id
        self.tier = tier

class MockRequestState:
    def __init__(self, user: Optional[MockUser]):
        self.user = user

# --- Fixtures ---

# The redis_container_session fixture in conftest.py now handles container start/stop
# and setting the REDIS_URL environment variable for the entire session.

# Import the correct connection helper and types
from src.cache.connection import get_redis, close_redis
from typing import AsyncGenerator
import redis.asyncio as aioredis # For type hint

@pytest_asyncio.fixture(scope="function", autouse=True)
async def redis_client() -> AsyncGenerator[aioredis.Redis, None]:
    """
    Provides a function-scoped async Redis client connected via the helper
    and clears relevant rate limit keys before each test. Relies on the session-scoped
    redis_container_session fixture in conftest.py.
    """
    # Get connection using the helper
    test_conn = await get_redis()
    if not test_conn:
        pytest.fail("Failed to get Redis connection via get_redis() in test setup. Session fixture might have failed.")

    # Clear all keys matching the rate limit namespace before each test function
    keys_deleted = 0
    try:
        async for key in test_conn.scan_iter(match=f"{NAMESPACE}rl:*"): # Use NAMESPACE
            # Add timeout to delete operation within the loop
            await asyncio.wait_for(test_conn.delete(key), timeout=2.0)
            keys_deleted += 1
        if keys_deleted > 0:
            logger.debug(f"Cleared {keys_deleted} rcpe:rl:* keys before test.")
    except asyncio.TimeoutError:
        logger.error("Redis delete operation timed out during pre-test key clearing.")
        pytest.fail("Redis timeout during pre-test key clearing.")
    except RedisError as e:
        logger.error(f"Redis error during key clearing: {e}")
        pytest.fail(f"Redis error during pre-test key clearing: {e}")

    # Reset Lua script SHA cache in middleware before each test
    # This ensures tests don't interfere with each other if SHA loading fails in one
    # Need to import sys for this approach
    import sys
    if 'src.middleware.rate_limit' in sys.modules:
         middleware_module = sys.modules['src.middleware.rate_limit']
         # Check if the lock exists before trying to reset SHA
         if hasattr(middleware_module, '_lua_sha_lock'):
             async with middleware_module._lua_sha_lock: # Ensure thread-safety if tests run in parallel
                 if hasattr(middleware_module, '_lua_sha'):
                     middleware_module._lua_sha = None
                     logger.debug("Reset rate limit Lua SHA cache.")
         else:
             # Fallback if lock doesn't exist (older version?)
             if hasattr(middleware_module, '_lua_sha'):
                 middleware_module._lua_sha = None
                 logger.debug("Reset rate limit Lua SHA cache (without lock).")


    yield test_conn # Provide the connection for direct checks if needed

    # No explicit close needed here


@pytest.fixture(scope="function")
def test_app() -> FastAPI: # Removed redis_container dependency
    """Creates a FastAPI app instance with the middleware."""
    app = FastAPI()
    app.add_middleware(RateLimitingMiddleware)

    @app.get("/limited")
    async def limited_route(request: Request):
        # This endpoint relies on middleware attaching user state
        user_id = getattr(getattr(request.state, 'user', None), 'id', 'unknown')
        return JSONResponse({"message": f"Success for user {user_id}"})

    @app.get("/unlimited")
    async def unlimited_route():
        # Endpoint without user state dependency for basic tests
        return JSONResponse({"message": "Success unlimited"})

    return app

@pytest.fixture(scope="function")
def client(test_app: FastAPI) -> TestClient:
    """Provides a TestClient for the FastAPI app."""
    # Using httpx TestClient which handles async routes
    with TestClient(test_app) as c:
        yield c

# --- Helper Function ---

async def make_requests(client: TestClient, num_requests: int, user_id: str, tier: str, path: str = "/limited"):
    """Makes multiple async requests using the TestClient."""
    responses = []
    # TestClient doesn't directly support setting state easily for multiple requests.
    # We need to patch the middleware's dispatch method or the point where it reads state.
    # Patching `request.state` directly within the TestClient call is tricky.
    # A more robust way is to patch where the middleware *accesses* the state.

    async def mock_dispatch(self, request: Request, call_next):
        # Inject the mock state just before the original dispatch logic runs
        request._state = MockRequestState(user=MockUser(user_id=user_id, tier=tier))
        # Need to find the original dispatch method to call it
        # This is complex. Alternative: Patch get_redis_connection or the Lua call
        # Easier Alternative: Modify the test endpoint to simulate state setting
        # Let's try patching the state access within the middleware code directly for the test duration
        with patch('src.middleware.rate_limit.RateLimitingMiddleware.dispatch', autospec=True) as mock_dispatch_method:

            async def side_effect_dispatch(slf, req, cn):
                # The actual middleware instance is 'slf'
                # Modify the request 'req' that the *real* dispatch will receive
                req._state = MockRequestState(user=MockUser(user_id=user_id, tier=tier))
                # Now call the *original* unbound method with the instance and args
                # This requires getting the original method before patching.
                # Let's rethink: TestClient might not be ideal for complex middleware state mocking.
                # Let's test by calling the middleware dispatch directly.

                # --- Direct Middleware Call Approach ---
                original_dispatch = RateLimitingMiddleware.dispatch # Store original
                async def call_endpoint(request: Request):
                    # Simulate the endpoint being called by call_next
                    return Response(status_code=200, content=b'{"message":"Success"}')

                middleware = RateLimitingMiddleware(app=None) # App instance not needed for direct call

                tasks = []
                for i in range(num_requests):
                    # Create a mock request object for each call
                    mock_req = Request({
                        "type": "http",
                        "method": "GET",
                        "path": path,
                        "headers": [],
                        "query_string": b"",
                        "scope": { # Need a basic scope
                             "type": "http", "method": "GET", "path": path, "headers": [],
                             "state": {} # Start with empty state dict
                        }
                    })
                    # Set the state directly on the mock request
                    mock_req._state = MockRequestState(user=MockUser(user_id=user_id, tier=tier))
                    tasks.append(middleware.dispatch(mock_req, call_endpoint))

                results = await asyncio.gather(*tasks, return_exceptions=True)
                return results # Return list of Response objects or exceptions

    # Use the direct middleware call approach
    responses = await call_endpoint_directly(num_requests, user_id, tier, path)
    return responses


async def call_endpoint_directly(num_requests: int, user_id: str, tier: str, path: str = "/limited"):
    """Helper to call the middleware dispatch directly with mocked requests."""
    async def mock_call_next(request: Request):
        # Simulate the endpoint being called by call_next
        # Return a basic success response that the middleware can add headers to
        return Response(status_code=200, content=b'{"message":"Success"}', media_type="application/json")

    middleware = RateLimitingMiddleware(app=None) # App instance not strictly needed here

    tasks = []
    for _ in range(num_requests):
        # Create a mock request object for each call
        # Scope needs enough info for the middleware (method, path, state)
        scope = {
            "type": "http",
            "method": "GET",
            "path": path,
            "headers": [],
            "query_string": b"",
            "client": ("127.0.0.1", 8000), # Add client if needed for IP fallback
            "state": {} # Initialize state dict
        }
        mock_req = Request(scope)
        # Set the state directly on the mock request's scope state dict
        mock_req.scope["state"]["user"] = MockUser(user_id=user_id, tier=tier)

        tasks.append(middleware.dispatch(mock_req, mock_call_next))

    results = await asyncio.gather(*tasks, return_exceptions=True)
    return results


# --- Test Functions ---

@pytest.mark.asyncio
async def test_rate_limit_free_tier():
    """Tests rate limiting for the free tier."""
    user_id = f"user_free_{uuid.uuid4()}"
    tier = "free"
    limit = RATE_LIMITS[tier]
    num_requests = limit + 5 # Exceed limit

    logger.info(f"Test Free Tier: Making {num_requests} requests for user {user_id} (limit {limit})")
    responses = await call_endpoint_directly(num_requests, user_id, tier)

    success_count = 0
    limited_count = 0
    for i, resp in enumerate(responses):
        assert not isinstance(resp, Exception), f"Request {i+1} failed with exception: {resp}"
        expected_remaining = max(0, limit - (i + 1))

        assert "x-ratelimit-limit" in resp.headers
        assert resp.headers["x-ratelimit-limit"] == str(limit)

        if resp.status_code == 200:
            success_count += 1
            assert "x-ratelimit-remaining" in resp.headers
            assert resp.headers["x-ratelimit-remaining"] == str(expected_remaining)
            assert "retry-after" not in resp.headers
        elif resp.status_code == 429:
            limited_count += 1
            assert "x-ratelimit-remaining" in resp.headers
            assert resp.headers["x-ratelimit-remaining"] == "0"
            assert "retry-after" in resp.headers
            # Retry-After should be roughly WINDOW_SIZE_SECONDS
            assert abs(int(resp.headers["retry-after"]) - WINDOW_SIZE_SECONDS) <= 1
        else:
            pytest.fail(f"Unexpected status code {resp.status_code} for request {i+1}")

    assert success_count == limit
    assert limited_count == num_requests - limit


@pytest.mark.asyncio
async def test_rate_limit_premium_tier():
    """Tests rate limiting for the premium tier."""
    user_id = f"user_prem_{uuid.uuid4()}"
    tier = "premium"
    limit = RATE_LIMITS[tier]
    num_requests = limit + 10 # Exceed limit

    logger.info(f"Test Premium Tier: Making {num_requests} requests for user {user_id} (limit {limit})")
    responses = await call_endpoint_directly(num_requests, user_id, tier)

    success_count = 0
    limited_count = 0
    for i, resp in enumerate(responses):
        assert not isinstance(resp, Exception), f"Request {i+1} failed with exception: {resp}"
        expected_remaining = max(0, limit - (i + 1))

        assert resp.headers["x-ratelimit-limit"] == str(limit)
        if resp.status_code == 200:
            success_count += 1
            assert resp.headers["x-ratelimit-remaining"] == str(expected_remaining)
            assert "retry-after" not in resp.headers
        elif resp.status_code == 429:
            limited_count += 1
            assert resp.headers["x-ratelimit-remaining"] == "0"
            assert "retry-after" in resp.headers
            assert abs(int(resp.headers["retry-after"]) - WINDOW_SIZE_SECONDS) <= 1
        else:
            pytest.fail(f"Unexpected status code {resp.status_code} for request {i+1}")

    assert success_count == limit
    assert limited_count == num_requests - limit


@pytest.mark.asyncio
async def test_rate_limit_unknown_tier_uses_default():
    """Tests that an unknown tier falls back to the default limit."""
    user_id = f"user_unknown_{uuid.uuid4()}"
    tier = "enterprise" # Not in RATE_LIMITS
    limit = RATE_LIMITS["default"] # Should use default
    num_requests = limit + 5

    logger.info(f"Test Unknown Tier: Making {num_requests} requests for user {user_id} (tier '{tier}', expected limit {limit})")
    responses = await call_endpoint_directly(num_requests, user_id, tier)

    success_count = 0
    limited_count = 0
    for i, resp in enumerate(responses):
         assert not isinstance(resp, Exception)
         if resp.status_code == 200:
             success_count += 1
             assert resp.headers["x-ratelimit-limit"] == str(limit)
         elif resp.status_code == 429:
             limited_count += 1
             assert resp.headers["x-ratelimit-limit"] == str(limit)
         else:
             pytest.fail(f"Unexpected status code {resp.status_code}")

    assert success_count == limit
    assert limited_count == num_requests - limit


@pytest.mark.asyncio
async def test_rate_limit_concurrent_hits():
    """Tests concurrent requests to ensure atomicity (no race conditions)."""
    user_id = f"user_concurrent_{uuid.uuid4()}"
    tier = "free"
    limit = RATE_LIMITS[tier]
    num_concurrent_requests = limit + 20 # Fire more than the limit concurrently

    logger.info(f"Test Concurrent: Making {num_concurrent_requests} concurrent requests for user {user_id} (limit {limit})")
    # Use the direct call helper which already uses asyncio.gather
    responses = await call_endpoint_directly(num_concurrent_requests, user_id, tier)

    success_count = 0
    limited_count = 0
    for resp in responses:
        assert not isinstance(resp, Exception)
        if resp.status_code == 200:
            success_count += 1
        elif resp.status_code == 429:
            limited_count += 1
        else:
            pytest.fail(f"Unexpected status code {resp.status_code}")

    # Due to concurrency, the exact order isn't guaranteed, but the *total*
    # number of successful requests should not exceed the limit.
    logger.info(f"Test Concurrent: Success count = {success_count}, Limited count = {limited_count}")
    assert success_count <= limit
    # It's possible slightly fewer than `limit` succeed if multiple requests
    # read the count just before it hits the limit and then multiple increments
    # push it over the limit before others can read. The key is *not exceeding* limit.
    assert limited_count == num_concurrent_requests - success_count
    assert success_count + limited_count == num_concurrent_requests


@pytest.mark.asyncio
async def test_rate_limit_redis_down():
    """Tests that requests are allowed (fail open) when Redis is down."""
    user_id = f"user_redis_down_{uuid.uuid4()}"
    tier = "free"
    num_requests = 5

    logger.info("Test Redis Down: Simulating connection failure")
    # Patch get_redis (from src.cache.connection) used by the middleware
    with patch('src.cache.connection.get_redis', new_callable=AsyncMock) as mock_get_conn:
        mock_get_conn.return_value = None # Simulate connection failure

        responses = await call_endpoint_directly(num_requests, user_id, tier)

        assert len(responses) == num_requests
        for i, resp in enumerate(responses):
            assert not isinstance(resp, Exception)
            assert resp.status_code == 200 # Should pass through
            # Headers should NOT be added if Redis is down
            assert "x-ratelimit-limit" not in resp.headers
            assert "x-ratelimit-remaining" not in resp.headers
            logger.debug(f"Redis down response {i+1}: Status {resp.status_code}")

    # --- Test Redis Error during script execution ---
    await close_redis() # Reset pool state after patch
    logger.info("Test Redis Down: Simulating RedisError during EVALSHA/EVAL")
    real_conn = await get_redis()
    assert real_conn is not None

    with patch.object(real_conn, 'evalsha', new_callable=AsyncMock) as mock_evalsha, \
         patch.object(real_conn, 'eval', new_callable=AsyncMock) as mock_eval:
        mock_evalsha.side_effect = RedisError("Simulated EVALSHA error")
        mock_eval.side_effect = RedisError("Simulated EVAL error") # Also patch EVAL fallback

        responses = await call_endpoint_directly(num_requests, user_id, tier)

        assert len(responses) == num_requests
        for i, resp in enumerate(responses):
            assert not isinstance(resp, Exception)
            assert resp.status_code == 200 # Should fail open
            assert "x-ratelimit-limit" not in resp.headers # No headers on error
            logger.debug(f"Redis error response {i+1}: Status {resp.status_code}")

    await close_redis() # Reset pool state


@pytest.mark.asyncio
async def test_rate_limit_no_user_state():
    """Tests that requests pass through if user state is missing."""
    num_requests = 5
    logger.info("Test No User State: Making requests without user info")

    # Use the TestClient approach here, as it's simpler for no state
    # Need a client fixture that doesn't automatically patch state
    app = FastAPI()
    app.add_middleware(RateLimitingMiddleware)
    @app.get("/no_state_test")
    async def no_state_route():
        return JSONResponse({"message": "Success"})

    with TestClient(app) as local_client:
        for i in range(num_requests):
            response = local_client.get("/no_state_test")
            assert response.status_code == 200
            # No rate limit headers should be added
            assert "x-ratelimit-limit" not in response.headers
            assert "x-ratelimit-remaining" not in response.headers
            logger.debug(f"No user state response {i+1}: Status {response.status_code}")