import asyncio
import logging
import os
import pickle
import time
import uuid
import hashlib # Added top-level import
from typing import Callable, Optional # Added Optional import
from unittest.mock import patch, AsyncMock

import pytest
import pytest_asyncio
import redis.asyncio as redis
from redis.exceptions import RedisError
# from testcontainers.redis import RedisContainer # Removed unused import
from typing import AsyncGenerator # Import needed type hint

# Assuming src is importable (e.g., added to PYTHONPATH or using package structure)
# Import only necessary symbols from decorators and the new connection helper
from src.cache.decorators import redis_cache, invalidate, NAMESPACE # Use NAMESPACE constant
from src.cache.utils import clear_cache
from src.cache.connection import get_redis, close_redis # Import from connection module

# Configure logging for tests
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# --- Fixtures ---

# The redis_container_session fixture in conftest.py now handles container start/stop
# and setting the REDIS_URL environment variable for the entire session.

# Import the correct connection helper
from src.cache.connection import get_redis, close_redis

@pytest_asyncio.fixture(scope="function", autouse=True)
async def redis_client() -> AsyncGenerator[redis.Redis, None]:
    """
    Provides a function-scoped async Redis client connected via the helper
    and clears relevant keys before each test. Relies on the session-scoped
    redis_container_session fixture in conftest.py to manage the container
    and environment variable.
    """
    # Get connection using the helper (which now uses the correct REDIS_URL)
    test_conn = await get_redis()
    if not test_conn:
        # The session fixture should have already failed if connection wasn't possible
        pytest.fail("Failed to get Redis connection via get_redis() in test setup. Session fixture might have failed.")

    # Clear all keys matching the namespace before each test function
    keys_deleted = 0
    try:
        async for key in test_conn.scan_iter(match=f"{NAMESPACE}*"): # Use NAMESPACE
            # Add timeout to delete operation within the loop
            await asyncio.wait_for(test_conn.delete(key), timeout=2.0)
            keys_deleted += 1
        if keys_deleted > 0:
            logger.debug(f"Cleared {keys_deleted} rcpe:* keys before test.")
    except asyncio.TimeoutError:
        logger.error("Redis delete operation timed out during pre-test key clearing.")
        pytest.fail("Redis timeout during pre-test key clearing.")
    except RedisError as e:
        logger.error(f"Redis error during key clearing: {e}")
        # Depending on policy, might want to fail the test here
        pytest.fail(f"Redis error during pre-test key clearing: {e}")


    yield test_conn # Provide the connection for direct interaction if needed

    # No explicit close needed here for the connection object itself,
    # as the pool is managed centrally. The session fixture handles final pool closure.


@pytest.fixture()
def cached_function_fixture():
    """Fixture that defines the cached function *after* Redis setup."""
    # Define the function inside the fixture scope
    @redis_cache(ttl=60)
    async def cached_function(arg1: str, kwarg1: int = 0) -> dict:
        """A simple async function to test caching."""
        global mock_call_count
        mock_call_count += 1
        logger.debug(f"Executing cached_function with arg1={arg1}, kwarg1={kwarg1}")
        return {"result": f"{arg1}-{kwarg1}", "uuid": str(uuid.uuid4())}
    return cached_function


# --- Test Functions ---

# Mocked function to track calls
mock_call_count = 0

# Original cached_function removed from module level

@redis_cache(ttl=0)
async def non_caching_function(x: int) -> int:
    """A function with TTL=0, should not be cached."""
    global mock_call_count
    mock_call_count += 1
    logger.debug(f"Executing non_caching_function with x={x}")
    return x * 2

async def non_picklable_result_func() -> Callable:
    """Returns a non-picklable local function."""
    def local_func():
        pass
    return local_func

@redis_cache(ttl=60)
async def cached_non_picklable_func() -> Callable:
     """Decorated function returning non-picklable result."""
     global mock_call_count
     mock_call_count += 1
     return await non_picklable_result_func()


@pytest.mark.asyncio
async def test_cache_hit_miss(redis_client: redis.Redis, cached_function_fixture):
    """Tests cache miss, hit, and different args."""
    cached_function = cached_function_fixture # Get function from fixture
    global mock_call_count
    mock_call_count = 0

    # 1. First call (miss)
    logger.info("Test: First call (miss)")
    result1 = await cached_function("data1", kwarg1=10)
    assert mock_call_count == 1
    assert result1["result"] == "data1-10"
    uuid1 = result1["uuid"]

    # Verify data in Redis (optional, but good for debugging)
    # Construct the expected key
    # import hashlib # Removed local import
    arg_key_part = pickle.dumps((("data1",), {"kwarg1": 10}), protocol=5)
    hashed_args = hashlib.sha1(arg_key_part).hexdigest()
    # Use the correct key format including module and qualname
    expected_key = f"{NAMESPACE}{cached_function.__module__}.{cached_function.__qualname__}:{hashed_args}"
    # Add timeout to direct Redis check
    raw_value = await asyncio.wait_for(redis_client.get(expected_key), timeout=2.0)
    assert raw_value is not None
    cached_data = pickle.loads(raw_value)
    assert cached_data == result1

    # 2. Second call with same args (hit)
    logger.info("Test: Second call (hit)")
    await asyncio.sleep(0.1) # Ensure time progresses slightly, but within TTL
    result2 = await cached_function("data1", kwarg1=10)
    assert mock_call_count == 1 # Should not have incremented
    assert result2["result"] == "data1-10"
    assert result2["uuid"] == uuid1 # Should be the exact same object from cache

    # 3. Third call with different args (miss)
    logger.info("Test: Third call, different args (miss)")
    result3 = await cached_function("data2", kwarg1=20)
    assert mock_call_count == 2 # Should increment
    assert result3["result"] == "data2-20"
    assert result3["uuid"] != uuid1

    # 4. Fourth call with original args again (hit)
    logger.info("Test: Fourth call, original args (hit)")
    result4 = await cached_function("data1", kwarg1=10)
    assert mock_call_count == 2 # Should not increment
    assert result4 == result1 # Check full dict equality


@pytest.mark.asyncio
async def test_cache_ttl_expiry(redis_client: redis.Redis):
    """Tests that cache entries expire after TTL."""
    global mock_call_count
    mock_call_count = 0

    @redis_cache(ttl=1) # Short TTL for testing
    async def short_ttl_func(val: str) -> str:
        global mock_call_count
        mock_call_count += 1
        return f"processed:{val}:{uuid.uuid4()}"

    # Call 1 (miss)
    logger.info("Test TTL: First call (miss)")
    res1 = await short_ttl_func("expire_test")
    assert mock_call_count == 1

    # Call 2 (hit - immediately after)
    logger.info("Test TTL: Second call (hit)")
    res2 = await short_ttl_func("expire_test")
    assert mock_call_count == 1
    assert res1 == res2

    # Wait for TTL to expire
    logger.info("Test TTL: Waiting for TTL expiry...")
    await asyncio.sleep(1.5)

    # Call 3 (miss - after expiry)
    logger.info("Test TTL: Third call (miss after expiry)")
    res3 = await short_ttl_func("expire_test")
    assert mock_call_count == 2
    assert res3 != res1


@pytest.mark.asyncio
async def test_cache_ttl_zero_skips_cache(redis_client: redis.Redis):
    """Tests that ttl=0 skips caching entirely."""
    global mock_call_count
    mock_call_count = 0

    # Call multiple times
    logger.info("Test TTL=0: First call")
    res1 = await non_caching_function(5)
    assert res1 == 10
    assert mock_call_count == 1

    logger.info("Test TTL=0: Second call")
    res2 = await non_caching_function(5)
    assert res2 == 10
    assert mock_call_count == 2 # Always executes

    # Check that no keys were added to Redis for this function
    keys_found = 0
    async for _ in redis_client.scan_iter(match=f"{NAMESPACE}{non_caching_function.__module__}.{non_caching_function.__qualname__}:*"): # Use NAMESPACE
        keys_found += 1
    assert keys_found == 0


@pytest.mark.asyncio
async def test_invalidate_function(redis_client: redis.Redis, cached_function_fixture):
    """Tests the invalidate function from the decorators module."""
    cached_function = cached_function_fixture # Get function from fixture
    global mock_call_count
    mock_call_count = 0

    # Populate cache with multiple keys
    logger.info("Test Invalidate: Populating cache")
    await cached_function("invalidate_test", kwarg1=1) # Key 1
    await cached_function("invalidate_test", kwarg1=2) # Key 2
    await cached_function("other_data", kwarg1=3)      # Key 3
    assert mock_call_count == 3

    # Invalidate a subset using pattern
    pattern_to_invalidate = f"{cached_function.__module__}.{cached_function.__qualname__}:*" # Matches all keys for this func
    logger.info(f"Test Invalidate: Invalidating pattern '{pattern_to_invalidate}'")
    # Note: invalidate adds the NAMESPACE automatically and returns None
    await invalidate(pattern=f"{cached_function.__module__}.{cached_function.__qualname__}:*")
    # No return value to assert for decorators.invalidate

    # Verify keys are gone by trying to hit cache (should miss)
    logger.info("Test Invalidate: Verifying cache miss after invalidation")
    await cached_function("invalidate_test", kwarg1=1)
    assert mock_call_count == 4 # Incremented due to miss

    await cached_function("invalidate_test", kwarg1=2)
    assert mock_call_count == 5 # Incremented due to miss

    await cached_function("other_data", kwarg1=3)
    assert mock_call_count == 6 # Incremented due to miss


@pytest.mark.asyncio
async def test_clear_cache_utility(redis_client: redis.Redis, cached_function_fixture):
    """Tests the clear_cache utility function."""
    cached_function = cached_function_fixture # Get function from fixture
    global mock_call_count
    mock_call_count = 0

    # Populate cache
    logger.info("Test Clear Cache Util: Populating cache")
    res1 = await cached_function("clear_util_test", kwarg1=100) # Key 1
    res2 = await cached_function("clear_util_test", kwarg1=200) # Key 2
    res3 = await cached_function("another_func_domain", kwarg1=300) # Key 3
    assert mock_call_count == 3

    # Clear a subset using pattern
    pattern_to_clear = f"{cached_function.__module__}.{cached_function.__qualname__}:*" # Matches all keys for cached_function
    logger.info(f"Test Clear Cache Util: Clearing pattern '{pattern_to_clear}'")
    # Note: clear_cache adds the NAMESPACE_PREFIX automatically
    deleted_count = await clear_cache(pattern=pattern_to_clear)
    assert deleted_count == 3

    # Verify keys are gone (miss)
    logger.info("Test Clear Cache Util: Verifying cache miss")
    await cached_function("clear_util_test", kwarg1=100)
    assert mock_call_count == 4 # Miss

    # Verify keys directly in Redis (should be none for the pattern) - scan_iter itself doesn't block indefinitely
    keys_found = 0
    async for _ in redis_client.scan_iter(match=f"{NAMESPACE}{pattern_to_clear}"): # Use NAMESPACE
         keys_found += 1
    assert keys_found == 0


@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_cache_handles_none_return_with_sentinel(redis_client: redis.Redis):
    """Tests that None is correctly cached and retrieved using the sentinel."""
    global mock_call_count
    mock_call_count = 0

    @redis_cache(ttl=60)
    async def cached_func_returns_none(param: str) -> Optional[str]:
        global mock_call_count
        mock_call_count += 1
        logger.debug(f"Executing cached_func_returns_none with param={param}")
        if param == "return_none":
            return None
        return f"value:{param}"

    # Test case 1: Non-None value
    logger.info("Test Sentinel: First call (non-None, miss)")
    res1_val = await cached_func_returns_none("test_val")
    assert mock_call_count == 1
    assert res1_val == "value:test_val"

    logger.info("Test Sentinel: Second call (non-None, hit)")
    res2_val = await cached_func_returns_none("test_val")
    assert mock_call_count == 1 # Hit
    assert res2_val == "value:test_val"

    # Reset for None case
    mock_call_count = 0

    # Test case 2: None value
    logger.info("Test Sentinel: First call (None, miss)")
    res1_none = await cached_func_returns_none("return_none")
    assert mock_call_count == 1
    assert res1_none is None

    # Verify data in Redis (should be the sentinel object pickled)
    # import hashlib # Removed local import
    import pickle # Ensure pickle is imported
    # Import the class to check its type, not the instance for `is` comparison after unpickling
    from src.cache.decorators import _CacheSentinelClass
    arg_key_part_none = pickle.dumps((("return_none",), {}), protocol=5) # Args for "return_none"
    hashed_args_none = hashlib.sha1(arg_key_part_none).hexdigest()
    expected_key_none = f"{NAMESPACE}{cached_func_returns_none.__module__}.{cached_func_returns_none.__qualname__}:{hashed_args_none}"

    raw_value_none = await asyncio.wait_for(redis_client.get(expected_key_none), timeout=2.0)
    assert raw_value_none is not None, "Cache key for None result should exist"
    cached_data_none = pickle.loads(raw_value_none)
    # Check if the unpickled object is an instance of our sentinel class
    assert isinstance(cached_data_none, _CacheSentinelClass), "Cached value for None should be an instance of _CacheSentinelClass"
 
    logger.info("Test Sentinel: Second call (None, hit)")
    res2_none = await cached_func_returns_none("return_none")
    assert mock_call_count == 1 # Should be a cache hit
    assert res2_none is None
async def test_cache_handles_redis_down(cached_function_fixture):
    """Tests fallback behavior when Redis connection fails."""
    cached_function = cached_function_fixture # Get function from fixture
    global mock_call_count
    mock_call_count = 0

    # --- Test case 1: Connection fails during initial get_redis_connection ---
    logger.info("Test Redis Down: Simulating connection failure")
    # Patch get_redis (imported from src.cache.connection) to return None
    with patch('src.cache.connection.get_redis', new_callable=AsyncMock) as mock_get_conn:
        mock_get_conn.return_value = None

        result = await cached_function("live_data", kwarg1=50)
        assert mock_call_count == 1 # Function was executed
        assert result["result"] == "live_data-50"
        mock_get_conn.assert_called_once() # Ensure it was called

    # Reset count for next part
    mock_call_count = 0
    # No explicit close needed here, rely on session fixture teardown

    # --- Test case 2: Connection works initially, but fails during GET/SET ---
    logger.info("Test Redis Down: Simulating RedisError during GET")
    # Need a real connection first using the correct helper
    real_conn = await get_redis()
    assert real_conn is not None

    # Patch the 'get' method of the connection object to raise RedisError
    with patch.object(real_conn, 'get', new_callable=AsyncMock) as mock_redis_get:
        mock_redis_get.side_effect = RedisError("Simulated GET error")

        result = await cached_function("live_data_get_fail", kwarg1=60)
        assert mock_call_count == 1 # Function executed on GET failure
        assert result["result"] == "live_data_get_fail-60"
        mock_redis_get.assert_called_once() # GET was attempted

    # Reset count
    mock_call_count = 0
    # No explicit close needed here

    # --- Test case 3: Connection works for GET, but fails during SET (was SETEX) ---
    logger.info("Test Redis Down: Simulating RedisError during SET")
    real_conn_set = await get_redis()
    assert real_conn_set is not None

    # Ensure cache is empty for this key first
    arg_key_part_set = pickle.dumps((("live_data_set_fail",), {"kwarg1": 70}), protocol=5)
    hashed_args_set = hashlib.sha1(arg_key_part_set).hexdigest()
    key_set = f"{NAMESPACE}{cached_function.__module__}.{cached_function.__qualname__}:{hashed_args_set}" # Use NAMESPACE
    # Add timeout
    await asyncio.wait_for(real_conn_set.delete(key_set), timeout=2.0)
 
    # Patch 'setex' as the decorator uses redis_conn.setex(key, ttl, value)
    with patch.object(real_conn_set, 'setex', new_callable=AsyncMock) as mock_redis_setex:
        mock_redis_setex.side_effect = RedisError("Simulated SETEX error")
 
        result = await cached_function("live_data_set_fail", kwarg1=70)
        assert mock_call_count == 1 # Function executed (cache miss)
        assert result["result"] == "live_data_set_fail-70"
        mock_redis_setex.assert_called_once() # SETEX was attempted but failed
 
        # Verify the key was NOT set in Redis - add timeout
        val = await asyncio.wait_for(real_conn_set.get(key_set), timeout=2.0)
        assert val is None

    # No explicit close needed here


@pytest.mark.asyncio
async def test_cache_handles_unpickling_error(redis_client: redis.Redis, cached_function_fixture):
    """Tests behavior when cached data is corrupted or unpicklable."""
    cached_function = cached_function_fixture # Get function from fixture
    global mock_call_count
    mock_call_count = 0

    # Manually set a corrupted value in Redis for a specific key
    arg_key_part = pickle.dumps((("unpickle_test",), {"kwarg1": 1}), protocol=5)
    hashed_args = hashlib.sha1(arg_key_part).hexdigest()
    corrupted_key = f"{NAMESPACE}{cached_function.__module__}.{cached_function.__qualname__}:{hashed_args}" # Use NAMESPACE

    logger.info(f"Test Unpickle Error: Setting corrupted data for key {corrupted_key}")
    # Add timeout
    await asyncio.wait_for(redis_client.set(corrupted_key, b"this is not pickled data {"), timeout=2.0)

    # Call the function - should detect unpickling error and execute live
    logger.info("Test Unpickle Error: Calling function, expecting live execution")
    result = await cached_function("unpickle_test", kwarg1=1)
    assert mock_call_count == 1 # Executed live
    assert result["result"] == "unpickle_test-1"

    # Verify the corrupted key might still be there or optionally deleted by the decorator
    # Current implementation logs warning but doesn't delete, let's check it's still there - add timeout
    corrupted_value = await asyncio.wait_for(redis_client.get(corrupted_key), timeout=2.0)
    assert corrupted_value == b"this is not pickled data {"

    # Now, let's test the SET path again - it should overwrite the bad data
    logger.info("Test Unpickle Error: Calling again to overwrite corrupted data")
    result2 = await cached_function("unpickle_test", kwarg1=1)
    assert mock_call_count == 2 # Executed live again because previous SET failed implicitly
    assert result2["result"] == "unpickle_test-1"

    # Verify the key now contains valid data - add timeout
    valid_value = await asyncio.wait_for(redis_client.get(corrupted_key), timeout=2.0)
    assert valid_value is not None
    try:
        data = pickle.loads(valid_value)
        assert data == result2
    except pickle.UnpicklingError:
        pytest.fail("Data in cache was not overwritten with valid pickled data.")


@pytest.mark.asyncio
async def test_cache_handles_pickling_error_on_set(redis_client: redis.Redis):
    """Tests behavior when the return value cannot be pickled, ensuring the error branch is hit."""
    global mock_call_count
    mock_call_count = 0
 
    # Patch the logger for src.cache.decorators to check for the warning
    with patch('src.cache.decorators.logger.warning') as mock_decorator_logger_warning:
        logger.info("Test Pickling Error: Calling function with non-picklable return")
        # This function returns a local function, which cannot be pickled
        result = await cached_non_picklable_func()
        assert mock_call_count == 1
        assert callable(result) # Got the live result

        # Verify the specific warning for pickling failure was logged
        mock_decorator_logger_warning.assert_any_call(
            f"Failed to pickle result/sentinel for {cached_non_picklable_func.__qualname__} key {ANY}: {ANY}. Result not cached."
        )
 
    # Verify no key was created in Redis
    keys_found = 0
    pattern = f"{NAMESPACE}{cached_non_picklable_func.__module__}.{cached_non_picklable_func.__qualname__}:*" # Use NAMESPACE
    logger.debug(f"Test Pickling Error: Scanning for keys matching {pattern}")
    async for key_bytes in redis_client.scan_iter(match=pattern): # Iterate over bytes
        key_str = key_bytes.decode('utf-8', errors='replace') # Decode for logging
        logger.debug(f"Found unexpected key: {key_str}")
        keys_found += 1
    assert keys_found == 0
 
    # Call again, should execute live again as nothing was cached
    # and should log the warning again
    with patch('src.cache.decorators.logger.warning') as mock_decorator_logger_warning_again:
        logger.info("Test Pickling Error: Calling again, expecting live execution")
        result2 = await cached_non_picklable_func()
        assert mock_call_count == 2
        assert callable(result2)
        mock_decorator_logger_warning_again.assert_any_call(
            f"Failed to pickle result/sentinel for {cached_non_picklable_func.__qualname__} key {ANY}: {ANY}. Result not cached."
        )
 
# TODO: Add test for concurrent hits if needed, though basic hit/miss covers atomicity implicitly for GET/SET.
# Concurrency issues are more likely in complex operations like INCR (tested in rate limit).