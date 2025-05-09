import pytest
import asyncio
import json
from httpx import AsyncClient
from fastapi import status

# Assuming conftest.py and app setup are correct.
# Test client from conftest.py will trigger app startup/shutdown, initializing Redis.
from ..app.core.cache import get_from_cache, set_to_cache, invalidate_cache_key, DEFAULT_CACHE_TTL_SECONDS, get_redis_client # Use relative import
from ..app.main import CalculationRequest # Use relative import

# Fixture to ensure Redis is available for these tests
@pytest.fixture(scope="function", autouse=True) # Changed scope to function
async def ensure_redis_is_available():
    redis = await get_redis_client()
    if not redis:
        pytest.skip("Redis is not available, skipping cache tests.")
    try:
        await redis.ping()
    except Exception:
        pytest.skip("Redis ping failed, skipping cache tests.")

@pytest.mark.asyncio
async def test_set_and_get_cache(test_client: AsyncClient): # test_client fixture to ensure app (and redis) startup
    """Test basic set and get from cache."""
    key = "test_cache:mykey1"
    value = {"data": "some test data", "number": 123}
    
    await invalidate_cache_key(key) # Clean before test

    await set_to_cache(key, json.dumps(value), expire_seconds=60)
    retrieved_value_str = await get_from_cache(key)
    
    assert retrieved_value_str is not None
    retrieved_value = json.loads(retrieved_value_str)
    assert retrieved_value == value

    await invalidate_cache_key(key) # Clean after test

@pytest.mark.asyncio
async def test_cache_miss(test_client: AsyncClient):
    """Test cache miss returns None."""
    key = "test_cache:nonexistentkey"
    await invalidate_cache_key(key) # Ensure it's not there
    
    retrieved_value = await get_from_cache(key)
    assert retrieved_value is None

@pytest.mark.asyncio
async def test_cache_ttl_expiration(test_client: AsyncClient):
    """Test that a cache key expires after its TTL."""
    key = "test_cache:ttl_key"
    value = "temporary data"
    ttl_short = 1  # 1 second TTL for quick testing

    await invalidate_cache_key(key)

    await set_to_cache(key, value, expire_seconds=ttl_short)
    
    # Check it's there immediately
    retrieved_immediately = await get_from_cache(key)
    assert retrieved_immediately == value
    
    # Wait for TTL to expire
    await asyncio.sleep(ttl_short + 0.5) # Sleep a bit longer than TTL
    
    retrieved_after_ttl = await get_from_cache(key)
    assert retrieved_after_ttl is None

@pytest.mark.asyncio
async def test_cache_invalidation(test_client: AsyncClient):
    """Test explicit cache key invalidation."""
    key = "test_cache:invalidate_me"
    value = "to be deleted"
    
    await set_to_cache(key, value, expire_seconds=60)
    assert await get_from_cache(key) == value # Verify it's set
    
    deleted = await invalidate_cache_key(key)
    assert deleted is True
    assert await get_from_cache(key) is None

    # Test deleting a non-existent key
    deleted_non_existent = await invalidate_cache_key("test_cache:does_not_exist_anyway")
    assert deleted_non_existent is False


@pytest.mark.asyncio
async def test_astrology_endpoint_uses_cache(test_client: AsyncClient, mocker):
    """
    Test that the astrology endpoint uses the cache.
    This involves mocking the actual calculation to see if cache is hit.
    """
    payload = {"birth_date": "2024-01-01", "birth_time": "12:00:00", "latitude": 0.0, "longitude": 0.0}
    request_obj = CalculationRequest(**payload)
    # For Pydantic V2, dump to dict first, then use json.dumps for sorting
    request_dict = request_obj.model_dump()
    cache_key = f"astrology:{json.dumps(request_dict, sort_keys=True)}"

    # Mock the underlying calculation service to ensure it's not called on cache hit
    # Import the actual response model
    from app.schemas.astrology_schemas import AstrologyChartResponse, PlanetaryPosition # Add PlanetaryPosition if north_node is one

    mocked_calc_service = mocker.patch(
        "app.main.calculate_astrological_chart",
        autospec=True # Ensures the mock has the same signature as the original
    )
    
    # Define a dummy response for the mocked service for the first call (cache miss)
    # It must be an instance of AstrologyChartResponse
    dummy_response_model = AstrologyChartResponse(
        request_data=payload,
        planetary_positions=[],
        house_cusps=[],
        aspects=[],
        north_node=None # Or a PlanetaryPosition instance if schema expects that for north_node
    )
    mocked_calc_service.return_value = dummy_response_model
    
    # Ensure cache is clean for this key
    await invalidate_cache_key(cache_key)

    # 1. First call (cache miss) - calculation service should be called
    response1 = await test_client.post("/calculate/astrology", json=payload)
    assert response1.status_code == status.HTTP_200_OK
    mocked_calc_service.assert_called_once() # Service was called

    # Verify the data was cached (by checking Redis directly or by assuming set_to_cache worked)
    cached_data_str = await get_from_cache(cache_key)
    assert cached_data_str is not None 
    # We expect the response from the first call to be what's cached.
    # The cache stores the JSON string of the Pydantic model.
    assert json.loads(cached_data_str) == response1.json()


    # Reset mock for the second call
    mocked_calc_service.reset_mock()

    # 2. Second call (cache hit) - calculation service should NOT be called
    response2 = await test_client.post("/calculate/astrology", json=payload)
    assert response2.status_code == status.HTTP_200_OK
    mocked_calc_service.assert_not_called() # Service was NOT called

    assert response1.json() == response2.json() # Response should be the same

    # Clean up
    await invalidate_cache_key(cache_key)
    mocker.stopall() # Stop all mocks started by this test function

# Similar test for /calculate/human_design can be added: test_human_design_endpoint_uses_cache