import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from typing import AsyncGenerator, Generator
import os

# Set REDIS_URL for tests to connect to the host-mapped port
os.environ["REDIS_URL"] = "redis://localhost:6380/0"

# Adjust the import path based on how you run pytest.
# If running pytest from `services/chart_calc/`:
# from app.main import app # Original import
# If running pytest from the root of the project, you might need:
from services.chart_calc.app.main import app # Using explicit path from root

# To make Redis and Kafka optional for tests or use mocks:
# We can mock them here or within specific test files.
# For now, the app will try to connect; tests for cache/kafka will need running services or mocks.

# pytest-asyncio provides its own event_loop fixture.
# Defining a custom one like below can cause conflicts if not done carefully.
# It's generally recommended to let pytest-asyncio manage the loop unless
# a very specific custom loop policy or setup is required for the entire session.
# The previous `pass` here was likely causing the NoneType error for the event loop.
#
# @pytest_asyncio.fixture(scope="session")
# async def event_loop():
#     """
#     Creates an event loop for the session.
#     pytest-asyncio provides this by default if not defined.
#     Explicitly defining can sometimes help with complex setups.
#     """
#     # loop = asyncio.get_event_loop_policy().new_event_loop()
#     # yield loop
#     # loop.close()
#     # Simpler: pytest-asyncio handles this. If issues arise, uncomment above.
#     pass # Rely on pytest-asyncio's default event loop management

@pytest_asyncio.fixture(scope="function") # "function" scope for client to ensure clean state per test
async def test_client() -> AsyncGenerator[AsyncClient, None]:
    """
    Provides an asynchronous test client for the FastAPI application.
    """
    # The app's startup/shutdown events (Redis/Kafka connections) will be triggered here.
    # Ensure Redis/Kafka are available or properly mocked if tests depending on them are run.
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://testserver") as client:
        yield client
    # Shutdown events are handled by AsyncClient context manager based on app's lifespan.

# --- Mocking Fixtures (Example for Kafka/Redis if not running services) ---

# @pytest.fixture(scope="session")
# def mock_redis_client(mocker):
#     """Mocks the Redis client for tests not requiring actual Redis."""
#     mock = mocker.patch("app.core.cache.get_redis_client")
#     mock_instance = mocker.AsyncMock(spec=aioredis.Redis)
#     mock_instance.ping.return_value = True
#     mock_instance.get.return_value = None # Default to cache miss
#     mock_instance.set.return_value = True
#     mock_instance.delete.return_value = 1
#     # Configure other methods as needed
#     mock.return_value = mock_instance
#     return mock_instance

# @pytest.fixture(scope="session")
# def mock_kafka_producer(mocker):
#     """Mocks the Kafka producer."""
#     mock = mocker.patch("app.core.kafka_producer.get_kafka_producer")
#     mock_instance = mocker.AsyncMock(spec=AIOKafkaProducer)
#     mock_instance.start.return_value = None
#     mock_instance.stop.return_value = None
#     mock_instance.send_and_wait.return_value = None # Simulate successful send
#     mock.return_value = mock_instance
#     return mock_instance

# To use these mocks, a test function can depend on them:
# async def test_something_with_mocked_redis(test_client, mock_redis_client):
#     ...

# For now, tests will assume services are available or will be skipped/fail if not.
# Specific test files can implement more targeted mocks if needed.