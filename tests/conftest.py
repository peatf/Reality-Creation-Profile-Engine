import importlib
import os # Import os module
import pytest_asyncio
import asyncio
import pytest
import src.cache.connection as conn # Use alias for brevity
import src.cache.decorators as decorators # Import decorators module
from neo4j import AsyncGraphDatabase, AsyncDriver
import httpx # For API client

# Removed import of tests.cache.test_cache

# Create a session-scoped event loop for session-scoped fixtures
@pytest_asyncio.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for each test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest_asyncio.fixture(scope="session", autouse=True)
async def redis_session():
    """
    Session-scoped fixture to set REDIS_URL, ensure connection/decorator/test
    modules are reloaded in the correct order for tests.
    """
    # Explicitly set REDIS_URL for the test session environment
    os.environ["REDIS_URL"] = "redis://redis-test:6379/0"

    # ensure fresh connection + decorators
    await conn.close_redis() # Close any existing pool from initial imports
    importlib.reload(conn) # Reload connection module
    importlib.reload(decorators) # Reload decorators module

    # Removed reloading of the test module itself

    yield # Allow session tests to run
    # Cleanup after session: close the connection pool and reset the cached handle
    await conn.close_redis()


# --- API Test Client Fixture ---
@pytest_asyncio.fixture(scope="session")
async def api_client():
    """
    Provides an HTTPX client for making requests to the main FastAPI application
    running in the Docker E2E environment.
    """
    # Assumes the main API is running at http://main-api:8000 as per typical docker-compose setup
    # The service name 'api' should match what's in docker-compose.yml
    async with httpx.AsyncClient(base_url="http://api:8000", timeout=30.0) as client:
        # Wait for the API to be available - simple health check
        for _ in range(10): # Retry a few times
            try:
                # The 'api' service healthcheck in docker-compose.yml uses /health
                response = await client.get("/health") # Target the /health endpoint
                response.raise_for_status()
                print("API client connected to api service successfully.")
                break
            except (httpx.ConnectError, httpx.HTTPStatusError) as e:
                print(f"API client waiting for api service: {e}")
                await asyncio.sleep(3)
        else:
            pytest.fail("API client could not connect to api service at http://api:8000 after multiple retries.")
        yield client


# --- Neo4j Driver for E2E Tests ---
@pytest_asyncio.fixture(scope="session")
async def e2e_neo4j_driver():
    """
    Provides an AsyncDriver connected to the Neo4j instance used in the
    full Docker E2E environment (defined in infra/docker/docker-compose.yml).
    """
    # These should match the environment variables or defaults in infra/docker/docker-compose.yml for the neo4j service
    NEO4J_E2E_URI = os.getenv("NEO4J_E2E_URI", "bolt://neo4j:7687") # Service name 'neo4j' from docker-compose
    NEO4J_E2E_USER = os.getenv("NEO4J_E2E_USER", "neo4j")
    NEO4J_E2E_PASSWORD = os.getenv("NEO4J_E2E_PASSWORD", "testpassword") # Ensure this matches docker-compose

    driver = AsyncGraphDatabase.driver(NEO4J_E2E_URI, auth=(NEO4J_E2E_USER, NEO4J_E2E_PASSWORD))
    try:
        # Wait for Neo4j to be available
        for _ in range(10): # Retry a few times
            try:
                await driver.verify_connectivity()
                print(f"E2E Neo4j driver connected to {NEO4J_E2E_URI} successfully.")
                break
            except Exception as e:
                print(f"E2E Neo4j driver waiting for connection to {NEO4J_E2E_URI}: {e}")
                await asyncio.sleep(3)
        else:
            pytest.fail(f"E2E Neo4j driver could not connect to {NEO4J_E2E_URI} after multiple retries.")
        
        yield driver
    finally:
        await driver.close()
        print("E2E Neo4j driver closed.")