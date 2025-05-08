import importlib
import os # Import os module
import pytest_asyncio
import asyncio
import pytest
import src.cache.connection as conn # Use alias for brevity
import src.cache.decorators as decorators # Import decorators module
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