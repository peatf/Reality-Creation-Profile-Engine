import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock # Keep MagicMock just in case, though AsyncMock preferred for async methods
import services.kg.app.crud.base_dao as base_dao # Target module for patching

# Remove the previous mock_neo4j_driver fixture if it exists

@pytest.fixture(autouse=True)
def mock_neo4j_session(monkeypatch):
    """Mocks the async DB session using monkeypatch as per user suggestion."""

    # 1) Create the fake session object that the context manager yields
    # Use AsyncMock as its methods (execute_read/write) are awaited
    fake_session = AsyncMock()

    # Mock the result cursor returned by tx.run()
    mock_cursor = AsyncMock()
    mock_cursor.data = AsyncMock(return_value=[])
    mock_cursor.single = AsyncMock(return_value=None)
    mock_cursor.consume = AsyncMock()

    # Mock the transaction object passed to execute_read/write lambdas
    mock_tx = AsyncMock()
    mock_tx.run = AsyncMock(return_value=mock_cursor)

    # Configure the side effects for execute_read/write to simulate transaction execution
    async def execute_read_side_effect(tx_lambda, *args, **kwargs):
        return await tx_lambda(mock_tx)
    async def execute_write_side_effect(tx_lambda, *args, **kwargs):
        return await tx_lambda(mock_tx)

    fake_session.execute_read = AsyncMock(side_effect=execute_read_side_effect)
    fake_session.execute_write = AsyncMock(side_effect=execute_write_side_effect)
    fake_session.close = AsyncMock() # Mock close if it's called

    # 2) Create an async context‐manager mock whose __aenter__ yields that session
    # Use AsyncMock for the context manager itself
    fake_cm = AsyncMock()
    async def mock_aenter(*args, **kwargs):
        return fake_session # __aenter__ returns the session mock
    async def mock_aexit(*args, **kwargs):
        return None # __aexit__ returns None
    fake_cm.__aenter__ = mock_aenter
    fake_cm.__aexit__ = mock_aexit

    # 3) Monkey‐patch get_async_db_session to be an async function returning our CM
    async def fake_get_async_db_session(*args, **kwargs): # Accept arbitrary args
        return fake_cm # Return the context manager mock

    monkeypatch.setattr(
        base_dao, # Patch where it's imported/used
        "get_async_db_session",
        fake_get_async_db_session
    )

    # Patch the driver getter as well
    mock_driver_instance = AsyncMock()
    mock_driver_instance.verify_connectivity = AsyncMock()
    mock_driver_instance.close = AsyncMock()
    # Patch the class method directly
    monkeypatch.setattr(
        'services.kg.app.core.db.Neo4jDatabase.get_async_driver',
        AsyncMock(return_value=mock_driver_instance)
    )

    # Yield mocks needed by tests (optional)
    yield {
        "session": fake_session,
        "cursor": mock_cursor, # Renamed from tx_run_result for clarity
        "context_manager": fake_cm,
        "driver": mock_driver_instance # Added driver mock just in case
    }

# Keep commented out event loop fixture
# @pytest.fixture(scope="session")
# def event_loop(scope="session"):
#     """
#     Creates an explicit asyncio event loop for the test session.
#     This helps avoid issues with pytest-asyncio's default loop management in some environments.
#     """
#     import asyncio
#     policy = asyncio.get_event_loop_policy()
#     loop = policy.new_event_loop()
#     yield loop
#     loop.close()

# You can add more shared fixtures here, e.g., sample data for nodes/relationships.