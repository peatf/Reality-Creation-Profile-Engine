import pytest
import pytest_asyncio
from unittest.mock import MagicMock, patch, AsyncMock # Add patch, AsyncMock
from datetime import datetime, timezone

from services.kg.app.graph_schema.nodes import PersonNode
from services.kg.app.crud.person_crud import (
    create_person,
    get_person_by_user_id,
    update_person,
    delete_person
)
from services.kg.app.crud.base_dao import (
    NodeCreationError,
    NodeNotFoundError,
    UpdateError,
    DeletionError,
    UniqueConstraintViolationError,
    DAOException
)

# Sample Person Data
SAMPLE_PERSON_DATA = {
    "user_id": "test_user_123",
    "profile_id": "prof_abc",
    "name": "Test User",
    "birth_datetime_utc": datetime.now(timezone.utc),
    "birth_latitude": 34.05,
    "birth_longitude": -118.25
}
SAMPLE_PERSON_NODE = PersonNode(**SAMPLE_PERSON_DATA)

@pytest_asyncio.fixture
async def sample_person_node_fixture():
    return PersonNode(**SAMPLE_PERSON_DATA)

@pytest.mark.asyncio
async def test_create_person_success(mock_neo4j_driver, sample_person_node_fixture: PersonNode):
    """Test successful creation of a PersonNode."""
    mock_tx_run_result = mock_neo4j_driver["tx_run_result"]
    # Simulate the node properties being returned after creation
    mock_tx_run_result.single.return_value = {"p": SAMPLE_PERSON_DATA}

    created_person = await create_person(sample_person_node_fixture)

    assert created_person is not None
    assert created_person.user_id == sample_person_node_fixture.user_id
    assert created_person.name == sample_person_node_fixture.name
    
    # Check that execute_write was called (implicitly via mock_transaction_fn in conftest)
    mock_neo4j_driver["session"].execute_write.assert_called_once()
    # Further inspect the query if needed by accessing call_args of mock_tx.run

@pytest.mark.asyncio
async def test_create_person_already_exists(mock_neo4j_driver, sample_person_node_fixture: PersonNode):
    """Test creating a person that already exists (UniqueConstraintViolation)."""
    mock_session = mock_neo4j_driver["session"]
    # Simulate UniqueConstraintViolationError from the driver/base_dao
    mock_session.execute_write.side_effect = UniqueConstraintViolationError("Person already exists")

    with pytest.raises(NodeCreationError, match="already exists"):
        await create_person(sample_person_node_fixture)

@pytest.mark.asyncio
async def test_create_person_dao_exception(mock_neo4j_driver, sample_person_node_fixture: PersonNode):
    """Test a generic DAOException during person creation."""
    mock_session = mock_neo4j_driver["session"]
    mock_session.execute_write.side_effect = DAOException("Generic DB error")

    with pytest.raises(NodeCreationError, match="Database error creating person"):
        await create_person(sample_person_node_fixture)


@pytest.mark.asyncio
async def test_get_person_by_user_id_found(mock_neo4j_driver, sample_person_node_fixture: PersonNode):
    """Test successfully retrieving a person by user_id."""
    mock_tx_run_result = mock_neo4j_driver["tx_run_result"]
    # Simulate the node properties being returned
    mock_tx_run_result.data.return_value = [{"p": SAMPLE_PERSON_DATA}]

    retrieved_person = await get_person_by_user_id(sample_person_node_fixture.user_id)

    assert retrieved_person is not None
    assert retrieved_person.user_id == sample_person_node_fixture.user_id
    mock_neo4j_driver["session"].execute_read.assert_called_once()

@pytest.mark.asyncio
async def test_get_person_by_user_id_not_found(mock_neo4j_driver):
    """Test retrieving a person that does not exist."""
    mock_tx_run_result = mock_neo4j_driver["tx_run_result"]
    mock_tx_run_result.data.return_value = [] # No records found

    retrieved_person = await get_person_by_user_id("non_existent_user")
    assert retrieved_person is None

@pytest.mark.asyncio
async def test_get_person_dao_exception(mock_neo4j_driver):
    """Test DAOException during get_person."""
    mock_session = mock_neo4j_driver["session"]
    mock_session.execute_read.side_effect = DAOException("DB error during get")
    
    # The current get_person_by_user_id returns None on DAOException
    person = await get_person_by_user_id("any_id")
    assert person is None


@pytest.mark.asyncio
async def test_update_person_success(mock_neo4j_driver, sample_person_node_fixture: PersonNode):
    """Test successful update of a person."""
    mock_tx_run_result = mock_neo4j_driver["tx_run_result"]
    updated_name = "Updated Test User"
    updated_data_payload = {"name": updated_name, "birth_latitude": 35.0}
    
    # Simulate the updated node properties being returned
    expected_updated_node_data = SAMPLE_PERSON_DATA.copy()
    expected_updated_node_data["name"] = updated_name
    expected_updated_node_data["birth_latitude"] = 35.0
    mock_tx_run_result.single.return_value = {"p": expected_updated_node_data}

    updated_person = await update_person(sample_person_node_fixture.user_id, updated_data_payload)

    assert updated_person is not None
    assert updated_person.name == updated_name
    assert updated_person.birth_latitude == 35.0
    mock_neo4j_driver["session"].execute_write.assert_called_once()

@pytest.mark.asyncio
async def test_update_person_not_found(mock_neo4j_driver):
    """Test updating a person that does not exist."""
    mock_tx_run_result = mock_neo4j_driver["tx_run_result"]
    # First, execute_write for update returns None (node not found by MATCH)
    mock_tx_run_result.single.return_value = None 
    # Then, the subsequent get_person_by_user_id (to check existence) also returns None
    
    # To make get_person_by_user_id return None, we need to adjust the side_effect of execute_read
    # This is a bit tricky as execute_read is used by get_person_by_user_id
    # We can mock the get_person_by_user_id directly for this specific test path in update_person
    
    with patch('services.kg.app.crud.person_crud.get_person_by_user_id', AsyncMock(return_value=None)) as mock_get_person:
        with pytest.raises(NodeNotFoundError):
            await update_person("non_existent_user_for_update", {"name": "New Name"})
        mock_get_person.assert_called_once_with("non_existent_user_for_update")


@pytest.mark.asyncio
async def test_update_person_dao_exception(mock_neo4j_driver):
    """Test DAOException during person update."""
    mock_session = mock_neo4j_driver["session"]
    mock_session.execute_write.side_effect = DAOException("DB error during update")

    with pytest.raises(UpdateError, match="Database error updating person"):
        await update_person("any_user_id", {"name": "Some Name"})


@pytest.mark.asyncio
async def test_delete_person_success(mock_neo4j_driver, sample_person_node_fixture: PersonNode):
    """Test successful deletion of a person."""
    # Mock get_person_by_user_id to simulate node exists before deletion
    with patch('services.kg.app.crud.person_crud.get_person_by_user_id', AsyncMock(return_value=sample_person_node_fixture)):
        # execute_write for DELETE doesn't need to return specific data, just succeed
        mock_neo4j_driver["tx_run_result"].single.return_value = None # Or some summary

        deleted = await delete_person(sample_person_node_fixture.user_id)
        assert deleted is True
        mock_neo4j_driver["session"].execute_write.assert_called_once()

@pytest.mark.asyncio
async def test_delete_person_not_found(mock_neo4j_driver):
    """Test deleting a person that does not exist."""
    # Mock get_person_by_user_id to simulate node does not exist
    with patch('services.kg.app.crud.person_crud.get_person_by_user_id', AsyncMock(return_value=None)):
        deleted = await delete_person("non_existent_user_for_delete")
        assert deleted is False
        # execute_write should not be called if node not found by the initial check
        mock_neo4j_driver["session"].execute_write.assert_not_called()


@pytest.mark.asyncio
async def test_delete_person_dao_exception(mock_neo4j_driver, sample_person_node_fixture: PersonNode):
    """Test DAOException during person deletion."""
    # Mock get_person_by_user_id to simulate node exists
    with patch('services.kg.app.crud.person_crud.get_person_by_user_id', AsyncMock(return_value=sample_person_node_fixture)):
        mock_session = mock_neo4j_driver["session"]
        mock_session.execute_write.side_effect = DAOException("DB error during delete")

        with pytest.raises(DeletionError, match="Database error deleting person"):
            await delete_person(sample_person_node_fixture.user_id)