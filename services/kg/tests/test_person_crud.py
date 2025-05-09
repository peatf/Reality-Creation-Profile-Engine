import pytest
import pytest_asyncio
from unittest.mock import patch, AsyncMock
from datetime import datetime, timezone, timedelta

from services.kg.app.graph_schema.nodes import PersonNode
from services.kg.app.crud.person_crud import (
    upsert_person, # Changed
    get_person_by_user_id,
    update_person_properties, # Changed
    delete_person
)
from services.kg.app.crud.base_dao import (
    # NodeCreationError, # Less relevant for upsert
    NodeNotFoundError,
    UpdateError,
    DeletionError,
    UniqueConstraintViolationError, # Should not happen with MERGE on key
    DAOException
)
from services.kg.app.graph_schema.constants import PERSON_LABEL


# Sample Person Data
USER_ID = "test_user_123"
PROFILE_ID = "prof_abc"
INITIAL_NAME = "Test User"
INITIAL_LAT = 34.05
INITIAL_LON = -118.25
INITIAL_DATETIME_UTC = datetime.now(timezone.utc)

@pytest_asyncio.fixture
def sample_person_data() -> dict:
    return {
        "user_id": USER_ID,
        "profile_id": PROFILE_ID,
        "name": INITIAL_NAME,
        "birth_datetime_utc": INITIAL_DATETIME_UTC,
        "birth_latitude": INITIAL_LAT,
        "birth_longitude": INITIAL_LON
    }

@pytest_asyncio.fixture
def sample_person_node(sample_person_data: dict) -> PersonNode:
    return PersonNode(**sample_person_data)

# --- upsert_person Tests ---
@pytest.mark.asyncio
async def test_upsert_person_create_success(mock_neo4j_session, sample_person_node: PersonNode, sample_person_data: dict):
    """Test successful creation of a PersonNode via upsert_person."""
    mock_cursor = mock_neo4j_session["cursor"]
    mock_session_obj = mock_neo4j_session["session"]
    mock_tx = mock_neo4j_session["tx"] # Get mock_tx from the fixture yield

    # Simulate MERGE returning the created node's properties
    mock_cursor.single.return_value = {"p": sample_person_data}

    # --- Act ---
    created_person = await upsert_person(sample_person_node)

    # --- Assert ---
    assert created_person is not None
    assert created_person.user_id == sample_person_node.user_id
    assert created_person.name == sample_person_node.name

    # Assert mock calls *after* the function call
    mock_session_obj.execute_write.assert_called_once()
    mock_tx.run.assert_called_once()
    # Verify the query (simplified check, more detailed if needed)
    called_query = mock_tx.run.call_args[0][0]
    assert f"MERGE (p:{PERSON_LABEL} {{user_id: $user_id}})" in called_query
    assert "ON CREATE SET p = $create_props" in called_query
    assert "ON MATCH SET p += $match_props" in called_query
    
    call_params = mock_tx.run.call_args[0][1]
    assert call_params["user_id"] == USER_ID
    assert call_params["create_props"]["name"] == INITIAL_NAME
    assert call_params["match_props"]["name"] == INITIAL_NAME


@pytest.mark.asyncio
async def test_upsert_person_update_success(mock_neo4j_session, sample_person_node: PersonNode):
    """Test successful update of an existing PersonNode via upsert_person."""
    mock_cursor = mock_neo4j_session["cursor"]
    mock_session_obj = mock_neo4j_session["session"]
    mock_tx = mock_neo4j_session["tx"] # Get mock_tx from the fixture yield

    updated_name = "Updated Name via Upsert"
    updated_profile_id = "prof_xyz_upsert"
    updated_person_data_dict = sample_person_node.model_dump()
    updated_person_data_dict["name"] = updated_name
    updated_person_data_dict["profile_id"] = updated_profile_id
    
    updated_person_node_input = PersonNode(**updated_person_data_dict)

    # Simulate MERGE returning the updated node's properties
    mock_cursor.single.return_value = {"p": updated_person_data_dict}

    # --- Act ---
    updated_person = await upsert_person(updated_person_node_input)

    # --- Assert ---
    assert updated_person is not None
    assert updated_person.user_id == USER_ID
    assert updated_person.name == updated_name
    assert updated_person.profile_id == updated_profile_id

    # Assert mock calls *after* the function call
    mock_session_obj.execute_write.assert_called_once()
    mock_tx.run.assert_called_once()
    called_query = mock_tx.run.call_args[0][0]
    assert f"MERGE (p:{PERSON_LABEL} {{user_id: $user_id}})" in called_query
    
    call_params = mock_tx.run.call_args[0][1]
    assert call_params["match_props"]["name"] == updated_name
    assert call_params["match_props"]["profile_id"] == updated_profile_id


@pytest.mark.asyncio
async def test_upsert_person_dao_exception(mock_neo4j_session, sample_person_node: PersonNode):
    """Test a generic DAOException during person upsert."""
    mock_session_obj = mock_neo4j_session["session"]
    mock_session_obj.execute_write.side_effect = DAOException("Generic DB error for upsert")

    with pytest.raises(DAOException, match="Database error upserting person"):
        await upsert_person(sample_person_node)

# --- get_person_by_user_id Tests ---
@pytest.mark.asyncio
async def test_get_person_by_user_id_found(mock_neo4j_session, sample_person_data: dict):
    """Test successfully retrieving a person by user_id."""
    mock_cursor = mock_neo4j_session["cursor"]
    mock_session_obj = mock_neo4j_session["session"]
    mock_tx = mock_neo4j_session["tx"] # Get mock_tx from the fixture yield

    mock_cursor.data.return_value = [{"p": sample_person_data}]

    # --- Act ---
    retrieved_person = await get_person_by_user_id(USER_ID)

    # --- Assert ---
    assert retrieved_person is not None
    assert retrieved_person.user_id == USER_ID

    # Assert mock calls *after* the function call
    mock_session_obj.execute_read.assert_called_once()
    mock_tx.run.assert_called_once()
    called_query = mock_tx.run.call_args[0][0]
    assert f"MATCH (p:{PERSON_LABEL} {{user_id: $user_id}})" in called_query


@pytest.mark.asyncio
async def test_get_person_by_user_id_not_found(mock_neo4j_session):
    mock_cursor = mock_neo4j_session["cursor"]
    mock_cursor.data.return_value = []

    retrieved_person = await get_person_by_user_id("non_existent_user")
    assert retrieved_person is None

@pytest.mark.asyncio
async def test_get_person_dao_exception(mock_neo4j_session):
    mock_session_obj = mock_neo4j_session["session"]
    mock_session_obj.execute_read.side_effect = DAOException("DB error during get")
    
    person = await get_person_by_user_id("any_id")
    assert person is None # Current behavior is to return None on DAOException

# --- update_person_properties Tests ---
@pytest.mark.asyncio
async def test_update_person_properties_success(mock_neo4j_session, sample_person_data: dict):
    mock_cursor = mock_neo4j_session["cursor"]
    mock_session_obj = mock_neo4j_session["session"]
    mock_tx = mock_neo4j_session["tx"] # Get mock_tx from the fixture yield

    updated_name = "Updated Partially"
    update_payload = {"name": updated_name, "birth_latitude": 35.1}

    expected_node_data = sample_person_data.copy()
    expected_node_data.update(update_payload)
    mock_cursor.single.return_value = {"p": expected_node_data}

    # --- Act ---
    updated_person = await update_person_properties(USER_ID, update_payload)

    # --- Assert ---
    assert updated_person is not None
    assert updated_person.name == updated_name
    assert updated_person.birth_latitude == 35.1

    # Assert mock calls *after* the function call
    mock_session_obj.execute_write.assert_called_once()
    mock_tx.run.assert_called_once()
    called_query = mock_tx.run.call_args[0][0]
    assert f"MATCH (p:{PERSON_LABEL} {{user_id: $user_id}})" in called_query
    assert "SET p += $props_to_set" in called_query
    assert mock_tx.run.call_args[0][1]["props_to_set"] == update_payload

@pytest.mark.asyncio
async def test_update_person_properties_not_found(mock_neo4j_session):
    mock_cursor = mock_neo4j_session["cursor"]
    mock_session_obj = mock_neo4j_session["session"]
    mock_cursor.single.return_value = None # Simulate MATCH not finding the node

    with pytest.raises(NodeNotFoundError, match=f"Person with user_id 'non_existent_user' not found for update."):
        await update_person_properties("non_existent_user", {"name": "New Name"})
    mock_session_obj.execute_write.assert_called_once()


@pytest.mark.asyncio
async def test_update_person_properties_empty_payload(mock_neo4j_session, sample_person_node: PersonNode, sample_person_data: dict):
    """Test update_person_properties with an empty update payload."""
    mock_cursor = mock_neo4j_session["cursor"] # For the get_person_by_user_id call
    mock_cursor.data.return_value = [{"p": sample_person_data}]

    # Patch get_person_by_user_id as it's called internally by update_person_properties when payload is empty
    with patch('services.kg.app.crud.person_crud.get_person_by_user_id', AsyncMock(return_value=sample_person_node)) as mock_get:
        updated_person = await update_person_properties(USER_ID, {})
        mock_get.assert_called_once_with(USER_ID)
        assert updated_person == sample_person_node
    mock_neo4j_session["session"].execute_write.assert_not_called()


@pytest.mark.asyncio
async def test_update_person_properties_dao_exception(mock_neo4j_session):
    mock_session_obj = mock_neo4j_session["session"]
    mock_session_obj.execute_write.side_effect = DAOException("DB error during partial update")

    with pytest.raises(UpdateError, match="Database error updating person properties"):
        await update_person_properties(USER_ID, {"name": "Some Name"})

# --- delete_person Tests ---
@pytest.mark.asyncio
async def test_delete_person_success(mock_neo4j_session, sample_person_node: PersonNode):
    mock_session_obj = mock_neo4j_session["session"]
    mock_cursor = mock_neo4j_session["cursor"] # For the execute_write call in delete

    # Mock the internal get_person_by_user_id call within delete_person
    with patch('services.kg.app.crud.person_crud.get_person_by_user_id', AsyncMock(return_value=sample_person_node)) as mock_get:
        mock_cursor.single.return_value = None # DELETE doesn't typically return the node

        deleted = await delete_person(USER_ID)
        assert deleted is True
        mock_get.assert_called_once_with(USER_ID)
        mock_session_obj.execute_write.assert_called_once()
        # Verify query for delete
        mock_tx = mock_neo4j_session["tx"] # Get mock_tx from the fixture yield
        mock_tx.run.assert_called_once() # Check transaction ran
        called_delete_query = mock_tx.run.call_args[0][0]
        assert f"MATCH (p:{PERSON_LABEL} {{user_id: $user_id}})" in called_delete_query
        assert "DETACH DELETE p" in called_delete_query


@pytest.mark.asyncio
async def test_delete_person_not_found(mock_neo4j_session):
    mock_session_obj = mock_neo4j_session["session"]
    with patch('services.kg.app.crud.person_crud.get_person_by_user_id', AsyncMock(return_value=None)) as mock_get:
        deleted = await delete_person("non_existent_user_for_delete")
        assert deleted is False
        mock_get.assert_called_once_with("non_existent_user_for_delete")
        mock_session_obj.execute_write.assert_not_called()


@pytest.mark.asyncio
async def test_delete_person_dao_exception_on_get(mock_neo4j_session):
    """Test DAOException during the initial get in delete_person."""
    with patch('services.kg.app.crud.person_crud.get_person_by_user_id', AsyncMock(side_effect=DAOException("Error fetching before delete"))) as mock_get:
        with pytest.raises(DAOException, match="Error fetching before delete"): # Match the original error from get
            await delete_person(USER_ID)
        mock_get.assert_called_once_with(USER_ID)


@pytest.mark.asyncio
async def test_delete_person_dao_exception_on_delete(mock_neo4j_session, sample_person_node: PersonNode):
    mock_session_obj = mock_neo4j_session["session"]
    with patch('services.kg.app.crud.person_crud.get_person_by_user_id', AsyncMock(return_value=sample_person_node)):
        mock_session_obj.execute_write.side_effect = DAOException("DB error during actual delete")
        with pytest.raises(DeletionError, match="Database error deleting person"):
            await delete_person(USER_ID)