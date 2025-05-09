import pytest
import pytest_asyncio
from unittest.mock import patch, AsyncMock
import json

from services.kg.app.graph_schema.nodes import AstroFeatureNode
from services.kg.app.crud.astrofeature_crud import (
    upsert_astrofeature,         # Changed
    get_astrofeature_by_name_and_type, # Changed
    get_astrofeatures_by_type,
    update_astrofeature_properties, # Changed
    delete_astrofeature
)
from services.kg.app.crud.base_dao import (
    NodeNotFoundError,
    UpdateError,
    DeletionError,
    UniqueConstraintViolationError, # Should be rare with MERGE on logical key
    DAOException
)
from services.kg.app.graph_schema.constants import ASTROFEATURE_LABEL


# Sample AstroFeature Data
FEATURE_NAME = "Sun_in_Aries_Test"
FEATURE_TYPE = "planet_in_sign"
INITIAL_DETAILS_DICT = {"sign_degree": 10.5, "house": "1st", "orb": 1.0}
INITIAL_DETAILS_JSON_STR = json.dumps(INITIAL_DETAILS_DICT)

@pytest_asyncio.fixture
def sample_astrofeature_data() -> dict:
    """Raw data for DB mock return, details as JSON string."""
    return {
        "name": FEATURE_NAME,
        "feature_type": FEATURE_TYPE,
        "details": INITIAL_DETAILS_JSON_STR # Stored as JSON string in DB
    }

@pytest_asyncio.fixture
def sample_astrofeature_node(sample_astrofeature_data: dict) -> AstroFeatureNode:
    """AstroFeatureNode instance for passing to CRUD functions, details as dict."""
    # Node model expects dict for Json[Dict] field
    data_for_model = sample_astrofeature_data.copy()
    data_for_model["details"] = json.loads(sample_astrofeature_data["details"])
    return AstroFeatureNode(**data_for_model)


# --- upsert_astrofeature Tests ---
@pytest.mark.asyncio
async def test_upsert_astrofeature_create_success(mock_neo4j_session, sample_astrofeature_node: AstroFeatureNode, sample_astrofeature_data: dict):
    """Test successful creation of an AstroFeatureNode via upsert_astrofeature."""
    mock_cursor = mock_neo4j_session["cursor"]
    mock_session_obj = mock_neo4j_session["session"]
    # Access the mock_tx instance used by the session's execute_write/execute_merge
    # This assumes execute_write is called once and its first arg is the transaction lambda
    # And that lambda's __self__ attribute points to the mock_tx
    # This is a bit fragile and depends on conftest.py implementation details.
    # A cleaner way might be to have conftest yield mock_tx directly if needed for assertions.
    # For now, this is a common pattern to get to the mock_tx.run
    mock_tx = mock_session_obj.execute_write.call_args[0][0].__self__


    # Simulate MERGE returning the created node's properties (details as JSON string)
    mock_cursor.single.return_value = {"af": sample_astrofeature_data}

    created_feature = await upsert_astrofeature(sample_astrofeature_node)

    assert created_feature is not None
    assert created_feature.name == FEATURE_NAME
    assert created_feature.feature_type == FEATURE_TYPE
    assert created_feature.details == INITIAL_DETAILS_DICT # Model should parse JSON string to dict
    
    mock_session_obj.execute_write.assert_called_once()
    
    called_query = mock_tx.run.call_args[0][0]
    assert f"MERGE (af:{ASTROFEATURE_LABEL} {{name: $name, feature_type: $feature_type}})" in called_query
    assert "ON CREATE SET af = $create_props" in called_query
    assert "ON MATCH SET af += $match_props" in called_query
    
    call_params = mock_tx.run.call_args[0][1]
    assert call_params["name"] == FEATURE_NAME
    assert call_params["feature_type"] == FEATURE_TYPE
    # create_props will have details as a dict because AstroFeatureNode.model_dump() is used
    assert call_params["create_props"]["details"] == INITIAL_DETAILS_DICT
    # match_props will also have details as a dict
    assert call_params["match_props"]["details"] == INITIAL_DETAILS_DICT


@pytest.mark.asyncio
async def test_upsert_astrofeature_update_success(mock_neo4j_session, sample_astrofeature_node: AstroFeatureNode):
    mock_cursor = mock_neo4j_session["cursor"]
    mock_session_obj = mock_neo4j_session["session"]
    mock_tx = mock_neo4j_session["tx"] # Get mock_tx from the fixture yield

    updated_details_dict = {"sign_degree": 12.0, "house": "2nd", "description": "Updated via upsert", "orb": 1.5}

    # Input to upsert_astrofeature
    updated_node_input = sample_astrofeature_node.model_copy(update={"details": updated_details_dict})

    # DB mock return (details as JSON string)
    db_return_data = {
        "name": FEATURE_NAME,
        "feature_type": FEATURE_TYPE,
        "details": json.dumps(updated_details_dict)
    }
    mock_cursor.single.return_value = {"af": db_return_data}

    # --- Act ---
    updated_feature = await upsert_astrofeature(updated_node_input)

    # --- Assert ---
    assert updated_feature is not None
    assert updated_feature.name == FEATURE_NAME
    assert updated_feature.feature_type == FEATURE_TYPE
    assert updated_feature.details == updated_details_dict

    # Assert mock calls *after* the function call
    mock_session_obj.execute_write.assert_called_once()
    mock_tx.run.assert_called_once()
    call_params = mock_tx.run.call_args[0][1]
    assert call_params["match_props"]["details"] == updated_details_dict


@pytest.mark.asyncio
async def test_upsert_astrofeature_dao_exception(mock_neo4j_session, sample_astrofeature_node: AstroFeatureNode):
    mock_session_obj = mock_neo4j_session["session"]
    mock_session_obj.execute_write.side_effect = DAOException("Generic DB error for upsert astro")

    with pytest.raises(DAOException, match="Database error upserting AstroFeature"):
        await upsert_astrofeature(sample_astrofeature_node)

# --- get_astrofeature_by_name_and_type Tests ---
@pytest.mark.asyncio
async def test_get_astrofeature_by_name_and_type_found(mock_neo4j_session, sample_astrofeature_data: dict):
    mock_cursor = mock_neo4j_session["cursor"]
    mock_session_obj = mock_neo4j_session["session"]
    mock_tx = mock_neo4j_session["tx"] # Get mock_tx from the fixture yield

    mock_cursor.data.return_value = [{"af": sample_astrofeature_data}]

    # --- Act ---
    retrieved_feature = await get_astrofeature_by_name_and_type(FEATURE_NAME, FEATURE_TYPE)

    # --- Assert ---
    assert retrieved_feature is not None
    assert retrieved_feature.name == FEATURE_NAME
    assert retrieved_feature.feature_type == FEATURE_TYPE
    assert retrieved_feature.details == INITIAL_DETAILS_DICT

    # Assert mock calls *after* the function call
    mock_session_obj.execute_read.assert_called_once()
    mock_tx.run.assert_called_once()
    called_query = mock_tx.run.call_args[0][0]
    assert f"MATCH (af:{ASTROFEATURE_LABEL} {{name: $name, feature_type: $feature_type}})" in called_query

@pytest.mark.asyncio
async def test_get_astrofeature_by_name_and_type_not_found(mock_neo4j_session):
    mock_cursor = mock_neo4j_session["cursor"]
    mock_cursor.data.return_value = []

    retrieved_feature = await get_astrofeature_by_name_and_type("Non_Existent", "some_type")
    assert retrieved_feature is None

# --- get_astrofeatures_by_type Tests --- (largely unchanged but verify mock data)
@pytest.mark.asyncio
async def test_get_astrofeatures_by_type_found(mock_neo4j_session, sample_astrofeature_data: dict):
    mock_cursor = mock_neo4j_session["cursor"]
    mock_cursor.data.return_value = [{"af": sample_astrofeature_data}] # Simulating one found

    features = await get_astrofeatures_by_type(FEATURE_TYPE)
    assert len(features) == 1
    assert features[0].name == FEATURE_NAME
    assert features[0].details == INITIAL_DETAILS_DICT

# --- update_astrofeature_properties Tests ---
@pytest.mark.asyncio
async def test_update_astrofeature_properties_success(mock_neo4j_session, sample_astrofeature_data: dict):
    mock_cursor = mock_neo4j_session["cursor"]
    mock_session_obj = mock_neo4j_session["session"]
    mock_tx = mock_neo4j_session["tx"] # Get mock_tx from the fixture yield

    update_payload = {"details": {"description": "Partially Updated", "orb": 2.0}} # Completely replaces details

    # Expected DB state after update (details as JSON string)
    expected_db_data_after_update = sample_astrofeature_data.copy()
    expected_db_data_after_update["details"] = json.dumps(update_payload["details"])
    mock_cursor.single.return_value = {"af": expected_db_data_after_update}

    # --- Act ---
    updated_feature = await update_astrofeature_properties(FEATURE_NAME, FEATURE_TYPE, update_payload)

    # --- Assert ---
    assert updated_feature is not None
    assert updated_feature.details == update_payload["details"]

    # Assert mock calls *after* the function call
    mock_session_obj.execute_write.assert_called_once()
    mock_tx.run.assert_called_once()
    called_query = mock_tx.run.call_args[0][0]
    assert f"MATCH (af:{ASTROFEATURE_LABEL} {{name: $name, feature_type: $feature_type}})" in called_query
    assert "SET af += $props_to_set" in called_query
    assert mock_tx.run.call_args[0][1]["props_to_set"] == update_payload


@pytest.mark.asyncio
async def test_update_astrofeature_properties_not_found(mock_neo4j_session):
    mock_cursor = mock_neo4j_session["cursor"]
    mock_session_obj = mock_neo4j_session["session"]
    mock_cursor.single.return_value = None

    with pytest.raises(NodeNotFoundError):
        await update_astrofeature_properties("Non_Existent", "some_type", {"details": {"info": "new"}})
    mock_session_obj.execute_write.assert_called_once()

# --- delete_astrofeature Tests ---
@pytest.mark.asyncio
async def test_delete_astrofeature_success(mock_neo4j_session, sample_astrofeature_node: AstroFeatureNode): # sample_astrofeature_node has details as dict
    mock_session_obj = mock_neo4j_session["session"]
    mock_cursor = mock_neo4j_session["cursor"]
    mock_tx = mock_neo4j_session["tx"] # Get mock_tx from the fixture yield

    # Mock the internal get_astrofeature_by_name_and_type call
    with patch('services.kg.app.crud.astrofeature_crud.get_astrofeature_by_name_and_type', AsyncMock(return_value=sample_astrofeature_node)):
        mock_cursor.single.return_value = None # DELETE success

        # --- Act ---
        deleted = await delete_astrofeature(FEATURE_NAME, FEATURE_TYPE)

        # --- Assert ---
        assert deleted is True
        mock_session_obj.execute_write.assert_called_once()
        mock_tx.run.assert_called_once() # Check transaction ran
        called_delete_query = mock_tx.run.call_args[0][0]
        assert f"MATCH (af:{ASTROFEATURE_LABEL} {{name: $name, feature_type: $feature_type}})" in called_delete_query
        assert "DETACH DELETE af" in called_delete_query


@pytest.mark.asyncio
async def test_delete_astrofeature_not_found(mock_neo4j_session):
    mock_session_obj = mock_neo4j_session["session"]
    with patch('services.kg.app.crud.astrofeature_crud.get_astrofeature_by_name_and_type', AsyncMock(return_value=None)):
        deleted = await delete_astrofeature("Non_Existent", "some_type")
        assert deleted is False
        mock_session_obj.execute_write.assert_not_called()


@pytest.mark.asyncio
async def test_delete_astrofeature_dao_exception_on_delete(mock_neo4j_session, sample_astrofeature_node: AstroFeatureNode):
    mock_session_obj = mock_neo4j_session["session"]
    with patch('services.kg.app.crud.astrofeature_crud.get_astrofeature_by_name_and_type', AsyncMock(return_value=sample_astrofeature_node)):
        mock_session_obj.execute_write.side_effect = DAOException("DB error during actual delete astro")
        with pytest.raises(DeletionError, match="Database error deleting AstroFeature"):
            await delete_astrofeature(FEATURE_NAME, FEATURE_TYPE)