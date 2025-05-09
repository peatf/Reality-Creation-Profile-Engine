import pytest
import pytest_asyncio
from unittest.mock import patch, AsyncMock
import json

from services.kg.app.graph_schema.nodes import HDFeatureNode
from services.kg.app.crud.hdfeature_crud import (
    upsert_hdfeature,           # Changed
    get_hdfeature_by_name_and_type, # Changed
    get_hdfeatures_by_type,
    update_hdfeature_properties, # Changed
    delete_hdfeature
)
from services.kg.app.crud.base_dao import (
    NodeNotFoundError,
    UpdateError,
    DeletionError,
    UniqueConstraintViolationError, # Should be rare with MERGE on logical key
    DAOException
)
from services.kg.app.graph_schema.constants import HDFEATURE_LABEL


# Sample HDFeature Data
FEATURE_NAME = "Defined_Throat_Test"
FEATURE_TYPE = "center"
INITIAL_DETAILS_DICT = {"state": "Defined", "function": "Manifestation, communication"}
INITIAL_DETAILS_JSON_STR = json.dumps(INITIAL_DETAILS_DICT)

@pytest_asyncio.fixture
def sample_hdfeature_data() -> dict:
    """Raw data for DB mock return, details as JSON string."""
    return {
        "name": FEATURE_NAME,
        "feature_type": FEATURE_TYPE,
        "details": INITIAL_DETAILS_JSON_STR # Stored as JSON string in DB
    }

@pytest_asyncio.fixture
def sample_hdfeature_node(sample_hdfeature_data: dict) -> HDFeatureNode:
    """HDFeatureNode instance for passing to CRUD functions, details as dict."""
    data_for_model = sample_hdfeature_data.copy()
    data_for_model["details"] = json.loads(sample_hdfeature_data["details"])
    return HDFeatureNode(**data_for_model)


# --- upsert_hdfeature Tests ---
@pytest.mark.asyncio
async def test_upsert_hdfeature_create_success(mock_neo4j_session, sample_hdfeature_node: HDFeatureNode, sample_hdfeature_data: dict):
    """Test successful creation of an HDFeatureNode via upsert_hdfeature."""
    mock_cursor = mock_neo4j_session["cursor"]
    mock_session_obj = mock_neo4j_session["session"]
    mock_tx = mock_session_obj.execute_write.call_args[0][0].__self__

    mock_cursor.single.return_value = {"hf": sample_hdfeature_data}

    created_feature = await upsert_hdfeature(sample_hdfeature_node)

    assert created_feature is not None
    assert created_feature.name == FEATURE_NAME
    assert created_feature.feature_type == FEATURE_TYPE
    assert created_feature.details == INITIAL_DETAILS_DICT
    
    mock_session_obj.execute_write.assert_called_once()
    
    called_query = mock_tx.run.call_args[0][0]
    assert f"MERGE (hf:{HDFEATURE_LABEL} {{name: $name, feature_type: $feature_type}})" in called_query
    assert "ON CREATE SET hf = $create_props" in called_query
    assert "ON MATCH SET hf += $match_props" in called_query
    
    call_params = mock_tx.run.call_args[0][1]
    assert call_params["name"] == FEATURE_NAME
    assert call_params["feature_type"] == FEATURE_TYPE
    assert call_params["create_props"]["details"] == INITIAL_DETAILS_DICT
    assert call_params["match_props"]["details"] == INITIAL_DETAILS_DICT


@pytest.mark.asyncio
async def test_upsert_hdfeature_update_success(mock_neo4j_session, sample_hdfeature_node: HDFeatureNode):
    mock_cursor = mock_neo4j_session["cursor"]
    mock_session_obj = mock_neo4j_session["session"]
    mock_tx = mock_session_obj.execute_write.call_args[0][0].__self__

    updated_details_dict = {"state": "Defined", "function": "Manifestation & Metamorphosis", "voice": "I speak"}
    
    updated_node_input = sample_hdfeature_node.model_copy(update={"details": updated_details_dict})

    db_return_data = {
        "name": FEATURE_NAME,
        "feature_type": FEATURE_TYPE,
        "details": json.dumps(updated_details_dict)
    }
    mock_cursor.single.return_value = {"hf": db_return_data}

    # --- Act ---
    updated_feature = await upsert_hdfeature(updated_node_input)

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
async def test_upsert_hdfeature_dao_exception(mock_neo4j_session, sample_hdfeature_node: HDFeatureNode):
    mock_session_obj = mock_neo4j_session["session"]
    mock_session_obj.execute_write.side_effect = DAOException("Generic DB error for upsert hd")

    with pytest.raises(DAOException, match="Database error upserting HDFeature"):
        await upsert_hdfeature(sample_hdfeature_node)

# --- get_hdfeature_by_name_and_type Tests ---
@pytest.mark.asyncio
async def test_get_hdfeature_by_name_and_type_found(mock_neo4j_session, sample_hdfeature_data: dict):
    mock_cursor = mock_neo4j_session["cursor"]
    mock_session_obj = mock_neo4j_session["session"]
    mock_tx = mock_neo4j_session["tx"] # Get mock_tx from the fixture yield

    mock_cursor.data.return_value = [{"hf": sample_hdfeature_data}]

    # --- Act ---
    retrieved_feature = await get_hdfeature_by_name_and_type(FEATURE_NAME, FEATURE_TYPE)

    # --- Assert ---
    assert retrieved_feature is not None
    assert retrieved_feature.name == FEATURE_NAME
    assert retrieved_feature.feature_type == FEATURE_TYPE
    assert retrieved_feature.details == INITIAL_DETAILS_DICT

    # Assert mock calls *after* the function call
    mock_session_obj.execute_read.assert_called_once()
    mock_tx.run.assert_called_once()
    called_query = mock_tx.run.call_args[0][0]
    assert f"MATCH (hf:{HDFEATURE_LABEL} {{name: $name, feature_type: $feature_type}})" in called_query

@pytest.mark.asyncio
async def test_get_hdfeature_by_name_and_type_not_found(mock_neo4j_session):
    mock_cursor = mock_neo4j_session["cursor"]
    mock_cursor.data.return_value = []

    retrieved_feature = await get_hdfeature_by_name_and_type("Non_Existent", "some_type")
    assert retrieved_feature is None

# --- get_hdfeatures_by_type Tests ---
@pytest.mark.asyncio
async def test_get_hdfeatures_by_type_found(mock_neo4j_session, sample_hdfeature_data: dict):
    mock_cursor = mock_neo4j_session["cursor"]
    mock_cursor.data.return_value = [{"hf": sample_hdfeature_data}]

    features = await get_hdfeatures_by_type(FEATURE_TYPE)
    assert len(features) == 1
    assert features[0].name == FEATURE_NAME
    assert features[0].details == INITIAL_DETAILS_DICT

# --- update_hdfeature_properties Tests ---
@pytest.mark.asyncio
async def test_update_hdfeature_properties_success(mock_neo4j_session, sample_hdfeature_data: dict):
    mock_cursor = mock_neo4j_session["cursor"]
    mock_session_obj = mock_neo4j_session["session"]
    mock_tx = mock_neo4j_session["tx"] # Get mock_tx from the fixture yield

    update_payload = {"details": {"function": "Partially Updated Function", "extra": True}}

    expected_db_data_after_update = sample_hdfeature_data.copy()
    expected_db_data_after_update["details"] = json.dumps(update_payload["details"])
    mock_cursor.single.return_value = {"hf": expected_db_data_after_update}

    # --- Act ---
    updated_feature = await update_hdfeature_properties(FEATURE_NAME, FEATURE_TYPE, update_payload)

    # --- Assert ---
    assert updated_feature is not None
    assert updated_feature.details == update_payload["details"]

    # Assert mock calls *after* the function call
    mock_session_obj.execute_write.assert_called_once()
    mock_tx.run.assert_called_once() # Check transaction ran
    called_query = mock_tx.run.call_args[0][0]
    assert f"MATCH (hf:{HDFEATURE_LABEL} {{name: $name, feature_type: $feature_type}})" in called_query
    assert "SET hf += $props_to_set" in called_query
    assert mock_tx.run.call_args[0][1]["props_to_set"] == update_payload


@pytest.mark.asyncio
async def test_update_hdfeature_properties_not_found(mock_neo4j_session):
    mock_cursor = mock_neo4j_session["cursor"]
    mock_session_obj = mock_neo4j_session["session"]
    mock_cursor.single.return_value = None

    with pytest.raises(NodeNotFoundError):
        await update_hdfeature_properties("Non_Existent", "some_type", {"details": {"info": "new"}})
    mock_session_obj.execute_write.assert_called_once()

# --- delete_hdfeature Tests ---
@pytest.mark.asyncio
async def test_delete_hdfeature_success(mock_neo4j_session, sample_hdfeature_node: HDFeatureNode):
    mock_session_obj = mock_neo4j_session["session"]
    mock_cursor = mock_neo4j_session["cursor"]
    mock_tx = mock_neo4j_session["tx"] # Get mock_tx from the fixture yield

    with patch('services.kg.app.crud.hdfeature_crud.get_hdfeature_by_name_and_type', AsyncMock(return_value=sample_hdfeature_node)):
        mock_cursor.single.return_value = None

        deleted = await delete_hdfeature(FEATURE_NAME, FEATURE_TYPE)

        # --- Assert ---
        assert deleted is True
        mock_session_obj.execute_write.assert_called_once()
        mock_tx.run.assert_called_once() # Check transaction ran
        called_delete_query = mock_tx.run.call_args[0][0]
        assert f"MATCH (hf:{HDFEATURE_LABEL} {{name: $name, feature_type: $feature_type}})" in called_delete_query
        assert "DETACH DELETE hf" in called_delete_query


@pytest.mark.asyncio
async def test_delete_hdfeature_not_found(mock_neo4j_session):
    mock_session_obj = mock_neo4j_session["session"]
    with patch('services.kg.app.crud.hdfeature_crud.get_hdfeature_by_name_and_type', AsyncMock(return_value=None)):
        deleted = await delete_hdfeature("Non_Existent", "some_type")
        assert deleted is False
        mock_session_obj.execute_write.assert_not_called()


@pytest.mark.asyncio
async def test_delete_hdfeature_dao_exception_on_delete(mock_neo4j_session, sample_hdfeature_node: HDFeatureNode):
    mock_session_obj = mock_neo4j_session["session"]
    with patch('services.kg.app.crud.hdfeature_crud.get_hdfeature_by_name_and_type', AsyncMock(return_value=sample_hdfeature_node)):
        mock_session_obj.execute_write.side_effect = DAOException("DB error during actual delete hd")
        with pytest.raises(DeletionError, match="Database error deleting HDFeature"):
            await delete_hdfeature(FEATURE_NAME, FEATURE_TYPE)