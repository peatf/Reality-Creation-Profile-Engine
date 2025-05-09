import pytest
import pytest_asyncio
from unittest.mock import patch, AsyncMock
import json

from services.kg.app.graph_schema.nodes import TypologyResultNode
from services.kg.app.crud.typology_crud import (
    upsert_typology_result,         # Changed
    get_typology_result_by_assessment_id,
    get_typology_results_by_name,
    update_typology_result_properties, # Changed
    delete_typology_result
)
from services.kg.app.crud.base_dao import (
    NodeNotFoundError,
    UpdateError,
    DeletionError,
    UniqueConstraintViolationError, # Should not happen with MERGE on key
    DAOException
)
from services.kg.app.graph_schema.constants import TYPOLOGYRESULT_LABEL


# Sample TypologyResult Data
ASSESSMENT_ID = "assess_xyz_123"
TYPOLOGY_NAME = "TestEnneagram"
INITIAL_SCORE = "Type 9"
INITIAL_CONFIDENCE = 0.88
INITIAL_DETAILS_DICT = {"wing": "1", "notes": "Peacekeeper"}
INITIAL_DETAILS_JSON_STR = json.dumps(INITIAL_DETAILS_DICT)

@pytest_asyncio.fixture
def sample_typology_data() -> dict:
    """Raw data for DB mock return, details as JSON string."""
    return {
        "assessment_id": ASSESSMENT_ID,
        "typology_name": TYPOLOGY_NAME,
        "score": INITIAL_SCORE,
        "confidence": INITIAL_CONFIDENCE,
        "details": INITIAL_DETAILS_JSON_STR # Stored as JSON string in DB
    }

@pytest_asyncio.fixture
def sample_typology_node(sample_typology_data: dict) -> TypologyResultNode:
    """TypologyResultNode instance for passing to CRUD functions, details as dict."""
    data_for_model = sample_typology_data.copy()
    data_for_model["details"] = json.loads(sample_typology_data["details"])
    return TypologyResultNode(**data_for_model)


# --- upsert_typology_result Tests ---
@pytest.mark.asyncio
async def test_upsert_typology_result_create_success(mock_neo4j_session, sample_typology_node: TypologyResultNode, sample_typology_data: dict):
    """Test successful creation of a TypologyResultNode via upsert."""
    mock_cursor = mock_neo4j_session["cursor"]
    mock_session_obj = mock_neo4j_session["session"]
    mock_tx = mock_neo4j_session["tx"] # Get mock_tx from the fixture yield

    mock_cursor.single.return_value = {"tr": sample_typology_data}

    # --- Act ---
    created_result = await upsert_typology_result(sample_typology_node)

    # --- Assert ---
    assert created_result is not None
    assert created_result.assessment_id == ASSESSMENT_ID
    assert created_result.score == INITIAL_SCORE
    assert created_result.details == INITIAL_DETAILS_DICT

    # Assert mock calls *after* the function call
    mock_session_obj.execute_write.assert_called_once()
    mock_tx.run.assert_called_once()
    called_query = mock_tx.run.call_args[0][0]
    assert f"MERGE (tr:{TYPOLOGYRESULT_LABEL} {{assessment_id: $assessment_id}})" in called_query
    assert "ON CREATE SET tr = $create_props" in called_query
    assert "ON MATCH SET tr += $match_props" in called_query
    
    call_params = mock_tx.run.call_args[0][1]
    assert call_params["assessment_id"] == ASSESSMENT_ID
    assert call_params["create_props"]["score"] == INITIAL_SCORE
    assert call_params["match_props"]["score"] == INITIAL_SCORE


@pytest.mark.asyncio
async def test_upsert_typology_result_update_success(mock_neo4j_session, sample_typology_node: TypologyResultNode):
    mock_cursor = mock_neo4j_session["cursor"]
    mock_session_obj = mock_neo4j_session["session"]
    mock_tx = mock_neo4j_session["tx"] # Get mock_tx from the fixture yield

    updated_score = "Type 9w1"
    updated_details_dict = {"wing": "1", "notes": "Updated Peacekeeper", "level": 5}
    
    updated_node_input = sample_typology_node.model_copy(update={
        "score": updated_score,
        "details": updated_details_dict
    })

    db_return_data = {
        "assessment_id": ASSESSMENT_ID,
        "typology_name": TYPOLOGY_NAME,
        "score": updated_score,
        "confidence": INITIAL_CONFIDENCE, # Assuming confidence wasn't updated
        "details": json.dumps(updated_details_dict)
    }
    mock_cursor.single.return_value = {"tr": db_return_data}

    # --- Act ---
    updated_result = await upsert_typology_result(updated_node_input)

    # --- Assert ---
    assert updated_result is not None
    assert updated_result.assessment_id == ASSESSMENT_ID
    assert updated_result.score == updated_score
    assert updated_result.details == updated_details_dict

    # Assert mock calls *after* the function call
    mock_session_obj.execute_write.assert_called_once()
    mock_tx.run.assert_called_once() # Check transaction ran
    call_params = mock_tx.run.call_args[0][1]
    assert call_params["match_props"]["score"] == updated_score
    assert call_params["match_props"]["details"] == updated_details_dict


@pytest.mark.asyncio
async def test_upsert_typology_result_dao_exception(mock_neo4j_session, sample_typology_node: TypologyResultNode):
    mock_session_obj = mock_neo4j_session["session"]
    mock_session_obj.execute_write.side_effect = DAOException("Generic DB error for upsert typology")

    with pytest.raises(DAOException, match="Database error upserting TypologyResult"):
        await upsert_typology_result(sample_typology_node)

# --- get_typology_result_by_assessment_id Tests ---
@pytest.mark.asyncio
async def test_get_typology_result_by_assessment_id_found(mock_neo4j_session, sample_typology_data: dict):
    mock_cursor = mock_neo4j_session["cursor"]
    mock_session_obj = mock_neo4j_session["session"]
    mock_tx = mock_neo4j_session["tx"] # Get mock_tx from the fixture yield

    mock_cursor.data.return_value = [{"tr": sample_typology_data}]

    # --- Act ---
    retrieved_result = await get_typology_result_by_assessment_id(ASSESSMENT_ID)

    # --- Assert ---
    assert retrieved_result is not None
    assert retrieved_result.assessment_id == ASSESSMENT_ID
    assert retrieved_result.details == INITIAL_DETAILS_DICT

    # Assert mock calls *after* the function call
    mock_session_obj.execute_read.assert_called_once()
    mock_tx.run.assert_called_once()
    called_query = mock_tx.run.call_args[0][0]
    assert f"MATCH (tr:{TYPOLOGYRESULT_LABEL} {{assessment_id: $assessment_id}})" in called_query

@pytest.mark.asyncio
async def test_get_typology_result_by_assessment_id_not_found(mock_neo4j_session):
    mock_cursor = mock_neo4j_session["cursor"]
    mock_cursor.data.return_value = []

    retrieved_result = await get_typology_result_by_assessment_id("non_existent_assessment")
    assert retrieved_result is None

# --- get_typology_results_by_name Tests ---
@pytest.mark.asyncio
async def test_get_typology_results_by_name_found(mock_neo4j_session, sample_typology_data: dict):
    mock_cursor = mock_neo4j_session["cursor"]
    mock_cursor.data.return_value = [{"tr": sample_typology_data}]

    results = await get_typology_results_by_name(TYPOLOGY_NAME)
    assert len(results) == 1
    assert results[0].assessment_id == ASSESSMENT_ID
    assert results[0].details == INITIAL_DETAILS_DICT

# --- update_typology_result_properties Tests ---
@pytest.mark.asyncio
async def test_update_typology_result_properties_success(mock_neo4j_session, sample_typology_data: dict):
    mock_cursor = mock_neo4j_session["cursor"]
    mock_session_obj = mock_neo4j_session["session"]
    mock_tx = mock_neo4j_session["tx"] # Get mock_tx from the fixture yield

    update_payload = {"score": "Type 9w1 Updated", "confidence": 0.99}

    expected_db_data_after_update = sample_typology_data.copy()
    expected_db_data_after_update.update(update_payload)
    # details remain the initial JSON string as it wasn't in update_payload
    mock_cursor.single.return_value = {"tr": expected_db_data_after_update}

    # --- Act ---
    updated_result = await update_typology_result_properties(ASSESSMENT_ID, update_payload)

    # --- Assert ---
    assert updated_result is not None
    assert updated_result.score == "Type 9w1 Updated"
    assert updated_result.confidence == 0.99
    assert updated_result.details == INITIAL_DETAILS_DICT # Details unchanged

    # Assert mock calls *after* the function call
    mock_session_obj.execute_write.assert_called_once()
    mock_tx.run.assert_called_once()
    called_query = mock_tx.run.call_args[0][0]
    assert f"MATCH (tr:{TYPOLOGYRESULT_LABEL} {{assessment_id: $assessment_id}})" in called_query
    assert "SET tr += $props_to_set" in called_query
    assert mock_tx.run.call_args[0][1]["props_to_set"] == update_payload


@pytest.mark.asyncio
async def test_update_typology_result_properties_not_found(mock_neo4j_session):
    mock_cursor = mock_neo4j_session["cursor"]
    mock_session_obj = mock_neo4j_session["session"]
    mock_cursor.single.return_value = None

    with pytest.raises(NodeNotFoundError):
        await update_typology_result_properties("non_existent", {"score": "New"})
    mock_session_obj.execute_write.assert_called_once()

# --- delete_typology_result Tests ---
@pytest.mark.asyncio
async def test_delete_typology_result_success(mock_neo4j_session, sample_typology_node: TypologyResultNode):
    mock_session_obj = mock_neo4j_session["session"]
    mock_cursor = mock_neo4j_session["cursor"]
    mock_tx = mock_neo4j_session["tx"] # Get mock_tx from the fixture yield

    with patch('services.kg.app.crud.typology_crud.get_typology_result_by_assessment_id', AsyncMock(return_value=sample_typology_node)):
        mock_cursor.single.return_value = None

        deleted = await delete_typology_result(ASSESSMENT_ID)

        # --- Assert ---
        assert deleted is True
        mock_session_obj.execute_write.assert_called_once()
        mock_tx.run.assert_called_once() # Check transaction ran
        called_delete_query = mock_tx.run.call_args[0][0]
        assert f"MATCH (tr:{TYPOLOGYRESULT_LABEL} {{assessment_id: $assessment_id}})" in called_delete_query
        assert "DETACH DELETE tr" in called_delete_query


@pytest.mark.asyncio
async def test_delete_typology_result_not_found(mock_neo4j_session):
    mock_session_obj = mock_neo4j_session["session"]
    with patch('services.kg.app.crud.typology_crud.get_typology_result_by_assessment_id', AsyncMock(return_value=None)):
        deleted = await delete_typology_result("non_existent")
        assert deleted is False
        mock_session_obj.execute_write.assert_not_called()


@pytest.mark.asyncio
async def test_delete_typology_result_dao_exception_on_delete(mock_neo4j_session, sample_typology_node: TypologyResultNode):
    mock_session_obj = mock_neo4j_session["session"]
    with patch('services.kg.app.crud.typology_crud.get_typology_result_by_assessment_id', AsyncMock(return_value=sample_typology_node)):
        mock_session_obj.execute_write.side_effect = DAOException("DB error during actual delete typology")
        with pytest.raises(DeletionError, match="Database error deleting TypologyResult"):
            await delete_typology_result(ASSESSMENT_ID)