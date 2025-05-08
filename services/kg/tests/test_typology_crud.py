import pytest
import pytest_asyncio
from unittest.mock import MagicMock, patch, AsyncMock # Add AsyncMock

from services.kg.app.graph_schema.nodes import TypologyResultNode
from services.kg.app.crud.typology_crud import (
    create_typology_result,
    get_typology_result_by_assessment_id,
    get_typology_results_by_name,
    update_typology_result,
    delete_typology_result
)
from services.kg.app.crud.base_dao import (
    NodeCreationError,
    NodeNotFoundError,
    UpdateError,
    DeletionError,
    UniqueConstraintViolationError,
    DAOException
)

import json # Add json import

# Sample TypologyResult Data
SAMPLE_TYPOLOGY_RAW_DATA = {
    "assessment_id": "assess_xyz_123",
    "typology_name": "TestEnneagram",
    "score": "Type 9",
    "confidence": 0.88,
    "details": {"wing": "1", "notes": "Peacekeeper"}
}
# Removed module-level instantiation causing collection error
# SAMPLE_TYPOLOGY_NODE = TypologyResultNode(**SAMPLE_TYPOLOGY_MODEL_DATA)

@pytest_asyncio.fixture
async def sample_typology_node_fixture():
    # Create the model within the fixture, ensuring details are serialized
    model_data = {
         **SAMPLE_TYPOLOGY_RAW_DATA,
         "details": json.dumps(SAMPLE_TYPOLOGY_RAW_DATA["details"])
    }
    return TypologyResultNode(**model_data)

@pytest.mark.asyncio
async def test_create_typology_result_success(mock_neo4j_driver, sample_typology_node_fixture: TypologyResultNode):
    mock_tx_run_result = mock_neo4j_driver["tx_run_result"]
    # Mock DB returns properties as they would be stored (details as string)
    mock_db_return_data = {
        **SAMPLE_TYPOLOGY_RAW_DATA,
        "details": json.dumps(SAMPLE_TYPOLOGY_RAW_DATA["details"])
    }
    mock_tx_run_result.single.return_value = {"tr": mock_db_return_data}

    created_result = await create_typology_result(sample_typology_node_fixture) # Pass the validated model

    assert created_result is not None
    assert created_result.assessment_id == sample_typology_node_fixture.assessment_id
    mock_neo4j_driver["session"].execute_write.assert_called_once()

@pytest.mark.asyncio
async def test_create_typology_result_already_exists(mock_neo4j_driver, sample_typology_node_fixture: TypologyResultNode):
    mock_session = mock_neo4j_driver["session"]
    mock_session.execute_write.side_effect = UniqueConstraintViolationError("TypologyResult already exists")

    with pytest.raises(NodeCreationError, match="already exists"):
        await create_typology_result(sample_typology_node_fixture)

@pytest.mark.asyncio
async def test_create_typology_result_dao_exception(mock_neo4j_driver, sample_typology_node_fixture: TypologyResultNode):
    mock_session = mock_neo4j_driver["session"]
    mock_session.execute_write.side_effect = DAOException("Generic DB error")

    with pytest.raises(NodeCreationError, match="Database error creating TypologyResult"):
        await create_typology_result(sample_typology_node_fixture)

@pytest.mark.asyncio
async def test_get_typology_result_by_assessment_id_found(mock_neo4j_driver, sample_typology_node_fixture: TypologyResultNode):
    mock_tx_run_result = mock_neo4j_driver["tx_run_result"]
    # Assume DB returns properties with details as string
    mock_db_return_data = {
        **SAMPLE_TYPOLOGY_RAW_DATA,
        "details": json.dumps(SAMPLE_TYPOLOGY_RAW_DATA["details"])
    }
    mock_tx_run_result.data.return_value = [{"tr": mock_db_return_data}]

    retrieved_result = await get_typology_result_by_assessment_id(sample_typology_node_fixture.assessment_id)

    assert retrieved_result is not None
    assert retrieved_result.assessment_id == sample_typology_node_fixture.assessment_id
    mock_neo4j_driver["session"].execute_read.assert_called_once()

@pytest.mark.asyncio
async def test_get_typology_result_by_assessment_id_not_found(mock_neo4j_driver):
    mock_tx_run_result = mock_neo4j_driver["tx_run_result"]
    mock_tx_run_result.data.return_value = []

    retrieved_result = await get_typology_result_by_assessment_id("non_existent_assessment")
    assert retrieved_result is None

@pytest.mark.asyncio
async def test_get_typology_results_by_name_found(mock_neo4j_driver, sample_typology_node_fixture: TypologyResultNode):
    mock_tx_run_result = mock_neo4j_driver["tx_run_result"]
    # Assume DB returns properties with details as string
    mock_db_return_data = {
        **SAMPLE_TYPOLOGY_RAW_DATA,
        "details": json.dumps(SAMPLE_TYPOLOGY_RAW_DATA["details"])
    }
    mock_tx_run_result.data.return_value = [{"tr": mock_db_return_data}]

    results = await get_typology_results_by_name(sample_typology_node_fixture.typology_name)
    assert len(results) == 1
    assert results[0].assessment_id == sample_typology_node_fixture.assessment_id

@pytest.mark.asyncio
async def test_get_typology_results_by_name_not_found(mock_neo4j_driver):
    mock_tx_run_result = mock_neo4j_driver["tx_run_result"]
    mock_tx_run_result.data.return_value = []
    results = await get_typology_results_by_name("NonExistentTypologyName")
    assert len(results) == 0

@pytest.mark.asyncio
async def test_get_typology_result_dao_exception(mock_neo4j_driver):
    mock_session = mock_neo4j_driver["session"]
    mock_session.execute_read.side_effect = DAOException("DB error during get")
    
    result_node = await get_typology_result_by_assessment_id("any_assessment_id")
    assert result_node is None
    results_list = await get_typology_results_by_name("any_typology_name")
    assert results_list == []


@pytest.mark.asyncio
async def test_update_typology_result_success(mock_neo4j_driver, sample_typology_node_fixture: TypologyResultNode):
    mock_tx_run_result = mock_neo4j_driver["tx_run_result"]
    updated_score = "Type 9w1"
    
    expected_updated_raw_data = SAMPLE_TYPOLOGY_RAW_DATA.copy()
    expected_updated_raw_data["score"] = updated_score
    # Mock DB return (details as string)
    mock_db_return_data = {
         **expected_updated_raw_data,
         "details": json.dumps(expected_updated_raw_data["details"]) # Keep original details string
    }
    mock_tx_run_result.single.return_value = {"tr": mock_db_return_data}

    # Pass the dict to update
    updated_result = await update_typology_result(sample_typology_node_fixture.assessment_id, {"score": updated_score})

    assert updated_result is not None
    assert updated_result.score == updated_score
    mock_neo4j_driver["session"].execute_write.assert_called_once()

@pytest.mark.asyncio
async def test_update_typology_result_not_found(mock_neo4j_driver):
    mock_tx_run_result = mock_neo4j_driver["tx_run_result"]
    mock_tx_run_result.single.return_value = None

    with patch('services.kg.app.crud.typology_crud.get_typology_result_by_assessment_id', AsyncMock(return_value=None)):
        with pytest.raises(NodeNotFoundError):
            await update_typology_result("non_existent_assessment_update", {"score": "New Score"})

@pytest.mark.asyncio
async def test_update_typology_result_dao_exception(mock_neo4j_driver):
    mock_session = mock_neo4j_driver["session"]
    mock_session.execute_write.side_effect = DAOException("DB error during update")

    with pytest.raises(UpdateError, match="Database error updating TypologyResult"):
        await update_typology_result("any_assessment_id", {"score": "New Score"})

@pytest.mark.asyncio
async def test_delete_typology_result_success(mock_neo4j_driver, sample_typology_node_fixture: TypologyResultNode):
    with patch('services.kg.app.crud.typology_crud.get_typology_result_by_assessment_id', AsyncMock(return_value=sample_typology_node_fixture)):
        mock_neo4j_driver["tx_run_result"].single.return_value = None

        deleted = await delete_typology_result(sample_typology_node_fixture.assessment_id)
        assert deleted is True
        mock_neo4j_driver["session"].execute_write.assert_called_once()

@pytest.mark.asyncio
async def test_delete_typology_result_not_found(mock_neo4j_driver):
    with patch('services.kg.app.crud.typology_crud.get_typology_result_by_assessment_id', AsyncMock(return_value=None)):
        deleted = await delete_typology_result("non_existent_assessment_delete")
        assert deleted is False
        mock_neo4j_driver["session"].execute_write.assert_not_called()

@pytest.mark.asyncio
async def test_delete_typology_result_dao_exception(mock_neo4j_driver, sample_typology_node_fixture: TypologyResultNode):
    with patch('services.kg.app.crud.typology_crud.get_typology_result_by_assessment_id', AsyncMock(return_value=sample_typology_node_fixture)):
        mock_session = mock_neo4j_driver["session"]
        mock_session.execute_write.side_effect = DAOException("DB error during delete")

        with pytest.raises(DeletionError, match="Database error deleting TypologyResult"):
            await delete_typology_result(sample_typology_node_fixture.assessment_id)