import pytest
import pytest_asyncio
from unittest.mock import MagicMock, patch, AsyncMock # Add AsyncMock import

from services.kg.app.graph_schema.nodes import AstroFeatureNode
from services.kg.app.crud.astrofeature_crud import (
    create_astrofeature,
    get_astrofeature_by_name,
    get_astrofeatures_by_type,
    update_astrofeature,
    delete_astrofeature
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

# Sample AstroFeature Data
SAMPLE_ASTROFEATURE_RAW_DATA = {
    "name": "Sun_in_Aries_Test",
    "feature_type": "planet_in_sign",
    "details": {"sign_degree": 10.5, "house": "1st"}
}
# Removed module-level instantiation causing collection error
# SAMPLE_ASTROFEATURE_NODE = AstroFeatureNode(**SAMPLE_ASTROFEATURE_MODEL_DATA)

@pytest_asyncio.fixture
async def sample_astrofeature_node_fixture():
    # Create the model within the fixture, ensuring details are serialized
    model_data = {
         **SAMPLE_ASTROFEATURE_RAW_DATA,
         "details": json.dumps(SAMPLE_ASTROFEATURE_RAW_DATA["details"])
    }
    return AstroFeatureNode(**model_data)

@pytest.mark.asyncio
async def test_create_astrofeature_success(mock_neo4j_driver, sample_astrofeature_node_fixture: AstroFeatureNode):
    mock_tx_run_result = mock_neo4j_driver["tx_run_result"]
    # Mock DB returns properties as they would be stored (details as string)
    mock_db_return_data = {
        **SAMPLE_ASTROFEATURE_RAW_DATA,
        "details": json.dumps(SAMPLE_ASTROFEATURE_RAW_DATA["details"])
    }
    mock_tx_run_result.single.return_value = {"af": mock_db_return_data}

    created_feature = await create_astrofeature(sample_astrofeature_node_fixture) # Pass the validated model

    assert created_feature is not None
    assert created_feature.name == sample_astrofeature_node_fixture.name
    assert created_feature.feature_type == sample_astrofeature_node_fixture.feature_type
    mock_neo4j_driver["session"].execute_write.assert_called_once()

@pytest.mark.asyncio
async def test_create_astrofeature_already_exists(mock_neo4j_driver, sample_astrofeature_node_fixture: AstroFeatureNode):
    mock_session = mock_neo4j_driver["session"]
    mock_session.execute_write.side_effect = UniqueConstraintViolationError("AstroFeature already exists")

    with pytest.raises(NodeCreationError, match="already exists"):
        await create_astrofeature(sample_astrofeature_node_fixture)

@pytest.mark.asyncio
async def test_create_astrofeature_dao_exception(mock_neo4j_driver, sample_astrofeature_node_fixture: AstroFeatureNode):
    mock_session = mock_neo4j_driver["session"]
    mock_session.execute_write.side_effect = DAOException("Generic DB error")

    with pytest.raises(NodeCreationError, match="Database error creating AstroFeature"):
        await create_astrofeature(sample_astrofeature_node_fixture)

@pytest.mark.asyncio
async def test_get_astrofeature_by_name_found(mock_neo4j_driver, sample_astrofeature_node_fixture: AstroFeatureNode):
    mock_tx_run_result = mock_neo4j_driver["tx_run_result"]
    # Assume DB returns properties with details as string
    mock_db_return_data = {
        **SAMPLE_ASTROFEATURE_RAW_DATA,
        "details": json.dumps(SAMPLE_ASTROFEATURE_RAW_DATA["details"])
    }
    mock_tx_run_result.data.return_value = [{"af": mock_db_return_data}]

    retrieved_feature = await get_astrofeature_by_name(sample_astrofeature_node_fixture.name)

    assert retrieved_feature is not None
    assert retrieved_feature.name == sample_astrofeature_node_fixture.name
    mock_neo4j_driver["session"].execute_read.assert_called_once()

@pytest.mark.asyncio
async def test_get_astrofeature_by_name_not_found(mock_neo4j_driver):
    mock_tx_run_result = mock_neo4j_driver["tx_run_result"]
    mock_tx_run_result.data.return_value = []

    retrieved_feature = await get_astrofeature_by_name("Non_Existent_Feature")
    assert retrieved_feature is None

@pytest.mark.asyncio
async def test_get_astrofeatures_by_type_found(mock_neo4j_driver, sample_astrofeature_node_fixture: AstroFeatureNode):
    mock_tx_run_result = mock_neo4j_driver["tx_run_result"]
    # Assume DB returns properties with details as string
    mock_db_return_data = {
        **SAMPLE_ASTROFEATURE_RAW_DATA,
        "details": json.dumps(SAMPLE_ASTROFEATURE_RAW_DATA["details"])
    }
    mock_tx_run_result.data.return_value = [{"af": mock_db_return_data}]

    features = await get_astrofeatures_by_type(sample_astrofeature_node_fixture.feature_type)
    assert len(features) == 1
    assert features[0].name == sample_astrofeature_node_fixture.name

@pytest.mark.asyncio
async def test_get_astrofeatures_by_type_not_found(mock_neo4j_driver):
    mock_tx_run_result = mock_neo4j_driver["tx_run_result"]
    mock_tx_run_result.data.return_value = []
    features = await get_astrofeatures_by_type("non_existent_type")
    assert len(features) == 0

@pytest.mark.asyncio
async def test_get_astrofeature_dao_exception(mock_neo4j_driver):
    mock_session = mock_neo4j_driver["session"]
    mock_session.execute_read.side_effect = DAOException("DB error during get")
    
    feature = await get_astrofeature_by_name("any_name")
    assert feature is None # Current impl returns None on DAOException for gets
    features_list = await get_astrofeatures_by_type("any_type")
    assert features_list == [] # Current impl returns empty list

@pytest.mark.asyncio
async def test_update_astrofeature_success(mock_neo4j_driver, sample_astrofeature_node_fixture: AstroFeatureNode):
    mock_tx_run_result = mock_neo4j_driver["tx_run_result"]
    updated_details = {"sign_degree": 12.0, "house": "2nd", "description": "Updated"}
    
    expected_updated_raw_data = SAMPLE_ASTROFEATURE_RAW_DATA.copy()
    expected_updated_raw_data["details"] = updated_details
    # Mock DB return (details as string)
    mock_db_return_data = {
         **expected_updated_raw_data,
         "details": json.dumps(updated_details)
    }
    mock_tx_run_result.single.return_value = {"af": mock_db_return_data}

    # Pass the dict to update, CRUD should handle serialization
    updated_feature = await update_astrofeature(sample_astrofeature_node_fixture.name, {"details": json.dumps(updated_details)})

    assert updated_feature is not None
    assert updated_feature.details["description"] == "Updated"
    assert updated_feature.details["house"] == "2nd"
    mock_neo4j_driver["session"].execute_write.assert_called_once()

@pytest.mark.asyncio
async def test_update_astrofeature_not_found(mock_neo4j_driver):
    mock_tx_run_result = mock_neo4j_driver["tx_run_result"]
    mock_tx_run_result.single.return_value = None # Simulate update MATCH found nothing

    # Mock the subsequent get_astrofeature_by_name call within update_astrofeature
    with patch('services.kg.app.crud.astrofeature_crud.get_astrofeature_by_name', AsyncMock(return_value=None)):
        with pytest.raises(NodeNotFoundError):
            await update_astrofeature("Non_Existent_Feature_Update", {"details": {"info": "new info"}})

@pytest.mark.asyncio
async def test_update_astrofeature_dao_exception(mock_neo4j_driver):
    mock_session = mock_neo4j_driver["session"]
    mock_session.execute_write.side_effect = DAOException("DB error during update")

    with pytest.raises(UpdateError, match="Database error updating AstroFeature"):
        await update_astrofeature("any_feature_name", {"details": {"info": "new info"}})

@pytest.mark.asyncio
async def test_delete_astrofeature_success(mock_neo4j_driver, sample_astrofeature_node_fixture: AstroFeatureNode):
    with patch('services.kg.app.crud.astrofeature_crud.get_astrofeature_by_name', AsyncMock(return_value=sample_astrofeature_node_fixture)):
        mock_neo4j_driver["tx_run_result"].single.return_value = None # DELETE doesn't typically return the node

        deleted = await delete_astrofeature(sample_astrofeature_node_fixture.name)
        assert deleted is True
        mock_neo4j_driver["session"].execute_write.assert_called_once()

@pytest.mark.asyncio
async def test_delete_astrofeature_not_found(mock_neo4j_driver):
    with patch('services.kg.app.crud.astrofeature_crud.get_astrofeature_by_name', AsyncMock(return_value=None)):
        deleted = await delete_astrofeature("Non_Existent_Feature_Delete")
        assert deleted is False
        mock_neo4j_driver["session"].execute_write.assert_not_called()

@pytest.mark.asyncio
async def test_delete_astrofeature_dao_exception(mock_neo4j_driver, sample_astrofeature_node_fixture: AstroFeatureNode):
    with patch('services.kg.app.crud.astrofeature_crud.get_astrofeature_by_name', AsyncMock(return_value=sample_astrofeature_node_fixture)):
        mock_session = mock_neo4j_driver["session"]
        mock_session.execute_write.side_effect = DAOException("DB error during delete")

        with pytest.raises(DeletionError, match="Database error deleting AstroFeature"):
            await delete_astrofeature(sample_astrofeature_node_fixture.name)