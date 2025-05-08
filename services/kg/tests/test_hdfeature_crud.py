import pytest
import pytest_asyncio
from unittest.mock import MagicMock, patch, AsyncMock # Add AsyncMock

from services.kg.app.graph_schema.nodes import HDFeatureNode
from services.kg.app.crud.hdfeature_crud import (
    create_hdfeature,
    get_hdfeature_by_name,
    get_hdfeatures_by_type,
    update_hdfeature,
    delete_hdfeature
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

# Sample HDFeature Data
SAMPLE_HDFEATURE_RAW_DATA = {
    "name": "Defined_Throat_Test",
    "feature_type": "center",
    "details": {"state": "Defined", "function": "Manifestation, communication"}
}
# Removed module-level instantiation causing collection error
# SAMPLE_HDFEATURE_NODE = HDFeatureNode(**SAMPLE_HDFEATURE_MODEL_DATA)

@pytest_asyncio.fixture
async def sample_hdfeature_node_fixture():
    # Create the model within the fixture, ensuring details are serialized
    model_data = {
         **SAMPLE_HDFEATURE_RAW_DATA,
         "details": json.dumps(SAMPLE_HDFEATURE_RAW_DATA["details"])
    }
    return HDFeatureNode(**model_data)

@pytest.mark.asyncio
async def test_create_hdfeature_success(mock_neo4j_driver, sample_hdfeature_node_fixture: HDFeatureNode):
    mock_tx_run_result = mock_neo4j_driver["tx_run_result"]
    # Mock DB returns properties as they would be stored (details as string)
    mock_db_return_data = {
        **SAMPLE_HDFEATURE_RAW_DATA,
        "details": json.dumps(SAMPLE_HDFEATURE_RAW_DATA["details"])
    }
    mock_tx_run_result.single.return_value = {"hf": mock_db_return_data}

    created_feature = await create_hdfeature(sample_hdfeature_node_fixture) # Pass the validated model

    assert created_feature is not None
    assert created_feature.name == sample_hdfeature_node_fixture.name
    assert created_feature.feature_type == sample_hdfeature_node_fixture.feature_type
    mock_neo4j_driver["session"].execute_write.assert_called_once()

@pytest.mark.asyncio
async def test_create_hdfeature_already_exists(mock_neo4j_driver, sample_hdfeature_node_fixture: HDFeatureNode):
    mock_session = mock_neo4j_driver["session"]
    mock_session.execute_write.side_effect = UniqueConstraintViolationError("HDFeature already exists")

    with pytest.raises(NodeCreationError, match="already exists"):
        await create_hdfeature(sample_hdfeature_node_fixture)

@pytest.mark.asyncio
async def test_create_hdfeature_dao_exception(mock_neo4j_driver, sample_hdfeature_node_fixture: HDFeatureNode):
    mock_session = mock_neo4j_driver["session"]
    mock_session.execute_write.side_effect = DAOException("Generic DB error")

    with pytest.raises(NodeCreationError, match="Database error creating HDFeature"):
        await create_hdfeature(sample_hdfeature_node_fixture)


@pytest.mark.asyncio
async def test_get_hdfeature_by_name_found(mock_neo4j_driver, sample_hdfeature_node_fixture: HDFeatureNode):
    mock_tx_run_result = mock_neo4j_driver["tx_run_result"]
    # Assume DB returns properties with details as string
    mock_db_return_data = {
        **SAMPLE_HDFEATURE_RAW_DATA,
        "details": json.dumps(SAMPLE_HDFEATURE_RAW_DATA["details"])
    }
    mock_tx_run_result.data.return_value = [{"hf": mock_db_return_data}]

    retrieved_feature = await get_hdfeature_by_name(sample_hdfeature_node_fixture.name)

    assert retrieved_feature is not None
    assert retrieved_feature.name == sample_hdfeature_node_fixture.name
    mock_neo4j_driver["session"].execute_read.assert_called_once()

@pytest.mark.asyncio
async def test_get_hdfeature_by_name_not_found(mock_neo4j_driver):
    mock_tx_run_result = mock_neo4j_driver["tx_run_result"]
    mock_tx_run_result.data.return_value = []

    retrieved_feature = await get_hdfeature_by_name("Non_Existent_HD_Feature")
    assert retrieved_feature is None

@pytest.mark.asyncio
async def test_get_hdfeatures_by_type_found(mock_neo4j_driver, sample_hdfeature_node_fixture: HDFeatureNode):
    mock_tx_run_result = mock_neo4j_driver["tx_run_result"]
    # Assume DB returns properties with details as string
    mock_db_return_data = {
        **SAMPLE_HDFEATURE_RAW_DATA,
        "details": json.dumps(SAMPLE_HDFEATURE_RAW_DATA["details"])
    }
    mock_tx_run_result.data.return_value = [{"hf": mock_db_return_data}]

    features = await get_hdfeatures_by_type(sample_hdfeature_node_fixture.feature_type)
    assert len(features) == 1
    assert features[0].name == sample_hdfeature_node_fixture.name

@pytest.mark.asyncio
async def test_get_hdfeatures_by_type_not_found(mock_neo4j_driver):
    mock_tx_run_result = mock_neo4j_driver["tx_run_result"]
    mock_tx_run_result.data.return_value = []
    features = await get_hdfeatures_by_type("non_existent_hd_type")
    assert len(features) == 0
    
@pytest.mark.asyncio
async def test_get_hdfeature_dao_exception(mock_neo4j_driver):
    mock_session = mock_neo4j_driver["session"]
    mock_session.execute_read.side_effect = DAOException("DB error during get")
    
    feature = await get_hdfeature_by_name("any_hd_name")
    assert feature is None
    features_list = await get_hdfeatures_by_type("any_hd_type")
    assert features_list == []

@pytest.mark.asyncio
async def test_update_hdfeature_success(mock_neo4j_driver, sample_hdfeature_node_fixture: HDFeatureNode):
    mock_tx_run_result = mock_neo4j_driver["tx_run_result"]
    updated_details = {"state": "Defined", "function": "Manifestation, communication", "voice": "I speak"}
    
    expected_updated_raw_data = SAMPLE_HDFEATURE_RAW_DATA.copy()
    expected_updated_raw_data["details"] = updated_details
    # Mock DB return (details as string)
    mock_db_return_data = {
         **expected_updated_raw_data,
         "details": json.dumps(updated_details)
    }
    mock_tx_run_result.single.return_value = {"hf": mock_db_return_data}

    # Pass the dict to update, CRUD should handle serialization
    updated_feature = await update_hdfeature(sample_hdfeature_node_fixture.name, {"details": json.dumps(updated_details)})

    assert updated_feature is not None
    assert updated_feature.details["voice"] == "I speak"
    mock_neo4j_driver["session"].execute_write.assert_called_once()

@pytest.mark.asyncio
async def test_update_hdfeature_not_found(mock_neo4j_driver):
    mock_tx_run_result = mock_neo4j_driver["tx_run_result"]
    mock_tx_run_result.single.return_value = None

    with patch('services.kg.app.crud.hdfeature_crud.get_hdfeature_by_name', AsyncMock(return_value=None)):
        with pytest.raises(NodeNotFoundError):
            await update_hdfeature("Non_Existent_HD_Feature_Update", {"details": {"info": "new info"}})

@pytest.mark.asyncio
async def test_update_hdfeature_dao_exception(mock_neo4j_driver):
    mock_session = mock_neo4j_driver["session"]
    mock_session.execute_write.side_effect = DAOException("DB error during update")

    with pytest.raises(UpdateError, match="Database error updating HDFeature"):
        await update_hdfeature("any_hd_feature_name", {"details": {"info": "new info"}})

@pytest.mark.asyncio
async def test_delete_hdfeature_success(mock_neo4j_driver, sample_hdfeature_node_fixture: HDFeatureNode):
    with patch('services.kg.app.crud.hdfeature_crud.get_hdfeature_by_name', AsyncMock(return_value=sample_hdfeature_node_fixture)):
        mock_neo4j_driver["tx_run_result"].single.return_value = None

        deleted = await delete_hdfeature(sample_hdfeature_node_fixture.name)
        assert deleted is True
        mock_neo4j_driver["session"].execute_write.assert_called_once()

@pytest.mark.asyncio
async def test_delete_hdfeature_not_found(mock_neo4j_driver):
    with patch('services.kg.app.crud.hdfeature_crud.get_hdfeature_by_name', AsyncMock(return_value=None)):
        deleted = await delete_hdfeature("Non_Existent_HD_Feature_Delete")
        assert deleted is False
        mock_neo4j_driver["session"].execute_write.assert_not_called()

@pytest.mark.asyncio
async def test_delete_hdfeature_dao_exception(mock_neo4j_driver, sample_hdfeature_node_fixture: HDFeatureNode):
    with patch('services.kg.app.crud.hdfeature_crud.get_hdfeature_by_name', AsyncMock(return_value=sample_hdfeature_node_fixture)):
        mock_session = mock_neo4j_driver["session"]
        mock_session.execute_write.side_effect = DAOException("DB error during delete")

        with pytest.raises(DeletionError, match="Database error deleting HDFeature"):
            await delete_hdfeature(sample_hdfeature_node_fixture.name)