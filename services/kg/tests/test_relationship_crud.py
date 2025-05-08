import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from services.kg.app.graph_schema.nodes import PersonNode, AstroFeatureNode
from services.kg.app.graph_schema.relationships import HasFeatureProperties
from services.kg.app.graph_schema.schema_setup import (
    PERSON_LABEL, ASTROFEATURE_LABEL, HAS_FEATURE_REL
)
from services.kg.app.crud.relationship_crud import (
    create_relationship,
    link_person_to_astrofeature, # Example specific link
    get_relationships_from_node,
    delete_relationship
)
from services.kg.app.crud.base_dao import (
    RelationshipCreationError,
    DAOException
)

# Sample data for testing
SAMPLE_PERSON_USER_ID = "person_rel_test_user"
SAMPLE_ASTROFEATURE_NAME = "AstroFeature_Rel_Test"

SAMPLE_PERSON_PROPS = {"user_id": SAMPLE_PERSON_USER_ID}
SAMPLE_ASTROFEATURE_PROPS = {"name": SAMPLE_ASTROFEATURE_NAME}
SAMPLE_HAS_FEATURE_PROPS_MODEL = HasFeatureProperties(source_calculation_id="calc_abc_123")
SAMPLE_HAS_FEATURE_PROPS_DICT = SAMPLE_HAS_FEATURE_PROPS_MODEL.model_dump(exclude_none=True)


@pytest.mark.asyncio
async def test_create_relationship_success(mock_neo4j_driver):
    mock_tx_run_result = mock_neo4j_driver["tx_run_result"]
    # Simulate the result of the CREATE relationship query
    mock_tx_run_result.single.return_value = {
        "type": HAS_FEATURE_REL,
        "properties": SAMPLE_HAS_FEATURE_PROPS_DICT,
        "start_id": "element_id_person",
        "end_id": "element_id_astrofeature"
    }

    result = await create_relationship(
        start_node_label=PERSON_LABEL,
        start_node_props=SAMPLE_PERSON_PROPS,
        end_node_label=ASTROFEATURE_LABEL,
        end_node_props=SAMPLE_ASTROFEATURE_PROPS,
        relationship_type=HAS_FEATURE_REL,
        rel_props_model=SAMPLE_HAS_FEATURE_PROPS_MODEL
    )

    assert result is not None
    assert result["type"] == HAS_FEATURE_REL
    assert result["properties"]["source_calculation_id"] == SAMPLE_HAS_FEATURE_PROPS_MODEL.source_calculation_id
    mock_neo4j_driver["session"].execute_write.assert_called_once()
    
    # You could inspect the call_args of mock_tx.run to verify the query and params
    # For example:
    # called_query = mock_neo4j_driver["session"].execute_write.call_args[0][0].__self__.run.call_args[0][0]
    # called_params = mock_neo4j_driver["session"].execute_write.call_args[0][0].__self__.run.call_args[1]
    # assert "$rel_props" in called_query
    # assert called_params["rel_props"] == SAMPLE_HAS_FEATURE_PROPS_DICT


@pytest.mark.asyncio
async def test_create_relationship_nodes_not_found(mock_neo4j_driver):
    mock_tx_run_result = mock_neo4j_driver["tx_run_result"]
    mock_tx_run_result.single.return_value = None # Simulate MATCH failing to find nodes

    with pytest.raises(RelationshipCreationError, match="Start or end node not found"):
        await create_relationship(
            start_node_label=PERSON_LABEL,
            start_node_props={"user_id": "non_existent_start"},
            end_node_label=ASTROFEATURE_LABEL,
            end_node_props={"name": "non_existent_end"},
            relationship_type=HAS_FEATURE_REL
        )

@pytest.mark.asyncio
async def test_create_relationship_dao_exception(mock_neo4j_driver):
    mock_session = mock_neo4j_driver["session"]
    mock_session.execute_write.side_effect = DAOException("DB error during relationship create")

    with pytest.raises(RelationshipCreationError, match="Database error creating relationship"):
        await create_relationship(
            start_node_label=PERSON_LABEL,
            start_node_props=SAMPLE_PERSON_PROPS,
            end_node_label=ASTROFEATURE_LABEL,
            end_node_props=SAMPLE_ASTROFEATURE_PROPS,
            relationship_type=HAS_FEATURE_REL
        )

@pytest.mark.asyncio
async def test_link_person_to_astrofeature_success(mock_neo4j_driver):
    """Test the specific helper function for linking person to astrofeature."""
    mock_tx_run_result = mock_neo4j_driver["tx_run_result"]
    mock_tx_run_result.single.return_value = {
        "type": HAS_FEATURE_REL, "properties": SAMPLE_HAS_FEATURE_PROPS_DICT,
        "start_id": "p1", "end_id": "af1"
    }

    # Patch create_relationship as link_person_to_astrofeature calls it internally
    # Or, let it call the actual create_relationship and mock the driver behavior as above.
    # For this test, let's assume create_relationship is tested, and we test the passthrough.
    
    # We can use patch for the generic create_relationship if we want to isolate this specific linker
    with patch('services.kg.app.crud.relationship_crud.create_relationship', AsyncMock(return_value={
        "type": HAS_FEATURE_REL, "properties": SAMPLE_HAS_FEATURE_PROPS_DICT
    })) as mock_generic_create:
        result = await link_person_to_astrofeature(
            SAMPLE_PERSON_USER_ID, SAMPLE_ASTROFEATURE_NAME, SAMPLE_HAS_FEATURE_PROPS_MODEL
        )
        assert result is not None
        assert result["type"] == HAS_FEATURE_REL
        mock_generic_create.assert_called_once_with(
            start_node_label=PERSON_LABEL,
            start_node_props=SAMPLE_PERSON_PROPS,
            end_node_label=ASTROFEATURE_LABEL,
            end_node_props=SAMPLE_ASTROFEATURE_PROPS,
            relationship_type=HAS_FEATURE_REL,
            rel_props_model=SAMPLE_HAS_FEATURE_PROPS_MODEL
        )


@pytest.mark.asyncio
async def test_get_relationships_from_node_success(mock_neo4j_driver):
    mock_tx_run_result = mock_neo4j_driver["tx_run_result"]
    expected_rels_data = [
        {"type": HAS_FEATURE_REL, "properties": {"source_calculation_id": "calc1"}, "end_node_props": {"name": "Feature1"}},
        {"type": HAS_FEATURE_REL, "properties": {"source_calculation_id": "calc2"}, "end_node_props": {"name": "Feature2"}},
    ]
    mock_tx_run_result.data.return_value = expected_rels_data

    rels = await get_relationships_from_node(
        node_label=PERSON_LABEL,
        node_props=SAMPLE_PERSON_PROPS,
        relationship_type=HAS_FEATURE_REL,
        direction="out"
    )
    assert len(rels) == 2
    assert rels[0]["type"] == HAS_FEATURE_REL
    assert rels[0]["end_node_props"]["name"] == "Feature1"
    mock_neo4j_driver["session"].execute_read.assert_called_once()

@pytest.mark.asyncio
async def test_get_relationships_from_node_dao_exception(mock_neo4j_driver):
    mock_session = mock_neo4j_driver["session"]
    mock_session.execute_read.side_effect = DAOException("DB error getting rels")

    rels = await get_relationships_from_node(PERSON_LABEL, SAMPLE_PERSON_PROPS)
    assert rels == [] # Current impl returns empty list on DAOException

@pytest.mark.asyncio
async def test_delete_relationship_success(mock_neo4j_driver):
    mock_tx_run_result = mock_neo4j_driver["tx_run_result"]
    # DELETE query doesn't need to return specific data, just succeed
    mock_tx_run_result.single.return_value = None # Or a summary object

    deleted = await delete_relationship(
        start_node_label=PERSON_LABEL,
        start_node_props=SAMPLE_PERSON_PROPS,
        end_node_label=ASTROFEATURE_LABEL,
        end_node_props=SAMPLE_ASTROFEATURE_PROPS,
        relationship_type=HAS_FEATURE_REL
    )
    assert deleted is True
    mock_neo4j_driver["session"].execute_write.assert_called_once()

@pytest.mark.asyncio
async def test_delete_relationship_dao_exception(mock_neo4j_driver):
    mock_session = mock_neo4j_driver["session"]
    mock_session.execute_write.side_effect = DAOException("DB error deleting rel")

    deleted = await delete_relationship(
        PERSON_LABEL, SAMPLE_PERSON_PROPS, ASTROFEATURE_LABEL, SAMPLE_ASTROFEATURE_PROPS, HAS_FEATURE_REL
    )
    assert deleted is False # Current impl returns False on DAOException