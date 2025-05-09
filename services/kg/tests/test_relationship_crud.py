import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, patch

from services.kg.app.graph_schema.nodes import PersonNode, AstroFeatureNode
from services.kg.app.graph_schema.relationships import HasFeatureProperties
from services.kg.app.graph_schema.constants import ( # Changed import style
    PERSON_LABEL, ASTROFEATURE_LABEL, HAS_FEATURE_REL
)
from services.kg.app.crud.relationship_crud import (
    merge_relationship,             # Changed
    link_person_to_astrofeature,    # Changed signature
    get_relationships_from_node,
    delete_relationship
)
from services.kg.app.crud.base_dao import (
    RelationshipCreationError,
    DAOException,
    NodeNotFoundError # Added for delete test
)

# Sample data for testing
SAMPLE_PERSON_USER_ID = "person_rel_merge_user"
SAMPLE_ASTROFEATURE_NAME = "AstroFeature_Rel_Merge"
SAMPLE_ASTROFEATURE_TYPE = "planet_in_sign_reltest" # Added type

SAMPLE_PERSON_PROPS = {"user_id": SAMPLE_PERSON_USER_ID}
# Updated props to include composite key for AstroFeature
SAMPLE_ASTROFEATURE_PROPS = {"name": SAMPLE_ASTROFEATURE_NAME, "feature_type": SAMPLE_ASTROFEATURE_TYPE}

SAMPLE_HAS_FEATURE_PROPS_MODEL_V1 = HasFeatureProperties(source_calculation_id="calc_merge_123", relevance=0.8)
SAMPLE_HAS_FEATURE_PROPS_DICT_V1 = SAMPLE_HAS_FEATURE_PROPS_MODEL_V1.model_dump(exclude_none=True)

SAMPLE_HAS_FEATURE_PROPS_MODEL_V2 = HasFeatureProperties(source_calculation_id="calc_merge_456", relevance=0.9)
SAMPLE_HAS_FEATURE_PROPS_DICT_V2 = SAMPLE_HAS_FEATURE_PROPS_MODEL_V2.model_dump(exclude_none=True)


@pytest.mark.asyncio
async def test_merge_relationship_create_success(mock_neo4j_session):
    """Test creating a relationship using merge_relationship."""
    mock_cursor = mock_neo4j_session["cursor"]
    mock_session_obj = mock_neo4j_session["session"]
    mock_tx = mock_neo4j_session["tx"] # Get mock_tx from the fixture yield

    # Simulate the result of the MERGE relationship query (creation)
    mock_cursor.single.return_value = {
        "type": HAS_FEATURE_REL,
        "properties": SAMPLE_HAS_FEATURE_PROPS_DICT_V1,
        "start_id": "element_id_person",
        "end_id": "element_id_astrofeature"
    }

    result = await merge_relationship(
        start_node_label=PERSON_LABEL,
        start_node_props=SAMPLE_PERSON_PROPS,
        end_node_label=ASTROFEATURE_LABEL,
        end_node_props=SAMPLE_ASTROFEATURE_PROPS, # Now includes type
        relationship_type=HAS_FEATURE_REL,
        rel_props_model=SAMPLE_HAS_FEATURE_PROPS_MODEL_V1
    )

    assert result is not None
    assert result["type"] == HAS_FEATURE_REL
    assert result["properties"]["relevance"] == SAMPLE_HAS_FEATURE_PROPS_MODEL_V1.relevance
    mock_session_obj.execute_write.assert_called_once()
    
    called_query = mock_tx.run.call_args[0][0]
    assert f"MERGE (start_node)-[r:{HAS_FEATURE_REL}]->(end_node)" in called_query
    assert "ON CREATE SET r = $rel_props" in called_query
    assert "ON MATCH SET r = $rel_props" in called_query # Check if update clause is present
    
    call_params = mock_tx.run.call_args[0][1]
    assert call_params["rel_props"] == SAMPLE_HAS_FEATURE_PROPS_DICT_V1
    assert call_params["name_end"] == SAMPLE_ASTROFEATURE_NAME
    assert call_params["feature_type_end"] == SAMPLE_ASTROFEATURE_TYPE


@pytest.mark.asyncio
async def test_merge_relationship_update_success(mock_neo4j_session):
    """Test updating a relationship's properties using merge_relationship."""
    mock_cursor = mock_neo4j_session["cursor"]
    mock_session_obj = mock_neo4j_session["session"]
    mock_tx = mock_neo4j_session["tx"] # Get mock_tx from the fixture yield

    # Simulate MERGE finding the relationship and returning updated properties
    mock_cursor.single.return_value = {
        "type": HAS_FEATURE_REL,
        "properties": SAMPLE_HAS_FEATURE_PROPS_DICT_V2, # Return V2 props
        "start_id": "element_id_person",
        "end_id": "element_id_astrofeature"
    }

    # Call merge_relationship with V2 properties
    result = await merge_relationship(
        start_node_label=PERSON_LABEL,
        start_node_props=SAMPLE_PERSON_PROPS,
        end_node_label=ASTROFEATURE_LABEL,
        end_node_props=SAMPLE_ASTROFEATURE_PROPS,
        relationship_type=HAS_FEATURE_REL,
        rel_props_model=SAMPLE_HAS_FEATURE_PROPS_MODEL_V2 # Use V2 model
    )

    assert result is not None
    assert result["properties"]["relevance"] == SAMPLE_HAS_FEATURE_PROPS_MODEL_V2.relevance
    assert result["properties"]["source_calculation_id"] == SAMPLE_HAS_FEATURE_PROPS_MODEL_V2.source_calculation_id
    mock_session_obj.execute_write.assert_called_once()
    
    call_params = mock_tx.run.call_args[0][1]
    assert call_params["rel_props"] == SAMPLE_HAS_FEATURE_PROPS_DICT_V2 # Ensure V2 props were passed


@pytest.mark.asyncio
async def test_merge_relationship_nodes_not_found(mock_neo4j_session):
    """Test merge_relationship when start or end node MATCH fails."""
    mock_cursor = mock_neo4j_session["cursor"]
    mock_session_obj = mock_neo4j_session["session"]
    # Simulate the underlying execute_merge_query returning None because MATCH failed
    mock_session_obj.execute_write.return_value = None # Or configure cursor.single to return None

    with pytest.raises(RelationshipCreationError, match="Start or end node likely not found"):
        await merge_relationship(
            start_node_label=PERSON_LABEL,
            start_node_props={"user_id": "non_existent_start"},
            end_node_label=ASTROFEATURE_LABEL,
            end_node_props=SAMPLE_ASTROFEATURE_PROPS, # Use valid end props for test focus
            relationship_type=HAS_FEATURE_REL
        )

@pytest.mark.asyncio
async def test_merge_relationship_dao_exception(mock_neo4j_session):
    mock_session_obj = mock_neo4j_session["session"]
    mock_session_obj.execute_write.side_effect = DAOException("DB error during relationship merge")

    with pytest.raises(RelationshipCreationError, match="Database error merging relationship"):
        await merge_relationship(
            start_node_label=PERSON_LABEL,
            start_node_props=SAMPLE_PERSON_PROPS,
            end_node_label=ASTROFEATURE_LABEL,
            end_node_props=SAMPLE_ASTROFEATURE_PROPS,
            relationship_type=HAS_FEATURE_REL
        )

@pytest.mark.asyncio
async def test_link_person_to_astrofeature_success(mock_neo4j_session):
    """Test the specific helper function calls merge_relationship correctly."""
    # Patch merge_relationship to isolate the linker function's logic
    with patch('services.kg.app.crud.relationship_crud.merge_relationship', AsyncMock(return_value={
        "type": HAS_FEATURE_REL, "properties": SAMPLE_HAS_FEATURE_PROPS_DICT_V1
    })) as mock_generic_merge:
        result = await link_person_to_astrofeature(
            SAMPLE_PERSON_USER_ID,
            SAMPLE_ASTROFEATURE_NAME,
            SAMPLE_ASTROFEATURE_TYPE, # Added type parameter
            SAMPLE_HAS_FEATURE_PROPS_MODEL_V1
        )
        assert result is not None
        assert result["type"] == HAS_FEATURE_REL
        mock_generic_merge.assert_called_once_with(
            start_node_label=PERSON_LABEL,
            start_node_props=SAMPLE_PERSON_PROPS,
            end_node_label=ASTROFEATURE_LABEL,
            end_node_props=SAMPLE_ASTROFEATURE_PROPS, # Check props include type
            relationship_type=HAS_FEATURE_REL,
            rel_props_model=SAMPLE_HAS_FEATURE_PROPS_MODEL_V1
        )


@pytest.mark.asyncio
async def test_get_relationships_from_node_success(mock_neo4j_session):
    mock_cursor = mock_neo4j_session["cursor"]
    mock_session_obj = mock_neo4j_session["session"]
    expected_rels_data = [
        {"type": HAS_FEATURE_REL, "properties": {"relevance": 0.8}, "end_node_props": {"name": "Feature1", "feature_type": "type1"}},
        {"type": HAS_FEATURE_REL, "properties": {"relevance": 0.9}, "end_node_props": {"name": "Feature2", "feature_type": "type1"}},
    ]
    mock_cursor.data.return_value = expected_rels_data

    rels = await get_relationships_from_node(
        node_label=PERSON_LABEL,
        node_props=SAMPLE_PERSON_PROPS,
        relationship_type=HAS_FEATURE_REL,
        direction="out"
    )
    assert len(rels) == 2
    assert rels[0]["type"] == HAS_FEATURE_REL
    assert rels[0]["end_node_props"]["name"] == "Feature1"
    mock_session_obj.execute_read.assert_called_once()

@pytest.mark.asyncio
async def test_get_relationships_from_node_dao_exception(mock_neo4j_session):
    mock_session_obj = mock_neo4j_session["session"]
    mock_session_obj.execute_read.side_effect = DAOException("DB error getting rels")

    rels = await get_relationships_from_node(PERSON_LABEL, SAMPLE_PERSON_PROPS)
    assert rels == []

@pytest.mark.asyncio
async def test_delete_relationship_success(mock_neo4j_session):
    mock_cursor = mock_neo4j_session["cursor"]
    mock_session_obj = mock_neo4j_session["session"]
    mock_tx = mock_neo4j_session["tx"] # Get mock_tx from the fixture yield
    mock_cursor.single.return_value = None # DELETE success

    # --- Act ---
    deleted = await delete_relationship(
        start_node_label=PERSON_LABEL,
        start_node_props=SAMPLE_PERSON_PROPS,
        end_node_label=ASTROFEATURE_LABEL,
        end_node_props=SAMPLE_ASTROFEATURE_PROPS, # Includes type
        relationship_type=HAS_FEATURE_REL
    )

    # --- Assert ---
    assert deleted is True

    # Assert mock calls *after* the function call
    mock_session_obj.execute_write.assert_called_once()
    mock_tx.run.assert_called_once()
    # Verify query uses composite key for end node
    called_query = mock_tx.run.call_args[0][0]
    assert f"end_node:{ASTROFEATURE_LABEL} {{name: $name_end, feature_type: $feature_type_end}}" in called_query.replace(" ","") # Check composite key match


@pytest.mark.asyncio
async def test_delete_relationship_dao_exception(mock_neo4j_session):
    mock_session_obj = mock_neo4j_session["session"]
    mock_session_obj.execute_write.side_effect = DAOException("DB error deleting rel")

    # delete_relationship now returns False on DAOException
    deleted = await delete_relationship(
        PERSON_LABEL, SAMPLE_PERSON_PROPS, ASTROFEATURE_LABEL, SAMPLE_ASTROFEATURE_PROPS, HAS_FEATURE_REL
    )
    assert deleted is False