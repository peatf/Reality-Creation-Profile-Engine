import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, patch, ANY

from services.kg.app.utils.query_utils import (
    find_persons_with_features,
    count_persons_influenced_by_astrofeature,
    traverse_relationships,
    export_person_subgraph_cypher
)
from services.kg.app.crud.base_dao import DAOException
from services.kg.app.graph_schema.schema_setup import (
    PERSON_LABEL, ASTROFEATURE_LABEL, HDFEATURE_LABEL, HAS_FEATURE_REL, INFLUENCES_REL
)

# Sample data for mocking responses
SAMPLE_PERSON_PROPS = {"user_id": "user1", "name": "Test User 1"}
SAMPLE_ASTRO_PROPS = {"name": "Sun in Leo", "feature_type": "planet_in_sign"}
SAMPLE_HD_PROPS = {"name": "Generator", "feature_type": "hd_type"}

@pytest.mark.asyncio
async def test_find_persons_with_features_astro_only(mock_neo4j_driver):
    """Test finding persons with only an AstroFeature."""
    mock_execute_read = AsyncMock(return_value=[{"p": SAMPLE_PERSON_PROPS, "af": SAMPLE_ASTRO_PROPS}])
    with patch('services.kg.app.utils.query_utils.execute_read_query', mock_execute_read):
        results = await find_persons_with_features(astro_feature_name="Sun in Leo")

        assert len(results) == 1
        assert results[0]["p"] == SAMPLE_PERSON_PROPS
        assert results[0]["af"] == SAMPLE_ASTRO_PROPS
        mock_execute_read.assert_called_once()
        # Check query structure (simplified check)
        called_query = mock_execute_read.call_args[0][0]
        assert f"MATCH (p:{PERSON_LABEL})-[:{HAS_FEATURE_REL}]->(af:{ASTROFEATURE_LABEL}" in called_query
        assert f"RETURN p, af" in called_query

@pytest.mark.asyncio
async def test_find_persons_with_features_hd_only(mock_neo4j_driver):
    """Test finding persons with only an HDFeature."""
    mock_execute_read = AsyncMock(return_value=[{"p": SAMPLE_PERSON_PROPS, "hf": SAMPLE_HD_PROPS}])
    with patch('services.kg.app.utils.query_utils.execute_read_query', mock_execute_read):
        results = await find_persons_with_features(hd_feature_name="Generator")

        assert len(results) == 1
        assert results[0]["p"] == SAMPLE_PERSON_PROPS
        assert results[0]["hf"] == SAMPLE_HD_PROPS
        mock_execute_read.assert_called_once()
        called_query = mock_execute_read.call_args[0][0]
        assert f"MATCH (p:{PERSON_LABEL})-[:{HAS_FEATURE_REL}]->(hf:{HDFEATURE_LABEL}" in called_query
        assert f"RETURN p, hf" in called_query

@pytest.mark.asyncio
async def test_find_persons_with_features_both(mock_neo4j_driver):
    """Test finding persons with both AstroFeature and HDFeature."""
    mock_execute_read = AsyncMock(return_value=[{"p": SAMPLE_PERSON_PROPS, "af": SAMPLE_ASTRO_PROPS, "hf": SAMPLE_HD_PROPS}])
    with patch('services.kg.app.utils.query_utils.execute_read_query', mock_execute_read):
        results = await find_persons_with_features(astro_feature_name="Sun in Leo", hd_feature_name="Generator")

        assert len(results) == 1
        assert results[0]["p"] == SAMPLE_PERSON_PROPS
        assert results[0]["af"] == SAMPLE_ASTRO_PROPS
        assert results[0]["hf"] == SAMPLE_HD_PROPS
        mock_execute_read.assert_called_once()
        called_query = mock_execute_read.call_args[0][0]
        assert f"MATCH (p:{PERSON_LABEL})-[:{HAS_FEATURE_REL}]->(af:{ASTROFEATURE_LABEL}" in called_query
        assert f"(p)-[:{HAS_FEATURE_REL}]->(hf:{HDFEATURE_LABEL}" in called_query
        assert f"RETURN p, af, hf" in called_query

@pytest.mark.asyncio
async def test_find_persons_with_features_none_found(mock_neo4j_driver):
    """Test finding persons when no matches are found."""
    mock_execute_read = AsyncMock(return_value=[])
    with patch('services.kg.app.utils.query_utils.execute_read_query', mock_execute_read):
        results = await find_persons_with_features(astro_feature_name="NonExistentFeature")
        assert results == []

@pytest.mark.asyncio
async def test_find_persons_with_features_no_input(mock_neo4j_driver):
    """Test calling find_persons_with_features with no feature names."""
    results = await find_persons_with_features()
    assert results == []
    # execute_read_query should not be called
    with patch('services.kg.app.utils.query_utils.execute_read_query', AsyncMock()) as mock_exec:
         await find_persons_with_features()
         mock_exec.assert_not_called()

@pytest.mark.asyncio
async def test_count_persons_influenced_success(mock_neo4j_driver):
    """Test counting persons influenced by an AstroFeature."""
    mock_execute_read = AsyncMock(return_value=[{"person_count": 5}])
    with patch('services.kg.app.utils.query_utils.execute_read_query', mock_execute_read):
        count = await count_persons_influenced_by_astrofeature("Influential Feature")
        assert count == 5
        mock_execute_read.assert_called_once()
        called_query = mock_execute_read.call_args[0][0]
        assert f"MATCH (influencer_af:{ASTROFEATURE_LABEL}" in called_query
        assert f"-[:{INFLUENCES_REL}]->" in called_query
        assert f"RETURN count(DISTINCT p) as person_count" in called_query

@pytest.mark.asyncio
async def test_count_persons_influenced_zero(mock_neo4j_driver):
    """Test counting persons when none are influenced."""
    mock_execute_read = AsyncMock(return_value=[{"person_count": 0}]) # Query succeeds but count is 0
    with patch('services.kg.app.utils.query_utils.execute_read_query', mock_execute_read):
        count = await count_persons_influenced_by_astrofeature("NonInfluential Feature")
        assert count == 0

@pytest.mark.asyncio
async def test_count_persons_influenced_dao_exception(mock_neo4j_driver):
    """Test DAOException during count."""
    mock_execute_read = AsyncMock(side_effect=DAOException("DB error"))
    with patch('services.kg.app.utils.query_utils.execute_read_query', mock_execute_read):
        count = await count_persons_influenced_by_astrofeature("Some Feature")
        assert count == 0 # Should return 0 on error

@pytest.mark.asyncio
async def test_traverse_relationships_success(mock_neo4j_driver):
    """Test successful relationship traversal."""
    # Mocking a Path object is complex. We'll return a list of dicts simulating path data.
    mock_path_data = [{"path": "simulated_path_object_1"}, {"path": "simulated_path_object_2"}]
    mock_execute_read = AsyncMock(return_value=mock_path_data)
    with patch('services.kg.app.utils.query_utils.execute_read_query', mock_execute_read):
        results = await traverse_relationships(
            start_node_label=PERSON_LABEL,
            start_node_identifier_property="user_id",
            start_node_identifier_value="user1",
            max_hops=2,
            relationship_types=[HAS_FEATURE_REL]
        )
        assert len(results) == 2
        assert results == mock_path_data # Check if raw path data is returned
        mock_execute_read.assert_called_once()
        called_query = mock_execute_read.call_args[0][0]
        assert f"MATCH path = (start_node:{PERSON_LABEL} {{user_id: $start_val}})" in called_query
        # Check key parts of the query pattern
        assert f":{HAS_FEATURE_REL}*1..2" in called_query # Check hops and type
        assert "->" in called_query # Check direction
        assert "RETURN path" in called_query

@pytest.mark.asyncio
async def test_traverse_relationships_invalid_hops(mock_neo4j_driver):
    """Test traversal with invalid hop values."""
    with pytest.raises(ValueError, match="Invalid min_hops or max_hops"):
        await traverse_relationships("Label", "prop", "val", min_hops=-1)
    with pytest.raises(ValueError, match="Invalid min_hops or max_hops"):
        await traverse_relationships("Label", "prop", "val", min_hops=2, max_hops=1)

@pytest.mark.asyncio
async def test_traverse_relationships_invalid_direction(mock_neo4j_driver):
    """Test traversal with invalid direction."""
    with pytest.raises(ValueError, match="Invalid direction"):
        await traverse_relationships("Label", "prop", "val", direction="sideways")

@pytest.mark.asyncio
async def test_export_person_subgraph_cypher_success(mock_neo4j_driver):
    """Test successful Cypher script generation for a person's subgraph."""
    # Mock the two execute_read_query calls
    mock_person_data = [{"p": {"user_id": "export_user", "name": "Export Me"}}]
    mock_feature_data = [
        {
            "f": {"name": "Feature A", "feature_type": "type1"},
            "rel_type": HAS_FEATURE_REL,
            "rel_props": {"source_calculation_id": "calc1"},
            "feature_labels": [ASTROFEATURE_LABEL]
        },
        {
            "f": {"name": "Feature B", "feature_type": "type2"},
            "rel_type": HAS_FEATURE_REL,
            "rel_props": {}, # Empty props
            "feature_labels": [HDFEATURE_LABEL]
        }
    ]
    
    # Use side_effect to return different values for consecutive calls
    mock_execute_read = AsyncMock(side_effect=[mock_person_data, mock_feature_data])

    with patch('services.kg.app.utils.query_utils.execute_read_query', mock_execute_read):
        cypher_script = await export_person_subgraph_cypher("export_user")

        assert "CREATE (p_export_user:Person {user_id: 'export_user', name: 'Export Me'});" in cypher_script
        assert f"CREATE (astrofeature_Feature_A:{ASTROFEATURE_LABEL} {{name: 'Feature A', feature_type: 'type1'}});" in cypher_script
        assert f"CREATE (hdfeature_Feature_B:{HDFEATURE_LABEL} {{name: 'Feature B', feature_type: 'type2'}});" in cypher_script
        assert f"CREATE (p_export_user)-[:{HAS_FEATURE_REL} {{source_calculation_id: 'calc1'}}]->(astrofeature_Feature_A);" in cypher_script
        assert f"CREATE (p_export_user)-[:{HAS_FEATURE_REL}]->(hdfeature_Feature_B);" in cypher_script # No props for this rel
        
        assert mock_execute_read.call_count == 2

@pytest.mark.asyncio
async def test_export_person_subgraph_cypher_person_not_found(mock_neo4j_driver):
    """Test Cypher export when the person node is not found."""
    mock_execute_read = AsyncMock(return_value=[]) # Simulate person not found
    with patch('services.kg.app.utils.query_utils.execute_read_query', mock_execute_read):
        cypher_script = await export_person_subgraph_cypher("non_existent_export_user")
        assert "// Person with user_id 'non_existent_export_user' not found." in cypher_script
        mock_execute_read.assert_called_once() # Only the first query runs