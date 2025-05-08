import pytest
import json
from unittest.mock import AsyncMock, patch, MagicMock, ANY # Add ANY

# Import consumers and schemas (adjust paths if needed)
from services.kg.app.consumers.chart_consumer import ChartCalculatedConsumer
from services.kg.app.consumers.typology_consumer import TypologyAssessedConsumer
from services.chart_calc.app.schemas.request_schemas import CalculationRequest
from services.chart_calc.app.schemas.astrology_schemas import AstrologyChartResponse, PlanetaryPosition
from services.chart_calc.app.schemas.human_design_schemas import HumanDesignChartResponse, GateActivation, DefinedCenter, Channel
from services.kg.app.graph_schema.nodes import PersonNode, AstroFeatureNode, HDFeatureNode, TypologyResultNode

# --- Test ChartCalculatedConsumer ---

# Sample CHART_CALCULATED Event Data
SAMPLE_BIRTH_DATA = {"birth_date": "1995-08-10", "birth_time": "11:22:33", "latitude": 40.7128, "longitude": -74.0060}
SAMPLE_ASTRO_CHART = {
    "request_data": SAMPLE_BIRTH_DATA,
    "planetary_positions": [{"name": "Sun", "sign": "Leo", "longitude": 137.5, "latitude": 0.0, "speed": 0.98, "sign_longitude": 17.5, "house": 10, "retrograde": False}],
    "house_cusps": [{"house_number": 1, "longitude": 150.0, "sign": "Virgo", "sign_longitude": 0.0}],
    "aspects": [{"planet1": "Sun", "planet2": "Moon", "aspect_type": "Square", "orb": 2.1, "is_applying": False}],
    "north_node": None
}
SAMPLE_HD_CHART = {
    "request_data": SAMPLE_BIRTH_DATA,
    "type": "Projector", "strategy": "Wait for Invitation", "authority": "Splenic", "profile": "4/6", "definition": "Split",
    "incarnation_cross": "Some Cross", "motivation": None, "motivation_orientation": None, "perspective": None, "perspective_orientation": None,
    "conscious_sun_gate": {"gate_number": 10, "line_number": 4, "planet": "Sun", "is_conscious": True},
    "unconscious_sun_gate": {"gate_number": 15, "line_number": 4, "planet": "Sun", "is_conscious": False},
    "defined_centers": [{"name": "Ajna"}, {"name": "Spleen"}],
    "open_centers": ["Head", "Throat", "G", "Heart", "Sacral", "Root", "Solar Plexus"],
    "channels": [{"channel_number": "20-57", "name": "The Brainwave"}],
    "gates": [{"gate_number": 10, "line_number": 4, "planet": "Sun", "is_conscious": True}, {"gate_number": 20, "line_number": 1, "planet": "Earth", "is_conscious": True}],
    "environment": None
}
SAMPLE_CHART_EVENT_VALUE = {
    "calculation_type": "both", # Or 'astrology', 'human_design'
    "request_payload": SAMPLE_BIRTH_DATA,
    "result_summary": {
        "astrology_chart": SAMPLE_ASTRO_CHART,
        "human_design_chart": SAMPLE_HD_CHART
    },
    "timestamp": "2024-01-01T10:00:00Z",
    "service_version": "1.0"
}
SAMPLE_USER_ID = "chart_consumer_user_1"

@pytest.mark.asyncio
async def test_chart_consumer_process_message_success():
    """Test successful processing of a CHART_CALCULATED message."""
    consumer = ChartCalculatedConsumer()
    
    # Mock all CRUD functions called within process_message
    with patch('services.kg.app.consumers.chart_consumer.get_person_by_user_id', AsyncMock(return_value=None)) as mock_get_person, \
         patch('services.kg.app.consumers.chart_consumer.create_person', AsyncMock(return_value=PersonNode(user_id=SAMPLE_USER_ID))) as mock_create_person, \
         patch('services.kg.app.consumers.chart_consumer.get_astrofeature_by_name', AsyncMock(return_value=None)) as mock_get_astro, \
         patch('services.kg.app.consumers.chart_consumer.create_astrofeature', AsyncMock(return_value=AstroFeatureNode(name='Sun in Leo', feature_type='planet_in_sign', details={}))) as mock_create_astro, \
         patch('services.kg.app.consumers.chart_consumer.link_person_to_astrofeature', AsyncMock()) as mock_link_astro, \
         patch('services.kg.app.consumers.chart_consumer.get_hdfeature_by_name', AsyncMock(return_value=None)) as mock_get_hd, \
         patch('services.kg.app.consumers.chart_consumer.create_hdfeature', AsyncMock(return_value=HDFeatureNode(name='Projector', feature_type='hd_type', details={}))) as mock_create_hd, \
         patch('services.kg.app.consumers.chart_consumer.link_person_to_hdfeature', AsyncMock()) as mock_link_hd:

        await consumer.process_message(SAMPLE_USER_ID, SAMPLE_CHART_EVENT_VALUE)

        # Assertions: Check if CRUD functions were called correctly
        mock_get_person.assert_called_once_with(SAMPLE_USER_ID)
        mock_create_person.assert_called_once()
        # Check calls for specific features
        mock_get_astro.assert_any_call("Sun in Leo")
        mock_create_astro.assert_any_call(AstroFeatureNode(name='Sun in Leo', feature_type='planet_in_sign', details=ANY))
        mock_link_astro.assert_any_call(SAMPLE_USER_ID, "Sun in Leo", ANY)
        
        mock_get_hd.assert_any_call("Projector") # Example HD feature name
        mock_create_hd.assert_any_call(HDFeatureNode(name='Projector', feature_type='hd_type', details=ANY))
        mock_link_hd.assert_any_call(SAMPLE_USER_ID, "Projector", ANY)
        # Add more assertions for other features and relationships...

@pytest.mark.asyncio
async def test_chart_consumer_process_message_missing_key():
    """Test message processing skips if key (user_id) is missing."""
    consumer = ChartCalculatedConsumer()
    with patch('services.kg.app.consumers.chart_consumer.logger.error') as mock_logger:
        await consumer.process_message(None, SAMPLE_CHART_EVENT_VALUE)
        mock_logger.assert_called_with("CHART_CALCULATED event missing user_id in Kafka message key. Skipping.")

# Add more tests for chart consumer: existing person update, partial data, DAO errors etc.


# --- Test TypologyAssessedConsumer ---

# Sample TYPOLOGY_ASSESSED Event Data
SAMPLE_TYPOLOGY_EVENT_VALUE = {
    "assessment_id": "typology_assess_consumer_1",
    "typology_name": "TestEnneagram",
    "score": "Type 5",
    "confidence": 0.92,
    "trace": {"wing": "4", "level": "Healthy"},
    "timestamp": "2024-01-02T11:00:00Z"
    # Assuming user_id comes from Kafka key
}
SAMPLE_TYPOLOGY_USER_ID = "typology_consumer_user_1"

@pytest.mark.asyncio
async def test_typology_consumer_process_message_success():
    """Test successful processing of a TYPOLOGY_ASSESSED message."""
    consumer = TypologyAssessedConsumer()

    with patch('services.kg.app.consumers.typology_consumer.get_person_by_user_id', AsyncMock(return_value=None)) as mock_get_person, \
         patch('services.kg.app.consumers.typology_consumer.create_person', AsyncMock(return_value=PersonNode(user_id=SAMPLE_TYPOLOGY_USER_ID))) as mock_create_person, \
         patch('services.kg.app.consumers.typology_consumer.get_typology_result_by_assessment_id', AsyncMock(return_value=None)) as mock_get_typology, \
         patch('services.kg.app.consumers.typology_consumer.create_typology_result', AsyncMock(return_value=TypologyResultNode(**SAMPLE_TYPOLOGY_EVENT_VALUE))) as mock_create_typology, \
         patch('services.kg.app.consumers.typology_consumer.link_person_to_typologyresult', AsyncMock()) as mock_link_typology:
        
        await consumer.process_message(SAMPLE_TYPOLOGY_USER_ID, SAMPLE_TYPOLOGY_EVENT_VALUE)

        mock_get_person.assert_called_once_with(SAMPLE_TYPOLOGY_USER_ID)
        mock_create_person.assert_called_once()
        mock_get_typology.assert_called_once_with("typology_assess_consumer_1")
        mock_create_typology.assert_called_once()
        # Check payload passed to create_typology_result
        create_call_args = mock_create_typology.call_args[0][0]
        assert isinstance(create_call_args, TypologyResultNode)
        assert create_call_args.assessment_id == "typology_assess_consumer_1"
        assert create_call_args.score == "Type 5"
        # Pydantic v2 Json type automatically parses the string back to dict
        assert create_call_args.details == {"wing": "4", "level": "Healthy"}
        
        mock_link_typology.assert_called_once()

@pytest.mark.asyncio
async def test_typology_consumer_process_message_missing_fields():
    """Test message processing skips if required fields are missing."""
    consumer = TypologyAssessedConsumer()
    invalid_payload = SAMPLE_TYPOLOGY_EVENT_VALUE.copy()
    del invalid_payload["assessment_id"]
    with patch('services.kg.app.consumers.typology_consumer.logger.error') as mock_logger:
        await consumer.process_message(SAMPLE_TYPOLOGY_USER_ID, invalid_payload)
        mock_logger.assert_called_with(ANY, exc_info=False) # Check log message content if needed

# Add more tests for typology consumer: existing result, DAO errors etc.


# --- Integration Tests Placeholder ---
# These would require setting up testcontainers for Kafka and Neo4j

# @pytest.mark.integration
# @pytest.mark.asyncio
# async def test_kafka_to_neo4j_chart_flow(kafka_producer_test, neo4j_test_session):
#     """
#     Integration test: Produce CHART_CALCULATED event, verify data in Neo4j.
#     - kafka_producer_test: Fixture providing a Kafka producer connected to test Kafka.
#     - neo4j_test_session: Fixture providing a Neo4j session connected to test Neo4j.
#     """
#     # 1. Produce sample CHART_CALCULATED message using kafka_producer_test
#     user_id = "integration_test_user_chart"
#     await kafka_producer_test.send("CHART_CALCULATED", key=user_id, value=SAMPLE_CHART_EVENT_VALUE)
    
#     # 2. Wait for consumer to process (add appropriate delay or check mechanism)
#     await asyncio.sleep(5) # Simple delay, better to check for results

#     # 3. Query Neo4j using neo4j_test_session to verify nodes/relationships
#     result = neo4j_test_session.run("MATCH (p:Person {user_id: $uid})-[:HAS_FEATURE]->(f:AstroFeature {name: $fname}) RETURN p, f", 
#                                     uid=user_id, fname="Sun in Leo")
#     record = result.single()
#     assert record is not None
#     assert record["p"]["user_id"] == user_id
#     assert record["f"]["name"] == "Sun in Leo"
    
#     # 4. Cleanup test data in Neo4j and Kafka topics if necessary

# @pytest.mark.integration
# @pytest.mark.asyncio
# async def test_kafka_to_neo4j_typology_flow(kafka_producer_test, neo4j_test_session):
#     """
#     Integration test: Produce TYPOLOGY_ASSESSED event, verify data in Neo4j.
#     """
#     # Similar structure to the chart flow test...
#     pass