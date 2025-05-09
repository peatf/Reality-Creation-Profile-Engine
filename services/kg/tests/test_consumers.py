import pytest
import pytest_asyncio
import json
import asyncio # For asyncio.sleep
from unittest.mock import AsyncMock, patch, MagicMock, ANY, call
from datetime import datetime

from pydantic import ValidationError
from neo4j.exceptions import ServiceUnavailable as Neo4jServiceUnavailable
from aiokafka.errors import KafkaError # Import base KafkaError instead

# Import consumers and schemas (adjust paths if needed)
from services.kg.app.consumers.chart_consumer import ChartCalculatedConsumer, CHART_CALCULATED_TOPIC
from services.kg.app.consumers.typology_consumer import TypologyAssessedConsumer, TOPIC_TYPOLOGY_ASSESSED
from services.chart_calc.app.schemas.request_schemas import CalculationRequest
from services.chart_calc.app.schemas.astrology_schemas import AstrologyChartResponse, PlanetaryPosition
from services.chart_calc.app.schemas.human_design_schemas import HumanDesignChartResponse, GateActivation, DefinedCenter, Channel
from services.kg.app.graph_schema.nodes import PersonNode, AstroFeatureNode, HDFeatureNode, TypologyResultNode
from services.kg.app.crud.base_dao import NodeCreationError, UniqueConstraintViolationError, DAOException

# --- Test ChartCalculatedConsumer ---

# Sample CHART_CALCULATED Event Data (can be moved to conftest if shared more widely)
SAMPLE_BIRTH_DATA = {"birth_date": "1995-08-10", "birth_time": "11:22:33", "latitude": 40.7128, "longitude": -74.0060}
SAMPLE_ASTRO_CHART_DATA = { # Renamed to avoid conflict with Pydantic model name
    "request_data": SAMPLE_BIRTH_DATA,
    "planetary_positions": [{"name": "Sun", "sign": "Leo", "longitude": 137.5, "latitude": 0.0, "speed": 0.98, "sign_longitude": 17.5, "house": 10, "retrograde": False}],
    "house_cusps": [{"house_number": 1, "longitude": 150.0, "sign": "Virgo", "sign_longitude": 0.0}],
    "aspects": [{"planet1": "Sun", "planet2": "Moon", "aspect_type": "Square", "orb": 2.1, "is_applying": False}],
    "north_node": None # Keep as None, or provide a valid dict if testing North Node processing
}
SAMPLE_HD_CHART_DATA = { # Renamed
    "request_data": SAMPLE_BIRTH_DATA,
    "type": "Projector", "strategy": "Wait for Invitation", "authority": "Splenic", "profile": "4/6", "definition": "Split",
    "incarnation_cross": "Some Cross", "motivation": None, "motivation_orientation": None, "perspective": None, "perspective_orientation": None,
    "conscious_sun_gate": {"gate_number": 10, "line_number": 4, "planet": "Sun", "is_conscious": True},
    "unconscious_sun_gate": {"gate_number": 15, "line_number": 4, "planet": "Sun", "is_conscious": False},
    "defined_centers": [{"name": "Ajna"}, {"name": "Spleen"}],
    "open_centers": ["Head", "Throat", "G", "Heart", "Sacral", "Root", "Solar Plexus"],
    "channels": [{"channel_number": "20-57", "name": "The Brainwave"}],
    "gates": [{"gate_number": 10, "line_number": 4, "planet": "Sun", "is_conscious": True}, {"gate_number": 20, "line_number": 1, "planet": "Earth", "is_conscious": True}],
    "environment": None,
    "g_center_access_type": "Projector Defined", # Added for completeness
    "g_center_access_definition": "Accesses G via Spleen" # Added
}
SAMPLE_CHART_EVENT_VALUE = {
    "calculation_type": "both",
    "request_payload": SAMPLE_BIRTH_DATA,
    "result_summary": {
        "astrology_chart": SAMPLE_ASTRO_CHART_DATA,
        "human_design_chart": SAMPLE_HD_CHART_DATA
    },
    "timestamp": "2024-01-01T10:00:00Z",
    "service_version": "1.0"
}
SAMPLE_USER_ID = "chart_consumer_user_1"

@pytest_asyncio.fixture # Changed to async fixture
async def chart_consumer_instance(mock_aio_kafka_consumer_factory, mock_aio_kafka_producer_factory): # Added async
    """Fixture to create a ChartCalculatedConsumer instance with mocked Kafka clients."""
    consumer = ChartCalculatedConsumer()
    # Replace the consumer's Kafka clients with mocks from the factories
    consumer.consumer = mock_aio_kafka_consumer_factory
    consumer.dlq_producer = mock_aio_kafka_producer_factory
    return consumer

class TestChartCalculatedConsumer:

    @pytest.mark.asyncio
    async def test_process_message_success(self, chart_consumer_instance, mock_consumer_record_factory, mock_kafka_settings):
        """Test successful processing of a CHART_CALCULATED message."""
        consumer = chart_consumer_instance
        mock_message = mock_consumer_record_factory(
            topic=CHART_CALCULATED_TOPIC,
            partition=0, offset=0,
            key=SAMPLE_USER_ID, value=SAMPLE_CHART_EVENT_VALUE
        )
        # Configure the consumer mock to yield this message
        consumer.consumer.__aiter__.return_value = [mock_message]

        # Mock all CRUD functions used by the consumer
        with patch('services.kg.app.consumers.chart_consumer.get_person_by_user_id', AsyncMock(return_value=None)) as mock_get_person, \
             patch('services.kg.app.consumers.chart_consumer.upsert_person', AsyncMock(return_value=PersonNode(user_id=SAMPLE_USER_ID, birth_datetime_utc=datetime.now(), birth_latitude=0.0, birth_longitude=0.0))) as mock_upsert_person, \
             patch('services.kg.app.consumers.chart_consumer.update_person_properties', AsyncMock(return_value=PersonNode(user_id=SAMPLE_USER_ID, birth_datetime_utc=datetime.now(), birth_latitude=0.0, birth_longitude=0.0))) as mock_update_person_props, \
             patch('services.kg.app.consumers.chart_consumer.get_astrofeature_by_name_and_type', AsyncMock(return_value=None)) as mock_get_astro, \
             patch('services.kg.app.consumers.chart_consumer.upsert_astrofeature', AsyncMock(return_value=AstroFeatureNode(name='Sun in Leo', feature_type='planet_in_sign', details="{}"))) as mock_upsert_astro, \
             patch('services.kg.app.consumers.chart_consumer.link_person_to_astrofeature', AsyncMock()) as mock_link_astro, \
             patch('services.kg.app.consumers.chart_consumer.get_hdfeature_by_name_and_type', AsyncMock(return_value=None)) as mock_get_hd, \
             patch('services.kg.app.consumers.chart_consumer.upsert_hdfeature', AsyncMock(return_value=HDFeatureNode(name='Projector', feature_type='hd_type', details="{}"))) as mock_upsert_hd, \
             patch('services.kg.app.consumers.chart_consumer.link_person_to_hdfeature', AsyncMock()) as mock_link_hd:

            # Run the consumer loop for one message
            consumer_task = asyncio.create_task(consumer.consume())
            await asyncio.sleep(0.5) # Increased sleep duration
            consumer._running = False # Signal consumer to stop
            await consumer_task # Wait for consumer to finish
 
            # Assertions
            mock_get_person.assert_called_once_with(SAMPLE_USER_ID)
            mock_upsert_person.assert_called_once() # Check upsert_person call
            mock_update_person_props.assert_not_called() # Should not be called if person was created

            # Check calls for specific features (example)
            mock_get_astro.assert_any_call("Sun in Leo", "planet_in_sign") # Check type argument
            mock_upsert_astro.assert_any_call(AstroFeatureNode(name='Sun in Leo', feature_type='planet_in_sign', details=ANY)) # Check upsert_astrofeature call
            mock_link_astro.assert_any_call(SAMPLE_USER_ID, "Sun in Leo", "planet_in_sign", ANY) # Check type argument

            mock_get_hd.assert_any_call("Projector", "hd_type") # Check type argument
            mock_upsert_hd.assert_any_call(HDFeatureNode(name='Projector', feature_type='hd_type', details=ANY)) # Check upsert_hdfeature call
            mock_link_hd.assert_any_call(SAMPLE_USER_ID, "Projector", "hd_type", ANY) # Check type argument

            consumer.consumer.commit.assert_called_once() # Check commit was called
            consumer.dlq_producer.send_and_wait.assert_not_called() # Ensure DLQ was not used

    @pytest.mark.asyncio
    async def test_process_message_missing_key(self, chart_consumer_instance, mock_consumer_record_factory, mock_kafka_settings):
        """Test message processing sends to DLQ if key (user_id) is missing."""
        consumer = chart_consumer_instance
        # Create a message with a None key
        malformed_message = mock_consumer_record_factory(
            topic=CHART_CALCULATED_TOPIC, partition=0, offset=1,
            key=None, value=SAMPLE_CHART_EVENT_VALUE
        )
        consumer.consumer.__aiter__.return_value = [malformed_message]

        with patch('services.kg.app.consumers.chart_consumer.logger.error') as mock_logger_error, \
             patch('services.kg.app.consumers.chart_consumer.logger.warning') as mock_logger_warning:

            consumer_task = asyncio.create_task(consumer.consume())
            await asyncio.sleep(0.1)
            consumer._running = False
            await consumer_task

            # Check that an error was logged by process_message (ValueError raised)
            # and then process_message_with_retry logged it and sent to DLQ
            mock_logger_error.assert_any_call("CHART_CALCULATED event missing user_id in Kafka message key.")
            mock_logger_warning.assert_any_call(
                f"Sending message for key None from topic {CHART_CALCULATED_TOPIC} to DLQ topic {mock_kafka_settings.chart_consumer_dlq_topic} due to value_error_malformed_message"
            )
            consumer.dlq_producer.send_and_wait.assert_called_once()
            dlq_call_args = consumer.dlq_producer.send_and_wait.call_args
            assert dlq_call_args[0][0] == mock_kafka_settings.chart_consumer_dlq_topic
            assert dlq_call_args[1]['key'] is None
            assert dlq_call_args[1]['value']['error_type'] == "value_error_malformed_message"
            consumer.consumer.commit.assert_called_once() # Commit should happen after DLQ

    @pytest.mark.asyncio
    async def test_process_message_missing_request_payload(self, chart_consumer_instance, mock_consumer_record_factory, mock_kafka_settings):
        """Test message with missing 'request_payload' is sent to DLQ."""
        consumer = chart_consumer_instance
        invalid_event_value = SAMPLE_CHART_EVENT_VALUE.copy()
        del invalid_event_value["request_payload"]

        malformed_message = mock_consumer_record_factory(
            topic=CHART_CALCULATED_TOPIC, partition=0, offset=2,
            key=SAMPLE_USER_ID, value=invalid_event_value
        )
        consumer.consumer.__aiter__.return_value = [malformed_message]

        with patch('services.kg.app.consumers.chart_consumer.logger.error') as mock_logger_error, \
             patch('services.kg.app.consumers.chart_consumer.logger.warning') as mock_logger_warning:

            consumer_task = asyncio.create_task(consumer.consume())
            await asyncio.sleep(0.1)
            consumer._running = False
            await consumer_task

            # process_message raises ValueError, process_message_with_retry catches it and sends to DLQ
            mock_logger_error.assert_any_call(f"Missing 'request_payload' in CHART_CALCULATED event for user {SAMPLE_USER_ID}.")
            mock_logger_warning.assert_any_call(
                f"Sending message for key {SAMPLE_USER_ID} from topic {CHART_CALCULATED_TOPIC} to DLQ topic {mock_kafka_settings.chart_consumer_dlq_topic} due to value_error_malformed_message"
            )
            consumer.dlq_producer.send_and_wait.assert_called_once()
            dlq_call_args = consumer.dlq_producer.send_and_wait.call_args
            assert dlq_call_args[0][0] == mock_kafka_settings.chart_consumer_dlq_topic
            assert dlq_call_args[1]['key'] == SAMPLE_USER_ID
            assert dlq_call_args[1]['value']['error_type'] == "value_error_malformed_message"
            assert f"Missing 'request_payload' in CHART_CALCULATED event for user {SAMPLE_USER_ID}" in dlq_call_args[1]['value']['error_message']
            consumer.consumer.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_message_malformed_json(self, chart_consumer_instance, mock_consumer_record_factory, mock_kafka_settings):
        """Test malformed JSON message is sent to DLQ by the main consume loop."""
        consumer = chart_consumer_instance
        malformed_json_bytes = b"{'this is not valid json" # Intentionally malformed

        # Create a mock record where the value is raw bytes that will fail JSON deserialization
        # The consumer's value_deserializer is `lambda v: json.loads(v.decode('utf-8'))`
        # So, we need to provide bytes that, when decoded, are not valid JSON.
        raw_message_record = mock_consumer_record_factory(
            topic=CHART_CALCULATED_TOPIC, partition=0, offset=3,
            key=SAMPLE_USER_ID, value=malformed_json_bytes # Pass raw bytes
        )
        # We need to mock the consumer's value_deserializer to raise JSONDecodeError
        # OR ensure the mock_consumer_record_factory produces a value that the *actual* deserializer fails on.
        # The latter is better for testing the consumer's actual behavior.
        # The `mock_consumer_record_factory` already encodes dicts to JSON bytes.
        # For this test, we directly set the value to malformed_json_bytes.

        # To simulate the deserializer in AIOKafkaConsumer raising an error *before* process_message_with_retry:
        # We'll have the consumer's __aiter__ yield a message that *would* cause the internal deserializer to fail.
        # The `value_deserializer` in the ChartCalculatedConsumer is `lambda v: json.loads(v.decode('utf-8'))`.
        # So, the `raw_message_record.value` should be bytes that, when decoded, are invalid JSON.

        # Re-create the record with the value that will cause the consumer's deserializer to fail.
        # The `mock_consumer_record_factory` normally json.dumps dicts. We need to bypass that for this test.
        # Let's make a special record for this.
        from aiokafka.structs import ConsumerRecord
        broken_message = ConsumerRecord(
            topic=CHART_CALCULATED_TOPIC, partition=0, offset=3,
            timestamp=123, timestamp_type=0, key=SAMPLE_USER_ID.encode('utf-8'), value=malformed_json_bytes,
            checksum=None, serialized_key_size=1, serialized_value_size=1, headers=[]
        )
        consumer.consumer.__aiter__.return_value = [broken_message]

        with patch('services.kg.app.consumers.chart_consumer.logger.error') as mock_logger_error, \
             patch('services.kg.app.consumers.chart_consumer.logger.warning') as mock_logger_warning, \
             patch('services.kg.app.consumers.chart_consumer.logger.critical') as mock_logger_critical:

            consumer_task = asyncio.create_task(consumer.consume())
            await asyncio.sleep(0.1)
            consumer._running = False
            await consumer_task

            # The error should be logged by the main consume loop's JSONDecodeError handler
            mock_logger_error.assert_any_call(
                f"FATAL: JSONDecodeError directly in consume loop for {CHART_CALCULATED_TOPIC} P:0 O:3. Value (raw): {malformed_json_bytes}. Error: {ANY}",
                exc_info=True
            )
            # DLQ send should be attempted
            mock_logger_warning.assert_any_call(
                f"Sending message for key {SAMPLE_USER_ID} from topic {CHART_CALCULATED_TOPIC} to DLQ topic {mock_kafka_settings.chart_consumer_dlq_topic} due to json_decode_error_in_consume_loop"
            )
            consumer.dlq_producer.send_and_wait.assert_called_once()
            dlq_call_args = consumer.dlq_producer.send_and_wait.call_args
            assert dlq_call_args[0][0] == mock_kafka_settings.chart_consumer_dlq_topic
            assert dlq_call_args[1]['key'] == SAMPLE_USER_ID
            assert dlq_call_args[1]['value']['error_type'] == "json_decode_error_in_consume_loop"
            # The value sent to DLQ would be the malformed bytes, possibly decoded with errors.
            assert malformed_json_bytes.decode('utf-8', 'replace') in dlq_call_args[1]['value']['original_value']
            consumer.consumer.commit.assert_called_once() # Commit after DLQ

    @pytest.mark.asyncio
    async def test_process_message_dao_transient_error_retry_success(self, chart_consumer_instance, mock_consumer_record_factory, mock_kafka_settings):
        """Test transient DAO error (Neo4jServiceUnavailable) leads to retry and eventual success."""
        consumer = chart_consumer_instance
        mock_message = mock_consumer_record_factory(
            topic=CHART_CALCULATED_TOPIC, partition=0, offset=4,
            key=SAMPLE_USER_ID, value=SAMPLE_CHART_EVENT_VALUE
        )
        consumer.consumer.__aiter__.return_value = [mock_message]

        # Mock get_person_by_user_id to fail first, then succeed
        mock_get_person = AsyncMock(side_effect=[
            Neo4jServiceUnavailable("Connection lost"), # First call fails
            PersonNode(user_id=SAMPLE_USER_ID, birth_datetime_utc=datetime.now(), birth_latitude=0.0, birth_longitude=0.0) # Second call succeeds
        ])

        with patch('services.kg.app.consumers.chart_consumer.get_person_by_user_id', mock_get_person), \
             patch('services.kg.app.consumers.chart_consumer.upsert_person', AsyncMock(return_value=PersonNode(user_id=SAMPLE_USER_ID, birth_datetime_utc=datetime.now(), birth_latitude=0.0, birth_longitude=0.0))) as mock_upsert_person, \
             patch('services.kg.app.consumers.chart_consumer.update_person_properties', AsyncMock()) as mock_update_person_props, \
             patch('services.kg.app.consumers.chart_consumer.get_astrofeature_by_name_and_type', AsyncMock(return_value=None)) as mock_get_astro, \
             patch('services.kg.app.consumers.chart_consumer.upsert_astrofeature', AsyncMock(return_value=AstroFeatureNode(name='Sun in Leo', feature_type='planet_in_sign', details="{}"))) as mock_upsert_astro, \
             patch('services.kg.app.consumers.chart_consumer.link_person_to_astrofeature', AsyncMock()) as mock_link_astro, \
             patch('services.kg.app.consumers.chart_consumer.get_hdfeature_by_name_and_type', AsyncMock(return_value=None)) as mock_get_hd, \
             patch('services.kg.app.consumers.chart_consumer.upsert_hdfeature', AsyncMock(return_value=HDFeatureNode(name='Projector', feature_type='hd_type', details="{}"))) as mock_upsert_hd, \
             patch('services.kg.app.consumers.chart_consumer.link_person_to_hdfeature', AsyncMock()) as mock_link_hd, \
             patch('services.kg.app.consumers.chart_consumer.logger.info') as mock_logger_info, \
             patch('services.kg.app.consumers.chart_consumer.logger.error') as mock_logger_error:

            consumer_task = asyncio.create_task(consumer.consume())
            await asyncio.sleep(0.2) # Allow time for retries
            consumer._running = False
            await consumer_task

            # Check that get_person_by_user_id was called twice (original + 1 retry)
            assert mock_get_person.call_count == 2
            # Check that create_person was called (since get_person eventually returned a value, no update needed if it was None initially)
            # If get_person_by_user_id returned None on the first attempt (after retry), then create_person would be called.
            # In this setup, get_person_by_user_id succeeds on the 2nd attempt, so create_person is NOT called if the first successful get_person returns a node.
            # Let's adjust the mock_get_person side_effect to be more explicit:
            # First call: Neo4jServiceUnavailable
            # Second call (retry): Returns None (person doesn't exist yet)
            # Then create_person should be called.

            # Reset mocks and re-run with clearer side_effect for get_person
            mock_get_person.reset_mock(side_effect=True)
            mock_get_person.side_effect = [
                Neo4jServiceUnavailable("Connection lost"), # First call
                None,                                     # Second call (retry) - person not found
                PersonNode(user_id=SAMPLE_USER_ID, birth_datetime_utc=datetime.now(), birth_latitude=0.0, birth_longitude=0.0) # Third call (after create_person) - this won't happen if create_person is successful
            ]
            mock_upsert_person.reset_mock()
            mock_update_person_props.reset_mock()
            consumer.dlq_producer.send_and_wait.reset_mock()
            consumer.consumer.commit.reset_mock()
            mock_logger_info.reset_mock()

            # Re-run the consume logic
            consumer.consumer.__aiter__.return_value = [mock_message] # Reset iterator
            consumer._running = True # Reset running flag
            consumer_task = asyncio.create_task(consumer.consume())
            await asyncio.sleep(0.2) # Allow time for retries
            consumer._running = False
            await consumer_task

            assert mock_get_person.call_count == 2 # Original + 1 retry
            mock_upsert_person.assert_called_once() # Called after get_person returned None on retry
            mock_update_person_props.assert_not_called() # upsert_person was called

            # Verify logs for retry attempts
            mock_logger_info.assert_any_call(f"Attempting to process message for key {SAMPLE_USER_ID}, attempt: 1")
            mock_logger_info.assert_any_call(f"Attempting to process message for key {SAMPLE_USER_ID}, attempt: 2")
            mock_logger_info.assert_any_call(f"Successfully processed message for key {SAMPLE_USER_ID} after 2 attempts.")

            consumer.dlq_producer.send_and_wait.assert_not_called()
            consumer.consumer.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_message_dao_transient_error_retry_exhausted_dlq(self, chart_consumer_instance, mock_consumer_record_factory, mock_kafka_settings):
        """Test transient DAO error (Neo4jServiceUnavailable) exhausts retries and sends to DLQ."""
        consumer = chart_consumer_instance
        mock_message = mock_consumer_record_factory(
            topic=CHART_CALCULATED_TOPIC, partition=0, offset=5,
            key=SAMPLE_USER_ID, value=SAMPLE_CHART_EVENT_VALUE
        )
        consumer.consumer.__aiter__.return_value = [mock_message]

        # Mock get_person_by_user_id to always fail with Neo4jServiceUnavailable
        mock_get_person = AsyncMock(side_effect=Neo4jServiceUnavailable("Connection lost"))

        with patch('services.kg.app.consumers.chart_consumer.get_person_by_user_id', mock_get_person), \
             patch('services.kg.app.consumers.chart_consumer.upsert_person', AsyncMock()) as mock_upsert_person, \
             patch('services.kg.app.consumers.chart_consumer.logger.info') as mock_logger_info, \
             patch('services.kg.app.consumers.chart_consumer.logger.error') as mock_logger_error, \
             patch('services.kg.app.consumers.chart_consumer.logger.warning') as mock_logger_warning:

            consumer_task = asyncio.create_task(consumer.consume())
            # Tenacity retries are configured for stop_after_attempt(3)
            # Wait long enough for 3 attempts with exponential backoff (1s, 2s, 4s approx)
            await asyncio.sleep(0.5) # Adjusted sleep for faster tests, assuming backoff is quick
            consumer._running = False
            await consumer_task

            assert mock_get_person.call_count == 3 # Original + 2 retries = 3 attempts
            mock_upsert_person.assert_not_called()

            # Verify logs for retry attempts
            mock_logger_info.assert_any_call(f"Attempting to process message for key {SAMPLE_USER_ID}, attempt: 1")
            mock_logger_info.assert_any_call(f"Attempting to process message for key {SAMPLE_USER_ID}, attempt: 2")
            mock_logger_info.assert_any_call(f"Attempting to process message for key {SAMPLE_USER_ID}, attempt: 3")

            # Verify error log after retries exhausted
            mock_logger_error.assert_any_call(
                f"Neo4jServiceUnavailable after retries for key {SAMPLE_USER_ID} from {CHART_CALCULATED_TOPIC} P:0 O:5. Error: Connection lost",
                exc_info=True
            )
            # Verify DLQ send
            mock_logger_warning.assert_any_call(
                f"Sending message for key {SAMPLE_USER_ID} from topic {CHART_CALCULATED_TOPIC} to DLQ topic {mock_kafka_settings.chart_consumer_dlq_topic} due to neo4j_unavailable_after_retries"
            )
            consumer.dlq_producer.send_and_wait.assert_called_once()
            dlq_call_args = consumer.dlq_producer.send_and_wait.call_args
            assert dlq_call_args[1]['value']['error_type'] == "neo4j_unavailable_after_retries"
            consumer.consumer.commit.assert_called_once() # Commit after DLQ

    @pytest.mark.asyncio
    async def test_process_message_dao_persistent_error_dlq(self, chart_consumer_instance, mock_consumer_record_factory, mock_kafka_settings):
        """Test persistent DAO error (UniqueConstraintViolationError) sends to DLQ without retry."""
        consumer = chart_consumer_instance
        mock_message = mock_consumer_record_factory(
            topic=CHART_CALCULATED_TOPIC, partition=0, offset=6,
            key=SAMPLE_USER_ID, value=SAMPLE_CHART_EVENT_VALUE
        )
        consumer.consumer.__aiter__.return_value = [mock_message]

        # Mock create_person to raise UniqueConstraintViolationError
        mock_create_person = AsyncMock(side_effect=UniqueConstraintViolationError("Node already exists"))

        with patch('services.kg.app.consumers.chart_consumer.get_person_by_user_id', AsyncMock(return_value=None)) as mock_get_person, \
             patch('services.kg.app.consumers.chart_consumer.upsert_person', mock_create_person), \
             patch('services.kg.app.consumers.chart_consumer.logger.info') as mock_logger_info, \
             patch('services.kg.app.consumers.chart_consumer.logger.error') as mock_logger_error, \
             patch('services.kg.app.consumers.chart_consumer.logger.warning') as mock_logger_warning:

            consumer_task = asyncio.create_task(consumer.consume())
            await asyncio.sleep(0.1)
            consumer._running = False
            await consumer_task

            mock_get_person.assert_called_once()
            mock_create_person.assert_called_once() # This mock is now patching upsert_person

            # Verify logs: one attempt, then error, then DLQ
            mock_logger_info.assert_any_call(f"Attempting to process message for key {SAMPLE_USER_ID}, attempt: 1")
            # Check that no more "Attempting to process" logs for attempts > 1
            for log_call in mock_logger_info.call_args_list:
                assert "attempt: 2" not in log_call[0][0]
                assert "attempt: 3" not in log_call[0][0]

            mock_logger_error.assert_any_call(
                f"Persistent DAO Error (UniqueConstraintViolationError or NodeCreationError) processing message for key {SAMPLE_USER_ID} from {CHART_CALCULATED_TOPIC} P:0 O:6. Error: Node already exists",
                exc_info=True
            )
            mock_logger_warning.assert_any_call(
                f"Sending message for key {SAMPLE_USER_ID} from topic {CHART_CALCULATED_TOPIC} to DLQ topic {mock_kafka_settings.chart_consumer_dlq_topic} due to persistent_dao_error"
            )
            consumer.dlq_producer.send_and_wait.assert_called_once()
            dlq_call_args = consumer.dlq_producer.send_and_wait.call_args
            assert dlq_call_args[1]['value']['error_type'] == "persistent_dao_error"
            consumer.consumer.commit.assert_called_once() # Commit after DLQ

    @pytest.mark.asyncio
    async def test_process_message_dlq_send_failure(self, chart_consumer_instance, mock_consumer_record_factory, mock_kafka_settings):
        """Test that if DLQ send fails, the original message offset is not committed."""
        consumer = chart_consumer_instance
        mock_message = mock_consumer_record_factory(
            topic=CHART_CALCULATED_TOPIC, partition=0, offset=7,
            key=SAMPLE_USER_ID, value=SAMPLE_CHART_EVENT_VALUE # Valid message that will hit a persistent error
        )
        consumer.consumer.__aiter__.return_value = [mock_message]

        # Mock create_person to raise a persistent error, forcing a DLQ attempt
        mock_create_person = AsyncMock(side_effect=UniqueConstraintViolationError("Node already exists"))
        # Mock the DLQ producer's send_and_wait to raise an error
        consumer.dlq_producer.send_and_wait = AsyncMock(side_effect=KafkaError("DLQ unavailable")) # Use KafkaError

        with patch('services.kg.app.consumers.chart_consumer.get_person_by_user_id', AsyncMock(return_value=None)) as mock_get_person, \
             patch('services.kg.app.consumers.chart_consumer.upsert_person', mock_create_person), \
             patch('services.kg.app.consumers.chart_consumer.logger.error') as mock_logger_error, \
             patch('services.kg.app.consumers.chart_consumer.logger.warning') as mock_logger_warning, \
             patch('services.kg.app.consumers.chart_consumer.logger.critical') as mock_logger_critical: # if we add critical logs for DLQ failure

            consumer_task = asyncio.create_task(consumer.consume())
            await asyncio.sleep(0.1)
            consumer._running = False
            await consumer_task

            mock_get_person.assert_called_once()
            mock_create_person.assert_called_once() # This mock is now patching upsert_person

            # Log for the persistent DAO error
            mock_logger_error.assert_any_call(
                f"Persistent DAO Error (UniqueConstraintViolationError or NodeCreationError) processing message for key {SAMPLE_USER_ID} from {CHART_CALCULATED_TOPIC} P:0 O:7. Error: Node already exists",
                exc_info=True
            )
            # Log for attempting to send to DLQ
            mock_logger_warning.assert_any_call(
                f"Sending message for key {SAMPLE_USER_ID} from topic {CHART_CALCULATED_TOPIC} to DLQ topic {mock_kafka_settings.chart_consumer_dlq_topic} due to persistent_dao_error"
            )
            # Log for the DLQ send failure itself (from _send_to_dlq)
            mock_logger_error.assert_any_call(
                f"Failed to send message for key {SAMPLE_USER_ID} to DLQ topic {mock_kafka_settings.chart_consumer_dlq_topic}: DLQ unavailable",
                exc_info=True
            )
            # Log from the main consume loop indicating message not processed and not sent to DLQ
            mock_logger_error.assert_any_call(
                f"Message for key {SAMPLE_USER_ID} on {CHART_CALCULATED_TOPIC} P:0 O:7 was not processed successfully and not sent to DLQ. Offset will not be committed."
            )

            consumer.dlq_producer.send_and_wait.assert_called_once() # Attempted to send to DLQ
            consumer.consumer.commit.assert_not_called() # Offset should NOT be committed

# Add more tests for chart consumer: existing person update, partial data, DAO errors etc.


# --- Test TypologyAssessedConsumer ---

SAMPLE_TYPOLOGY_EVENT_VALUE = {
    "assessment_id": "typology_assess_consumer_1",
    "typology_name": "TestEnneagram",
    "score": "Type 5",
    "confidence": 0.92,
    "details": {"wing": "4", "level": "Healthy"}, # Changed 'trace' to 'details' to match consumer logic
    "timestamp": "2024-01-02T11:00:00Z"
}
SAMPLE_TYPOLOGY_USER_ID = "typology_consumer_user_1"

@pytest_asyncio.fixture # Changed to async fixture
async def typology_consumer_instance(mock_aio_kafka_consumer_factory, mock_aio_kafka_producer_factory): # Added async
    """Fixture to create a TypologyAssessedConsumer instance with mocked Kafka clients."""
    consumer = TypologyAssessedConsumer()
    consumer.consumer = mock_aio_kafka_consumer_factory
    consumer.dlq_producer = mock_aio_kafka_producer_factory
    return consumer

class TestTypologyAssessedConsumer:
 
    @pytest.mark.asyncio
    async def test_process_message_success(self, typology_consumer_instance, mock_consumer_record_factory, mock_kafka_settings):
        """Test successful processing of a TYPOLOGY_ASSESSED message."""
        consumer = typology_consumer_instance
        mock_message = mock_consumer_record_factory(
            topic=TOPIC_TYPOLOGY_ASSESSED,
            partition=0, offset=0,
            key=SAMPLE_TYPOLOGY_USER_ID, value=SAMPLE_TYPOLOGY_EVENT_VALUE
        )
        consumer.consumer.__aiter__.return_value = [mock_message]
 
        # Mock DAO calls
        with patch('services.kg.app.consumers.typology_consumer.get_person_by_user_id', AsyncMock(return_value=None)) as mock_get_person, \
             patch('services.kg.app.consumers.typology_consumer.upsert_person', AsyncMock(return_value=PersonNode(user_id=SAMPLE_TYPOLOGY_USER_ID, name=f"User {SAMPLE_TYPOLOGY_USER_ID}"))) as mock_upsert_person, \
             patch('services.kg.app.consumers.typology_consumer.get_typology_result_by_assessment_id', AsyncMock(return_value=None)) as mock_get_typology, \
             patch('services.kg.app.consumers.typology_consumer.upsert_typology_result', AsyncMock(return_value=TypologyResultNode(assessment_id=SAMPLE_TYPOLOGY_EVENT_VALUE["assessment_id"], typology_name=SAMPLE_TYPOLOGY_EVENT_VALUE["typology_name"], score=SAMPLE_TYPOLOGY_EVENT_VALUE["score"], details="{}"))) as mock_upsert_typology, \
             patch('services.kg.app.consumers.typology_consumer.link_person_to_typologyresult', AsyncMock()) as mock_link_typology:
 
            consumer_task = asyncio.create_task(consumer.consume())
            await asyncio.sleep(0.1)
            consumer._running = False
            await consumer_task
 
            mock_get_person.assert_called_once_with(SAMPLE_TYPOLOGY_USER_ID)
            mock_upsert_person.assert_called_once()
            mock_get_typology.assert_called_once_with(SAMPLE_TYPOLOGY_EVENT_VALUE["assessment_id"])
            mock_upsert_typology.assert_called_once()
            # Check payload passed to upsert_typology_result
            upsert_call_args = mock_upsert_typology.call_args[0][0]
            assert isinstance(upsert_call_args, TypologyResultNode)
            assert upsert_call_args.assessment_id == SAMPLE_TYPOLOGY_EVENT_VALUE["assessment_id"]
            assert upsert_call_args.score == SAMPLE_TYPOLOGY_EVENT_VALUE["score"]
            assert json.loads(upsert_call_args.details) == SAMPLE_TYPOLOGY_EVENT_VALUE["details"]
            
            mock_link_typology.assert_called_once()
            link_args = mock_link_typology.call_args[1] # kwargs
            assert link_args['user_id'] == SAMPLE_TYPOLOGY_USER_ID
            assert link_args['assessment_id'] == SAMPLE_TYPOLOGY_EVENT_VALUE["assessment_id"]
            assert isinstance(link_args['props'].assessment_date, datetime)
 
            consumer.consumer.commit.assert_called_once()
            consumer.dlq_producer.send_and_wait.assert_not_called()
 
    @pytest.mark.asyncio
    async def test_process_message_missing_required_field(self, typology_consumer_instance, mock_consumer_record_factory, mock_kafka_settings):
        """Test TYPOLOGY_ASSESSED message with missing 'assessment_id' is sent to DLQ."""
        consumer = typology_consumer_instance
        invalid_payload = SAMPLE_TYPOLOGY_EVENT_VALUE.copy()
        del invalid_payload["assessment_id"]
 
        malformed_message = mock_consumer_record_factory(
            topic=TOPIC_TYPOLOGY_ASSESSED, partition=0, offset=1,
            key=SAMPLE_TYPOLOGY_USER_ID, value=invalid_payload
        )
        consumer.consumer.__aiter__.return_value = [malformed_message]
 
        with patch('services.kg.app.consumers.typology_consumer.logger.error') as mock_logger_error, \
             patch('services.kg.app.consumers.typology_consumer.logger.warning') as mock_logger_warning:
 
            consumer_task = asyncio.create_task(consumer.consume())
            await asyncio.sleep(0.1)
            consumer._running = False
            await consumer_task
 
            expected_error_msg_part = f"TYPOLOGY_ASSESSED event for user {SAMPLE_TYPOLOGY_USER_ID} is missing one or more required fields"
            # Check that the specific ValueError was logged by process_message
            assert any(expected_error_msg_part in call_args[0][0] for call_args in mock_logger_error.call_args_list if len(call_args[0]) > 0)
 
            mock_logger_warning.assert_any_call(
                f"Sending message for key {SAMPLE_TYPOLOGY_USER_ID} from topic {TOPIC_TYPOLOGY_ASSESSED} to DLQ topic {mock_kafka_settings.typology_consumer_dlq_topic} due to value_error_malformed_message"
            )
            consumer.dlq_producer.send_and_wait.assert_called_once()
            dlq_call_args = consumer.dlq_producer.send_and_wait.call_args
            assert dlq_call_args[1]['value']['error_type'] == "value_error_malformed_message"
            assert expected_error_msg_part in dlq_call_args[1]['value']['error_message']
            consumer.consumer.commit.assert_called_once()
 
    @pytest.mark.asyncio
    async def test_process_message_malformed_json(self, typology_consumer_instance, mock_consumer_record_factory, mock_kafka_settings):
        """Test malformed JSON message is sent to DLQ by the main consume loop for TypologyAssessedConsumer."""
        consumer = typology_consumer_instance
        malformed_json_bytes = b"{'this is not valid json"
 
        from aiokafka.structs import ConsumerRecord
        broken_message = ConsumerRecord(
            topic=TOPIC_TYPOLOGY_ASSESSED, partition=0, offset=3,
            timestamp=123, timestamp_type=0, key=SAMPLE_TYPOLOGY_USER_ID.encode('utf-8'), value=malformed_json_bytes,
            checksum=None, serialized_key_size=1, serialized_value_size=1, headers=[]
        )
        consumer.consumer.__aiter__.return_value = [broken_message]
 
        with patch('services.kg.app.consumers.typology_consumer.logger.error') as mock_logger_error, \
             patch('services.kg.app.consumers.typology_consumer.logger.warning') as mock_logger_warning, \
             patch('services.kg.app.consumers.typology_consumer.logger.critical') as mock_logger_critical:
 
            consumer_task = asyncio.create_task(consumer.consume())
            await asyncio.sleep(0.1)
            consumer._running = False
            await consumer_task
 
            mock_logger_error.assert_any_call(
                f"FATAL: JSONDecodeError directly in consume loop for {TOPIC_TYPOLOGY_ASSESSED} P:0 O:3. Value (raw): {malformed_json_bytes}. Error: {ANY}",
                exc_info=True
            )
            mock_logger_warning.assert_any_call(
                f"Sending message for key {SAMPLE_TYPOLOGY_USER_ID} from topic {TOPIC_TYPOLOGY_ASSESSED} to DLQ topic {mock_kafka_settings.typology_consumer_dlq_topic} due to json_decode_error_in_consume_loop"
            )
            consumer.dlq_producer.send_and_wait.assert_called_once()
            dlq_call_args = consumer.dlq_producer.send_and_wait.call_args
            assert dlq_call_args[1]['value']['error_type'] == "json_decode_error_in_consume_loop"
            consumer.consumer.commit.assert_called_once()
 
    @pytest.mark.asyncio
    async def test_process_message_dao_transient_error_retry_success(self, typology_consumer_instance, mock_consumer_record_factory, mock_kafka_settings):
        """Test transient DAO error (Neo4jServiceUnavailable) leads to retry and eventual success for TypologyAssessedConsumer."""
        consumer = typology_consumer_instance
        mock_message = mock_consumer_record_factory(
            topic=TOPIC_TYPOLOGY_ASSESSED, partition=0, offset=4,
            key=SAMPLE_TYPOLOGY_USER_ID, value=SAMPLE_TYPOLOGY_EVENT_VALUE
        )
        consumer.consumer.__aiter__.return_value = [mock_message]
 
        mock_get_person = AsyncMock(side_effect=[
            Neo4jServiceUnavailable("Connection lost"),
            None, # Person not found on retry
        ])
 
        with patch('services.kg.app.consumers.typology_consumer.get_person_by_user_id', mock_get_person), \
             patch('services.kg.app.consumers.typology_consumer.upsert_person', AsyncMock(return_value=PersonNode(user_id=SAMPLE_TYPOLOGY_USER_ID, name=f"User {SAMPLE_TYPOLOGY_USER_ID}"))) as mock_upsert_person, \
             patch('services.kg.app.consumers.typology_consumer.get_typology_result_by_assessment_id', AsyncMock(return_value=None)) as mock_get_typology, \
             patch('services.kg.app.consumers.typology_consumer.upsert_typology_result', AsyncMock(return_value=TypologyResultNode(assessment_id="id", typology_name="name", score="s", details="{}"))) as mock_upsert_typology, \
             patch('services.kg.app.consumers.typology_consumer.link_person_to_typologyresult', AsyncMock()) as mock_link_typology, \
             patch('services.kg.app.consumers.typology_consumer.logger.info') as mock_logger_info:
 
            consumer_task = asyncio.create_task(consumer.consume())
            await asyncio.sleep(0.2)
            consumer._running = False
            await consumer_task
 
            assert mock_get_person.call_count == 2
            mock_upsert_person.assert_called_once()
            mock_get_typology.assert_called_once()
            mock_upsert_typology.assert_called_once()
            mock_link_typology.assert_called_once()
 
            mock_logger_info.assert_any_call(f"Attempting to process TYPOLOGY_ASSESSED message for key {SAMPLE_TYPOLOGY_USER_ID}, attempt: 1")
            mock_logger_info.assert_any_call(f"Attempting to process TYPOLOGY_ASSESSED message for key {SAMPLE_TYPOLOGY_USER_ID}, attempt: 2")
            mock_logger_info.assert_any_call(f"Successfully processed TYPOLOGY_ASSESSED message for key {SAMPLE_TYPOLOGY_USER_ID} after 2 attempts.")
            consumer.dlq_producer.send_and_wait.assert_not_called()
            consumer.consumer.commit.assert_called_once()
 
    @pytest.mark.asyncio
    async def test_process_message_dao_transient_error_retry_exhausted_dlq(self, typology_consumer_instance, mock_consumer_record_factory, mock_kafka_settings):
        """Test transient DAO error (Neo4jServiceUnavailable) exhausts retries and sends to DLQ for TypologyAssessedConsumer."""
        consumer = typology_consumer_instance
        mock_message = mock_consumer_record_factory(
            topic=TOPIC_TYPOLOGY_ASSESSED, partition=0, offset=5,
            key=SAMPLE_TYPOLOGY_USER_ID, value=SAMPLE_TYPOLOGY_EVENT_VALUE
        )
        consumer.consumer.__aiter__.return_value = [mock_message]
        mock_get_person = AsyncMock(side_effect=Neo4jServiceUnavailable("Connection lost"))
 
        with patch('services.kg.app.consumers.typology_consumer.get_person_by_user_id', mock_get_person), \
             patch('services.kg.app.consumers.typology_consumer.upsert_person', AsyncMock()) as mock_upsert_person, \
             patch('services.kg.app.consumers.typology_consumer.logger.info') as mock_logger_info, \
             patch('services.kg.app.consumers.typology_consumer.logger.error') as mock_logger_error, \
             patch('services.kg.app.consumers.typology_consumer.logger.warning') as mock_logger_warning:
 
            # Ensure dlq_producer.send_and_wait is a fresh AsyncMock for this test
            consumer.dlq_producer.send_and_wait = AsyncMock()

            consumer_task = asyncio.create_task(consumer.consume())
            await asyncio.sleep(0.5) # Allow time for retries and DLQ send
            consumer._running = False
            await consumer_task
 
            assert mock_get_person.call_count == 3 # Original + 2 retries
            mock_upsert_person.assert_not_called() # Should fail before upsert
 
            mock_logger_error.assert_any_call(
                f"Neo4jServiceUnavailable after retries for TYPOLOGY_ASSESSED key {SAMPLE_TYPOLOGY_USER_ID} from {TOPIC_TYPOLOGY_ASSESSED} P:0 O:5. Error: Connection lost",
                exc_info=True
            )
            mock_logger_warning.assert_any_call(
                f"Sending message for key {SAMPLE_TYPOLOGY_USER_ID} from topic {TOPIC_TYPOLOGY_ASSESSED} to DLQ topic {mock_kafka_settings.typology_consumer_dlq_topic} due to neo4j_unavailable_after_retries"
            )
            consumer.dlq_producer.send_and_wait.assert_called_once()
            
            actual_call_args = consumer.dlq_producer.send_and_wait.call_args
            assert actual_call_args is not None, "DLQ send_and_wait was called but call_args is None"
            
            # Check kwargs for 'value'
            dlq_kwargs = actual_call_args.kwargs
            assert isinstance(dlq_kwargs, dict), f"call_args.kwargs is not a dict: {type(dlq_kwargs)}"
            
            dlq_sent_value = dlq_kwargs.get('value')
            assert isinstance(dlq_sent_value, dict), f"DLQ value is not a dict: {type(dlq_sent_value)}"
            assert dlq_sent_value.get('error_type') == "neo4j_unavailable_after_retries"
            
            consumer.consumer.commit.assert_called_once()
 
    @pytest.mark.asyncio
    async def test_process_message_dao_persistent_error_dlq(self, typology_consumer_instance, mock_consumer_record_factory, mock_kafka_settings):
        """Test persistent DAO error (UniqueConstraintViolationError) sends to DLQ without retry for TypologyAssessedConsumer."""
        consumer = typology_consumer_instance
        mock_message = mock_consumer_record_factory(
            topic=TOPIC_TYPOLOGY_ASSESSED, partition=0, offset=6,
            key=SAMPLE_TYPOLOGY_USER_ID, value=SAMPLE_TYPOLOGY_EVENT_VALUE
        )
        consumer.consumer.__aiter__.return_value = [mock_message]
        mock_upsert_person_effect = AsyncMock(side_effect=UniqueConstraintViolationError("Node already exists"))
 
        # mock_upsert_person_effect holds the mock object configured with the side effect for upsert_person
        with patch('services.kg.app.consumers.typology_consumer.get_person_by_user_id', AsyncMock(return_value=None)) as mock_get_person, \
             patch('services.kg.app.consumers.typology_consumer.upsert_person', mock_upsert_person_effect) as mock_upsert_person_in_test, \
             patch('services.kg.app.consumers.typology_consumer.logger.error') as mock_logger_error, \
             patch('services.kg.app.consumers.typology_consumer.logger.warning') as mock_logger_warning:
 
            consumer_task = asyncio.create_task(consumer.consume())
            await asyncio.sleep(0.5) # Increased sleep duration
            consumer._running = False
            await consumer_task
 
            mock_upsert_person_in_test.assert_called_once()
            mock_logger_error.assert_any_call(
                f"Persistent DAO Error processing TYPOLOGY_ASSESSED message for key {SAMPLE_TYPOLOGY_USER_ID} from {TOPIC_TYPOLOGY_ASSESSED} P:0 O:6. Error: Node already exists",
                exc_info=True
            )
            mock_logger_warning.assert_any_call(
                f"Sending message for key {SAMPLE_TYPOLOGY_USER_ID} from topic {TOPIC_TYPOLOGY_ASSESSED} to DLQ topic {mock_kafka_settings.typology_consumer_dlq_topic} due to persistent_dao_error"
            )
            consumer.dlq_producer.send_and_wait.assert_called_once()
            consumer.consumer.commit.assert_called_once()
 
    @pytest.mark.asyncio
    async def test_process_message_dlq_send_failure(self, typology_consumer_instance, mock_consumer_record_factory, mock_kafka_settings):
        """Test that if DLQ send fails, the original message offset is not committed for TypologyAssessedConsumer."""
        consumer = typology_consumer_instance
        mock_message = mock_consumer_record_factory(
            topic=TOPIC_TYPOLOGY_ASSESSED, partition=0, offset=7,
            key=SAMPLE_TYPOLOGY_USER_ID, value=SAMPLE_TYPOLOGY_EVENT_VALUE
        )
        consumer.consumer.__aiter__.return_value = [mock_message]
        mock_upsert_person_effect = AsyncMock(side_effect=UniqueConstraintViolationError("Node already exists"))
        consumer.dlq_producer.send_and_wait = AsyncMock(side_effect=KafkaError("DLQ unavailable"))
 
        # mock_upsert_person_effect holds the mock object configured with the side effect for upsert_person
        with patch('services.kg.app.consumers.typology_consumer.get_person_by_user_id', AsyncMock(return_value=None)) as mock_get_person, \
             patch('services.kg.app.consumers.typology_consumer.upsert_person', mock_upsert_person_effect) as mock_upsert_person_in_test, \
             patch('services.kg.app.consumers.typology_consumer.logger.error') as mock_logger_error, \
             patch('services.kg.app.consumers.typology_consumer.logger.warning') as mock_logger_warning, \
             patch('services.kg.app.consumers.typology_consumer.logger.critical') as mock_logger_critical:
 
            consumer_task = asyncio.create_task(consumer.consume())
            await asyncio.sleep(0.1)
            consumer._running = False
            await consumer_task
 
            mock_upsert_person_in_test.assert_called_once()
 
            # Check logs for persistent error, DLQ attempt, and DLQ failure
            mock_logger_error.assert_any_call(
                f"Persistent DAO Error processing TYPOLOGY_ASSESSED message for key {SAMPLE_TYPOLOGY_USER_ID} from {TOPIC_TYPOLOGY_ASSESSED} P:0 O:7. Error: Node already exists",
                exc_info=True
            )
            mock_logger_warning.assert_any_call(
                f"Sending message for key {SAMPLE_TYPOLOGY_USER_ID} from topic {TOPIC_TYPOLOGY_ASSESSED} to DLQ topic {mock_kafka_settings.typology_consumer_dlq_topic} due to persistent_dao_error"
            )
            mock_logger_error.assert_any_call(
                f"Failed to send message for key {SAMPLE_TYPOLOGY_USER_ID} to DLQ topic {mock_kafka_settings.typology_consumer_dlq_topic}: DLQ unavailable",
                exc_info=True
            )
            mock_logger_error.assert_any_call(
                 f"Outer exception processing TYPOLOGY_ASSESSED message for key {SAMPLE_TYPOLOGY_USER_ID} on {TOPIC_TYPOLOGY_ASSESSED} P:0 O:7: DLQ unavailable",
                 exc_info=True
            )
 
            consumer.dlq_producer.send_and_wait.assert_called_once()
            consumer.consumer.commit.assert_not_called() # Commit should NOT happen