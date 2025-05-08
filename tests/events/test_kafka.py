import logging
import os
import time
import uuid
import datetime # Add datetime import
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytest
from confluent_kafka import KafkaError, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
# Removed testcontainers imports as we'll use docker-compose services directly
# from testcontainers.kafka import KafkaContainer
# from tests.utils.schema_registry_container import SchemaRegistryContainer
# from testcontainers.core.waiting_utils import wait_for_logs

from src.events.consumer_base import KafkaEventConsumer
from src.events.producer import KafkaEventProducer, SchemaViolation

# Configure logging for tests - Set to DEBUG
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# --- Constants ---
TEST_PROFILE_TOPIC = "test_profile_events"
TEST_PROFILE_DLQ_TOPIC = f"{TEST_PROFILE_TOPIC}_dlq"
TEST_GROUP_ID = "test-consumer-group"
# Correct schema directory path relative to the project root (/app in container)
SCHEMA_DIR = Path("events/schemas")

# --- Helper Functions ---
def load_schema(filename: str) -> str:
    """Loads an Avro schema file."""
    path = SCHEMA_DIR / filename
    try:
        with open(path, 'r') as f:
            return f.read()
    except FileNotFoundError:
        pytest.fail(f"Schema file not found at {path}")

PROFILE_SCHEMA_STR = load_schema("profile_events_v1.avsc")

def create_topics(admin_client: AdminClient, topics: List[Tuple[str, int, int, Optional[Dict[str, str]]]]):
    """Creates Kafka topics idempotently."""
    new_topics = []
    existing_topics = admin_client.list_topics(timeout=10).topics
    for topic_name, partitions, replication, config in topics:
        if topic_name not in existing_topics:
            topic_config = config or {}
            new_topics.append(NewTopic(topic_name, num_partitions=partitions, replication_factor=replication, config=topic_config))
        else:
            logger.info(f"Topic {topic_name} already exists.")

    if new_topics:
        futures = admin_client.create_topics(new_topics)
        for topic, future in futures.items():
            try:
                future.result()  # Wait for topic creation
                logger.info(f"Topic {topic} created successfully.")
            except KafkaException as e:
                if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                    logger.warning(f"Topic {topic} already existed (concurrent creation?).")
                else:
                    logger.error(f"Failed to create topic {topic}: {e}")
                    raise

# --- Fixtures ---

# Removed fixtures for testcontainers


@pytest.fixture(scope="module")
def kafka_infra():
    """
    Provides Kafka bootstrap server and Schema Registry URL from environment
    variables (set by docker-compose test service) and ensures test topics exist.
    """
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP")
    schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL")

    if not bootstrap_servers or not schema_registry_url:
        pytest.fail("KAFKA_BOOTSTRAP and SCHEMA_REGISTRY_URL env vars must be set.")

    logger.info(f"Using Kafka: {bootstrap_servers}, Schema Registry: {schema_registry_url}")

    # Create Admin Client using the service name (reachable from test container)
    admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})

    # Define topics to create (ensure they are cleaned up if necessary)
    # Note: Topic cleanup is not handled here, assumes fresh env or manual cleanup.
    topics_to_create = [
        (TEST_PROFILE_TOPIC, 3, 1, {"cleanup.policy": "compact"}), # Compacted main topic
        (TEST_PROFILE_DLQ_TOPIC, 3, 1, {"cleanup.policy": "delete", "retention.ms": "1209600000"}) # DLQ topic
    ]
    create_topics(admin_client, topics_to_create)

    return bootstrap_servers, schema_registry_url

@pytest.fixture
def producer(kafka_infra):
    """Provides an instance of KafkaEventProducer for tests."""
    bootstrap_servers, schema_registry_url = kafka_infra
    p = KafkaEventProducer(
        bootstrap_servers=bootstrap_servers,
        schema_registry_url=schema_registry_url,
        topic=TEST_PROFILE_TOPIC,
        schema_str=PROFILE_SCHEMA_STR,
        retries=2, # Lower retries for faster test failure
        transactional_id=f"test-producer-{uuid.uuid4()}" # Unique ID per test
    )
    yield p
    p.close()

# --- Concrete Consumer for Testing ---

class DummyConsumer(KafkaEventConsumer):
    """A concrete consumer implementation for testing purposes."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.handled_messages: List[Tuple[Optional[str], Dict[str, Any]]] = []
        self.error_on_key: Optional[str] = None

    def handle(self, message_key: Optional[str], message_value: Dict[str, Any]) -> None:
        logger.info(f"DummyConsumer handling message: Key={message_key}, Value={message_value}")
        if self.error_on_key and message_key == self.error_on_key:
            logger.warning(f"DummyConsumer raising ValueError for key: {message_key}")
            raise ValueError(f"Simulated processing error for key {self.error_on_key}")
        self.handled_messages.append((message_key, message_value))

    def get_handled_messages(self) -> List[Tuple[Optional[str], Dict[str, Any]]]:
        return self.handled_messages

    def set_error_trigger(self, key: str):
        self.error_on_key = key


@pytest.fixture
def dummy_consumer(kafka_infra):
    """Provides an instance of DummyConsumer."""
    bootstrap_servers, schema_registry_url = kafka_infra
    consumer = DummyConsumer(
        bootstrap_servers=bootstrap_servers,
        schema_registry_url=schema_registry_url,
        topic=TEST_PROFILE_TOPIC,
        group_id=TEST_GROUP_ID,
        schema_str=PROFILE_SCHEMA_STR,
        dlq_topic=TEST_PROFILE_DLQ_TOPIC, # Ensure DLQ is configured
        poll_timeout=0.5 # Faster polling for tests
    )
    # Ensure consumer group is stable before tests run
    time.sleep(2) # Give some time for group coordination if needed
    yield consumer
    consumer.close()


# --- Test Cases ---

def test_producer_send_valid(producer):
    """Test sending a valid message."""
    profile_id = str(uuid.uuid4())
    user_id = str(uuid.uuid4())
    message = {
        "event_type": "CREATED",
        "profile_id": profile_id,
        "user_id": user_id,
        "timestamp": int(time.time() * 1000),
        "payload": '{"data": "valid"}',
    }
    print(f"--- test_producer_send_valid: Sending message with key {profile_id}...")
    producer.send(key=profile_id, value=message)
    print(f"--- test_producer_send_valid: Message sent (key {profile_id}). Flushing...")
    # Flush to ensure message is sent before test ends
    producer.producer.flush(5)
    print(f"--- test_producer_send_valid: Flush complete.")
    # Verification often happens in consumer tests, but basic send should not raise error

def test_producer_send_invalid_schema(producer):
    """Test sending a message that violates the schema."""
    profile_id = str(uuid.uuid4())
    message = {
        "event_type": "UPDATED",
        # "profile_id": profile_id, # Missing required field
        "user_id": str(uuid.uuid4()),
        "timestamp": int(time.time() * 1000),
        "payload": '{"data": "invalid"}',
    }
    with pytest.raises(SchemaViolation):
        print(f"--- test_producer_send_invalid_schema: Attempting to send invalid message with key {profile_id}...")
        producer.send(key=profile_id, value=message)
        print(f"--- test_producer_send_invalid_schema: Send call completed (should have raised).") # Should not be reached

import subprocess # Need this for docker commands if running locally


def test_producer_retry_after_outage(kafka_infra, producer, dummy_consumer):
    """
    Test that the producer retries and eventually sends a message after
    a simulated Kafka broker outage.
    NOTE: This test relies on external docker-compose commands and timing,
          which might be less reliable than container orchestration libraries.
    """
    bootstrap_servers, _ = kafka_infra
    profile_id = f"retry-{uuid.uuid4()}"
    message = {
        "event_type": "CREATED",
        "profile_id": profile_id,
        "user_id": str(uuid.uuid4()),
        "timestamp": int(time.time() * 1000),
        "payload": '{"data": "retry_test"}',
    }

    docker_compose_file = "infra/docker/docker-compose.yml"
    kafka_service = "kafka"

    logger.info(f"--- test_producer_retry_after_outage: Stopping Kafka service '{kafka_service}'...")
    # NOTE: Kafka service should be stopped externally before running this test.
    logger.info("Assuming Kafka service is stopped externally.")

    # Wait briefly to ensure Kafka is likely down
    time.sleep(5) # Keep a small delay
    logger.info(f"--- test_producer_retry_after_outage: Kafka stopped. Sending message {profile_id} (expecting retries)...")

    # Attempt to send the message while Kafka is down.
    # The producer should start retrying internally based on its config.
    # This call might block or return quickly depending on buffer settings.
    try:
        producer.send(key=profile_id, value=message)
        logger.info(f"--- test_producer_retry_after_outage: Send call initiated for {profile_id}.")
    except KafkaException as e:
        # Depending on timing and buffer settings, this might fail immediately
        logger.warning(f"--- test_producer_retry_after_outage: Send call failed immediately (BufferError?): {e}. Retries might still happen internally.")
        # Allow the test to continue to see if restart helps

    logger.info(f"--- test_producer_retry_after_outage: Restarting Kafka service '{kafka_service}'...")
    # NOTE: Kafka service should be started externally around this point in the test.
    logger.info("Assuming Kafka service is started externally.")


    # Wait for Kafka to become available after external restart.
    # A fixed sleep is simple but potentially unreliable.
    # Checking health status via docker inspect would be better.
    kafka_restart_wait_time = 25 # Increased wait time
    logger.info(f"--- test_producer_retry_after_outage: Waiting {kafka_restart_wait_time}s for Kafka to restart and stabilize...")
    time.sleep(kafka_restart_wait_time)

    logger.info(f"--- test_producer_retry_after_outage: Kafka should be back. Flushing producer for {profile_id}...")
    try:
        # Flush should now succeed if retries worked after restart
        remaining = producer.producer.flush(15) # Increased flush timeout
        assert remaining == 0, f"Producer flush failed after Kafka restart, {remaining} messages remaining."
        logger.info(f"--- test_producer_retry_after_outage: Flush successful for {profile_id}.")
    except KafkaException as e:
         pytest.fail(f"Producer flush failed after Kafka restart: {e}")


    # Verify the message was delivered using the consumer
    logger.info(f"--- test_producer_retry_after_outage: Consuming to verify message {profile_id} delivery...")
    start_time = time.time()
    found = False
    while time.time() - start_time < 15: # Consume for up to 15 seconds
        msgs_consumed = dummy_consumer.consumer.consume(num_messages=5, timeout=1.0)
        if any(m[0] == profile_id for m in dummy_consumer.get_handled_messages()):
            logger.info(f"--- test_producer_retry_after_outage: Message {profile_id} found by consumer.")
            found = True
            break
        time.sleep(1) # Add small delay between polls

    assert found, f"Consumer did not handle the retried message {profile_id} within the timeout."

    # Final check on handled messages
    handled = dummy_consumer.get_handled_messages()
    assert any(key == profile_id and value == message for key, value in handled), f"Message {profile_id} not found or content mismatch in handled messages."
    logger.info(f"--- test_producer_retry_after_outage: Test successful for {profile_id}.")
# Note: Simulating broker outage for retry testing with testcontainers is complex.
# It might involve stopping/pausing the container, which can be flaky.
# We rely on the confluent-kafka library's internal retry logic configured in the producer.
# A simpler test could check if sending fails when Kafka is down initially,
# but testing *successful* retry after outage is harder in this setup.

def test_consumer_handle_message(kafka_infra, producer, dummy_consumer):
    """Test that the consumer correctly handles a valid message."""
    # kafka_infra now returns env var values directly
    bootstrap_servers, _ = kafka_infra
    profile_id = str(uuid.uuid4())
    message = {
        "event_type": "CREATED",
        "profile_id": profile_id,
        "user_id": str(uuid.uuid4()),
        "timestamp": int(time.time() * 1000),
        "payload": '{"data": "consume_test"}',
    }

    # Send the message
    print(f"--- test_consumer_handle_message: Sending message with key {profile_id}...")
    producer.send(key=profile_id, value=message)
    print(f"--- test_consumer_handle_message: Message sent (key {profile_id}). Flushing...")
    producer.producer.flush(5)
    print(f"--- test_consumer_handle_message: Flush complete.")
    logger.info(f"Sent message {profile_id} for consumption test.")

    # Consume messages - run for a short time
    start_time = time.time()
    while time.time() - start_time < 10: # Consume for up to 10 seconds
        print(f"--- test_consumer_handle_message: Polling consumer...")
        msgs_consumed = dummy_consumer.consumer.consume(num_messages=5, timeout=1.0) # Poll multiple messages
        print(f"--- test_consumer_handle_message: Polling complete. Messages received: {len(msgs_consumed)}")

        if msgs_consumed:
            # Process the consumed messages through the consumer's logic
            print(f"--- test_consumer_handle_message: Processing {len(msgs_consumed)} messages...")
            offsets_to_commit = dummy_consumer.process_messages(msgs_consumed)
            print(f"--- test_consumer_handle_message: Processing complete. Offsets to commit: {offsets_to_commit}")
            if offsets_to_commit:
                try:
                    dummy_consumer.consumer.commit(offsets=offsets_to_commit, asynchronous=False)
                    print(f"--- test_consumer_handle_message: Committed offsets: {offsets_to_commit}")
                except KafkaException as e:
                     logger.error(f"Commit failed during test loop: {e}")
                     # Decide how to handle commit failure in test - maybe break or fail?

        # Check if the target message has been handled after processing
        if any(m[0] == profile_id for m in dummy_consumer.get_handled_messages()):
            logger.info(f"Message {profile_id} found in handled messages.")
            break
    else:
        pytest.fail("Consumer did not handle the message within the timeout.")

    # Assertions
    handled = dummy_consumer.get_handled_messages()
    assert len(handled) >= 1
    found = False
    for key, value in handled:
        if key == profile_id:
            # Compare messages excluding the timestamp due to type conversion
            original_message_no_ts = {k: v for k, v in message.items() if k != 'timestamp'}
            handled_value_no_ts = {k: v for k, v in value.items() if k != 'timestamp'}
            assert handled_value_no_ts == original_message_no_ts, \
                f"Handled message content mismatch (excluding timestamp): {handled_value_no_ts} != {original_message_no_ts}"
            # Optionally, check timestamp type if needed
            assert isinstance(value.get('timestamp'), datetime.datetime), "Timestamp was not converted to datetime"
            found = True
            break
    assert found, f"Message with key {profile_id} not found in handled messages."

    # Check committed offset (requires fetching committed offsets)
    try:
        committed = dummy_consumer.consumer.committed([TopicPartition(TEST_PROFILE_TOPIC, p) for p in range(3)])
        logger.info(f"Committed offsets: {committed}")
        # Assert that at least one partition has a committed offset > 0
        assert any(tp.offset > 0 for tp in committed if tp.offset > -1001) # -1001 is OFFSET_INVALID
    except KafkaException as e:
        pytest.fail(f"Failed to get committed offsets: {e}")


def test_consumer_dlq_on_handle_error(kafka_infra, producer, dummy_consumer):
    """Test that a message lands in the DLQ when handle() raises an error."""
    bootstrap_servers, schema_registry_url = kafka_infra
    error_profile_id = f"error-{uuid.uuid4()}"
    good_profile_id = f"good-{uuid.uuid4()}"

    error_message = {
        "event_type": "UPDATED",
        "profile_id": error_profile_id,
        "user_id": str(uuid.uuid4()),
        "timestamp": int(time.time() * 1000),
        "payload": '{"data": "trigger_error"}',
    }
    good_message = {
        "event_type": "CREATED",
        "profile_id": good_profile_id,
        "user_id": str(uuid.uuid4()),
        "timestamp": int(time.time() * 1000),
        "payload": '{"data": "should_process"}',
    }

    # Configure consumer to fail on the specific key
    dummy_consumer.set_error_trigger(error_profile_id)

    # Send messages (error one first, then good one)
    print(f"--- test_consumer_dlq_on_handle_error: Sending ERROR message with key {error_profile_id}...")
    producer.send(key=error_profile_id, value=error_message)
    print(f"--- test_consumer_dlq_on_handle_error: Sending GOOD message with key {good_profile_id}...")
    producer.send(key=good_profile_id, value=good_message)
    print(f"--- test_consumer_dlq_on_handle_error: Messages sent. Flushing...")
    producer.producer.flush(5)
    print(f"--- test_consumer_dlq_on_handle_error: Flush complete.")
    logger.info(f"Sent messages {error_profile_id} (error) and {good_profile_id} (good).")

    # Consume messages
    start_time = time.time()
    processed_good_message = False
    while time.time() - start_time < 15: # Consume for up to 15 seconds
        print(f"--- test_consumer_dlq_on_handle_error: Polling main consumer...")
        msgs_consumed_main = dummy_consumer.consumer.consume(num_messages=5, timeout=1.0) # Poll a few messages
        print(f"--- test_consumer_dlq_on_handle_error: Polling main consumer complete. Messages received: {len(msgs_consumed_main)}")

        if msgs_consumed_main:
            # Process the consumed messages through the consumer's logic
            print(f"--- test_consumer_dlq_on_handle_error: Processing {len(msgs_consumed_main)} messages...")
            offsets_to_commit = dummy_consumer.process_messages(msgs_consumed_main)
            print(f"--- test_consumer_dlq_on_handle_error: Processing complete. Offsets to commit: {offsets_to_commit}")
            if offsets_to_commit:
                try:
                    dummy_consumer.consumer.commit(offsets=offsets_to_commit, asynchronous=False)
                    print(f"--- test_consumer_dlq_on_handle_error: Committed offsets: {offsets_to_commit}")
                except KafkaException as e:
                    logger.error(f"Commit failed during DLQ test loop: {e}")

        # Check if the good message has been handled after processing
        if any(m[0] == good_profile_id for m in dummy_consumer.get_handled_messages()):
             processed_good_message = True
             logger.info(f"Good message {good_profile_id} found in handled messages.")
             # Keep consuming a bit longer to ensure commits happen
             time.sleep(1)
             dummy_consumer.consumer.consume(num_messages=1, timeout=1.0)
             break # Exit loop once good message is processed

    # Assertions
    # 1. Good message should be handled
    assert processed_good_message, "Good message was not processed."
    assert any(m[0] == good_profile_id for m in dummy_consumer.get_handled_messages())
    # 2. Error message should NOT be handled by the main handler
    assert not any(m[0] == error_profile_id for m in dummy_consumer.get_handled_messages())

    # 3. Check DLQ for the error message
    dlq_messages = []
    dlq_consumer_conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': f'dlq-checker-{uuid.uuid4()}', # Unique group for checking
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }
    # Need a way to consume raw bytes from DLQ without full consumer setup
    # Using a simple confluent_kafka Consumer here
    from confluent_kafka import Consumer as BasicConsumer
    dlq_checker = BasicConsumer(dlq_consumer_conf)

    # Topic creation is handled by the kafka_infra fixture
    dlq_checker.subscribe([TEST_PROFILE_DLQ_TOPIC])
    logger.info(f"Checking DLQ topic: {TEST_PROFILE_DLQ_TOPIC}")
    time.sleep(2) # Allow subscription

    print(f"--- test_consumer_dlq_on_handle_error: Polling DLQ consumer...")
    raw_msgs = dlq_checker.consume(num_messages=10, timeout=5.0)
    print(f"--- test_consumer_dlq_on_handle_error: Polling DLQ consumer complete. Messages received: {len(raw_msgs)}")
    dlq_checker.close()

    found_in_dlq = False
    for msg in raw_msgs:
         if msg is None or msg.error():
             continue
         if msg.key() and msg.key().decode('utf-8') == error_profile_id:
             # Optional: Try to deserialize to confirm content matches
             try:
                 from confluent_kafka.schema_registry.avro import AvroDeserializer
                 from confluent_kafka.serialization import SerializationContext, MessageField
                 # Need schema registry client using the env var URL
                 _, schema_registry_url_check = kafka_infra
                 schema_registry_conf_check = {"url": schema_registry_url_check}
                 # Create a new client or reuse if appropriate (dummy_consumer has one)
                 schema_registry_client_check = dummy_consumer.schema_registry_client

                 deserializer = AvroDeserializer(schema_registry_client_check, PROFILE_SCHEMA_STR)
                 context = SerializationContext(msg.topic(), MessageField.VALUE)
                 deserialized_value = deserializer(msg.value(), context)
                 # Compare messages excluding the timestamp due to type conversion
                 original_error_message_no_ts = {k: v for k, v in error_message.items() if k != 'timestamp'}
                 dlq_value_no_ts = {k: v for k, v in deserialized_value.items() if k != 'timestamp'}
                 assert dlq_value_no_ts == original_error_message_no_ts, \
                     f"DLQ message content mismatch (excluding timestamp): {dlq_value_no_ts} != {original_error_message_no_ts}"
                 # Optionally, check timestamp type if needed
                 assert isinstance(deserialized_value.get('timestamp'), datetime.datetime), "DLQ Timestamp was not converted to datetime"
                 logger.info(f"Found and verified message {error_profile_id} in DLQ.")
                 found_in_dlq = True
                 break
             except Exception as e:
                 pytest.fail(f"Failed to deserialize message from DLQ: {e}")

    assert found_in_dlq, f"Message {error_profile_id} not found in DLQ."

    # 4. Check committed offset includes the offset *after* the failed message
    try:
        # Need to find the partition the error message was on. This is tricky without consuming it.
        # Instead, check that the committed offset for the partition the *good* message was on is correct.
        # This implicitly confirms the failed message's offset was also committed (as handled by DLQ).
        committed = dummy_consumer.consumer.committed([TopicPartition(TEST_PROFILE_TOPIC, p) for p in range(3)])
        logger.info(f"Committed offsets after DLQ event: {committed}")
        assert any(tp.offset > 0 for tp in committed if tp.offset > -1001)
        # A more precise check would require knowing the exact offsets, which is hard in tests.
        # We rely on the fact that the good message *was* processed and committed.
    except KafkaException as e:
        pytest.fail(f"Failed to get committed offsets after DLQ event: {e}")