import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch
import services.kg.app.crud.base_dao as base_dao
from aiokafka.structs import ConsumerRecord
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from services.kg.app.core.config import KafkaSettings, kafka_settings
from neo4j import AsyncGraphDatabase, AsyncDriver
import asyncio
import time
import os
from testcontainers.kafka import KafkaContainer # Using testcontainers for Kafka
from testcontainers.neo4j import Neo4jContainer # Using testcontainers for Neo4j

# Remove the previous mock_neo4j_driver fixture if it exists

@pytest.fixture # Removed autouse=True
def mock_neo4j_session(monkeypatch):
    """Mocks the async DB session using monkeypatch as per user suggestion."""

    # 1) Create the fake session object that the context manager yields
    # Use AsyncMock as its methods (execute_read/write) are awaited
    fake_session = AsyncMock()

    # Mock the result cursor returned by tx.run()
    mock_cursor = AsyncMock()
    mock_cursor.data = AsyncMock(return_value=[])
    mock_cursor.single = AsyncMock(return_value=None)
    mock_cursor.consume = AsyncMock()

    # Mock the transaction object passed to execute_read/write lambdas
    mock_tx = AsyncMock()
    mock_tx.run = AsyncMock(return_value=mock_cursor)

    # Configure the side effects for execute_read/write to simulate transaction execution
    async def execute_read_side_effect(tx_lambda, *args, **kwargs):
        return await tx_lambda(mock_tx)
    async def execute_write_side_effect(tx_lambda, *args, **kwargs):
        return await tx_lambda(mock_tx)

    fake_session.execute_read = AsyncMock(side_effect=execute_read_side_effect)
    fake_session.execute_write = AsyncMock(side_effect=execute_write_side_effect)
    fake_session.close = AsyncMock() # Mock close if it's called

    # 2) Create an async context‐manager mock whose __aenter__ yields that session
    # Use AsyncMock for the context manager itself
    fake_cm = AsyncMock()
    async def mock_aenter(*args, **kwargs):
        return fake_session # __aenter__ returns the session mock
    async def mock_aexit(*args, **kwargs):
        return None # __aexit__ returns None
    fake_cm.__aenter__ = mock_aenter
    fake_cm.__aexit__ = mock_aexit

    # 3) Monkey‐patch get_async_db_session to be an async function returning our CM
    async def fake_get_async_db_session(*args, **kwargs): # Accept arbitrary args
        return fake_cm # Return the context manager mock

    monkeypatch.setattr(
        base_dao, # Patch where it's imported/used
        "get_async_db_session",
        fake_get_async_db_session
    )

    # Patch the driver getter as well
    mock_driver_instance = AsyncMock()
    mock_driver_instance.verify_connectivity = AsyncMock()
    mock_driver_instance.close = AsyncMock()
    # Patch the class method directly
    monkeypatch.setattr(
        'services.kg.app.core.db.Neo4jDatabase.get_async_driver',
        AsyncMock(return_value=mock_driver_instance)
    )

    # Yield mocks needed by tests (optional)
    yield {
        "session": fake_session,
        "cursor": mock_cursor, # Renamed from tx_run_result for clarity
        "context_manager": fake_cm,
        "driver": mock_driver_instance # Added driver mock just in case
    }

# Keep commented out event loop fixture
# @pytest.fixture(scope="session")
# def event_loop(scope="session"):
#     """
#     Creates an explicit asyncio event loop for the test session.
#     This helps avoid issues with pytest-asyncio's default loop management in some environments.
#     """
#     import asyncio
#     policy = asyncio.get_event_loop_policy()
#     loop = policy.new_event_loop()
#     yield loop
#     loop.close()

# You can add more shared fixtures here, e.g., sample data for nodes/relationships.

@pytest.fixture
def mock_kafka_settings():
    """Provides mock KafkaSettings with DLQ topics."""
    settings = KafkaSettings(
        bootstrap_servers="mock_kafka:9092",
        schema_registry_url="mock_registry:8081",
        chart_consumer_dlq_topic="test_dlq_chart_topic",
        typology_consumer_dlq_topic="test_dlq_typology_topic"
    )
    return settings

@pytest_asyncio.fixture
async def mock_aio_kafka_consumer_factory():
    """
    A factory fixture to create a mock AIOKafkaConsumer instance.
    This allows tests to get a fresh mock for each consumer instance if needed,
    or to use a shared one if patched at the class level.
    """
    mock_consumer = AsyncMock()
    mock_consumer.start = AsyncMock()
    mock_consumer.stop = AsyncMock()
    mock_consumer.commit = AsyncMock()
    # To make it an async iterator for `async for message in self.consumer:`
    mock_consumer.__aiter__.return_value = [] # Default to no messages
    return mock_consumer

@pytest_asyncio.fixture
async def mock_aio_kafka_producer_factory():
    """
    A factory fixture to create a mock AIOKafkaProducer instance.
    """
    mock_producer = AsyncMock()
    mock_producer.start = AsyncMock()
    mock_producer.stop = AsyncMock()
    mock_producer.send_and_wait = AsyncMock()
    return mock_producer

@pytest.fixture
def mock_consumer_record_factory():
    """Factory to create mock ConsumerRecord instances."""
    def _create_mock_record(topic, partition, offset, key, value):
        # Ensure value is bytes if it's supposed to be raw from Kafka before deserialization
        # For testing deserialized values, it can be a dict if consumer's deserializer is mocked/bypassed.
        # If testing the consumer's actual deserializer, value should be bytes.
        return ConsumerRecord(
            topic=topic,
            partition=partition,
            offset=offset,
            timestamp=1234567890, # dummy timestamp
            timestamp_type=0, # CreateTime
            key=key.encode('utf-8') if isinstance(key, str) else key,
            value=json.dumps(value).encode('utf-8') if isinstance(value, dict) else value,
            checksum=None,
            serialized_key_size=len(key.encode('utf-8') if isinstance(key, str) else key or b''),
            serialized_value_size=len(json.dumps(value).encode('utf-8') if isinstance(value, dict) else value or b''),
            headers=[]
        )
    return _create_mock_record

# --- Integration Test Fixtures for Dockerized Services ---

@pytest.fixture(scope="session")
def neo4j_container():
    """Starts a Neo4j container for the test session."""
    container = Neo4jContainer("neo4j:5.18") # Use a recent version
    # container.with_env("NEO4J_AUTH", "neo4j/testpassword") # Default is neo4j/neo4j
    container.with_exposed_ports(7687)
    try:
        container.start()
        # Wait for Neo4j to be ready
        # A more robust check would involve trying to connect.
        time.sleep(15) # Generous wait time for Neo4j to initialize
        yield container
    finally:
        container.stop()

@pytest.fixture(scope="session")
def kafka_container():
    """Starts a Kafka container for the test session."""
    container = KafkaContainer(image="confluentinc/cp-kafka:latest")
    container.with_exposed_ports(9093) # Expose Kafka port for broker
    # For Schema Registry, you'd add another container or use a Kafka container with it built-in.
    # For simplicity, this example focuses on Kafka broker.
    try:
        container.start()
        # Wait for Kafka to be ready
        time.sleep(10) # Wait for Kafka to initialize
        yield container
    finally:
        container.stop()

@pytest_asyncio.fixture(scope="session")
async def integration_neo4j_driver(neo4j_container: Neo4jContainer):
    """Provides an AsyncDriver connected to the test Neo4j container."""
    bolt_url = neo4j_container.get_connection_url().replace("bolt://", f"bolt://{neo4j_container.get_container_host_ip()}:") # Adjust for bolt
    # Correctly get exposed port for Bolt (7687)
    bolt_port = neo4j_container.get_exposed_port(7687)
    uri = f"bolt://{neo4j_container.get_container_host_ip()}:{bolt_port}"
    
    # Default auth for Neo4jContainer is neo4j/neo4j if not overridden
    driver = AsyncGraphDatabase.driver(uri, auth=("neo4j", "neo4j"))
    try:
        await driver.verify_connectivity()
        yield driver
    finally:
        await driver.close()

@pytest_asyncio.fixture(scope="function") # Function scope for clean slate per test
async def integration_kafka_producer(kafka_container: KafkaContainer):
    """Provides an AIOKafkaProducer connected to the test Kafka container."""
    bootstrap_servers = kafka_container.get_bootstrap_server()
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    await producer.start()
    try:
        yield producer
    finally:
        await producer.stop()

@pytest_asyncio.fixture(scope="function")
async def integration_kafka_consumer_factory(kafka_container: KafkaContainer):
    """
    Factory to create AIOKafkaConsumer instances connected to the test Kafka container.
    Allows creating consumers for specific topics within tests.
    """
    bootstrap_servers = kafka_container.get_bootstrap_server()
    
    async def _consumer_factory(topic: str, group_id: str):
        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset="earliest", # Start from the beginning for tests
            enable_auto_commit=False # Important for test control
        )
        await consumer.start()
        return consumer
        # Caller is responsible for stopping the consumer

    yield _consumer_factory


@pytest.fixture(scope="session")
def integration_kafka_settings(kafka_container: KafkaContainer):
    """Overrides KafkaSettings for integration tests."""
    bootstrap_servers = kafka_container.get_bootstrap_server()
    # For integration tests, schema registry might not be strictly needed if
    # we are testing raw message processing or if schemas are simple.
    # If Avro/SR is critical, a Schema Registry container would also be needed.
    return KafkaSettings(
        bootstrap_servers=bootstrap_servers,
        schema_registry_url="http://mock-schema-registry:8081", # Placeholder if not testing SR
        chart_consumer_dlq_topic="integration_dlq_chart_topic",
        typology_consumer_dlq_topic="integration_dlq_typology_topic",
        # Add other topics used by kg_service if any
        chart_calculated_topic="chart_calculated_v1", # Example input topic
        typology_assessed_topic="typology_assessed_v1" # Example input topic
    )

# This fixture can be used to override the Kafka settings for the kg_service application
# when it's run in an integration test context (e.g., if kg_service is started as a subprocess or Docker container).
# For now, tests will produce to Kafka and verify Neo4j directly.
# If kg_service itself is run, its config needs to point to these Dockerized services.

# Note: The existing autouse mock_neo4j_session and patch_kafka_settings_in_consumers
# might conflict with these integration fixtures if not managed.
# For integration tests, these mocks should ideally be disabled.
# This can be done by not making them autouse and applying them selectively,
# or by using pytest markers to skip them for integration tests.
# For now, we assume unit tests and integration tests are run separately or managed via markers.