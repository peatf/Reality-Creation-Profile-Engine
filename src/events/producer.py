import logging
import time
import uuid
from typing import Any, Dict, Optional

import json # Import json for parsing schema string if needed by AvroSchema

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.error import KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSchema, AvroSerializer # Import AvroSchema
from fastavro.validation import validate

# Custom Exception for Schema Violations
class SchemaViolation(ValueError):
    """Custom exception for Avro schema validation errors."""
    pass

logger = logging.getLogger(__name__)

class KafkaEventProducer:
    """
    A Kafka producer client with Avro serialization, schema registration,
    retries, and exactly-once semantics support.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        schema_registry_url: str,
        topic: str,
        schema_str: str,
        retries: int = 5,
        acks: str = 'all',
        transactional_id: Optional[str] = None,
    ):
        """
        Initializes the KafkaEventProducer.

        Args:
            bootstrap_servers: Kafka broker list (e.g., 'localhost:9092').
            schema_registry_url: URL for the Confluent Schema Registry.
            topic: The Kafka topic to produce messages to.
            schema_str: The Avro schema string for message values.
            retries: Number of times to retry sending a message on failure.
            acks: Message acknowledgement level ('all', 0, 1).
            transactional_id: Enables idempotence and transactions if set.
        """
        self.bootstrap_servers = bootstrap_servers
        self.schema_registry_url = schema_registry_url
        self.topic = topic
        self.retries = retries
        self.acks = acks
        self.transactional_id = transactional_id or f"producer-{topic}-{uuid.uuid4()}"

        # --- Schema Registry Client & Serializer ---
        schema_registry_conf = {"url": self.schema_registry_url}
        # Add retry logic for SchemaRegistryClient initialization
        sr_client = None
        for attempt in range(3): # Retry up to 3 times
            try:
                sr_client = SchemaRegistryClient(schema_registry_conf)
                # Attempt a basic operation to check connectivity (optional but good)
                # sr_client.get_subjects() # This might be too slow for init
                logger.info("SchemaRegistryClient initialized successfully.")
                break
            except Exception as e:
                logger.warning(f"SchemaRegistryClient init attempt {attempt+1} failed: {e}. Retrying in 2s...")
                if attempt >= 2:
                    logger.error("Failed to initialize SchemaRegistryClient after multiple retries.")
                    raise # Re-raise the last exception
                time.sleep(2)
        if sr_client is None:
             raise RuntimeError("Failed to initialize SchemaRegistryClient.") # Should not happen if loop completes/raises
        self.schema_registry_client = sr_client


        # Simplify: Pass schema string directly to serializer.
        # Registration will be handled by the serializer if auto.register.schemas=True,
        # or manually before first send if needed. Let's rely on serializer for now.
        # We removed the problematic AvroSchema() call.

        self.avro_serializer = AvroSerializer(
            self.schema_registry_client,
            schema_str, # Pass the raw schema string
            # Set auto.register.schemas to True for simplicity in testing,
            # though manual registration is often preferred in production.
            conf={'auto.register.schemas': True}
        )

        # For local validation with fastavro, parse the JSON string
        try:
            self.schema_for_validation = json.loads(schema_str)
        except json.JSONDecodeError as e:
            logger.error(f"Schema string is not valid JSON: {e}")
            raise ValueError(f"Invalid schema string provided (not JSON): {e}") from e

        # --- Kafka Producer Configuration ---
        producer_conf = {
            "bootstrap.servers": self.bootstrap_servers,
            "acks": self.acks,
            "retries": self.retries,
            "delivery.timeout.ms": 30000, # Corresponds roughly to retry cap
            "linger.ms": 10, # Small batching delay
            "enable.idempotence": True, # Required for EOS
            "transactional.id": self.transactional_id, # Enables EOS
            # Use default partitioner (consistent hashing)
        }
        self.producer = Producer(producer_conf)
        self.producer.init_transactions() # Initialize transactions

    def _delivery_report(self, err, msg):
        """ Called once for each message produced to indicate delivery result."""
        if err is not None:
            logger.error(f"Message delivery failed for key {msg.key()}: {err}")
            # In transactional mode, this error will likely abort the transaction
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

    def send(self, key: str, value: Dict[str, Any]) -> None:
        """
        Sends a message to the configured Kafka topic with Avro serialization.

        Args:
            key: The message key (string).
            value: The message value (dictionary conforming to the Avro schema).

        Raises:
            SchemaViolation: If the value does not conform to the Avro schema.
            KafkaException: If producing the message fails after retries.
        """
        # 1. Local Schema Validation (Fast Fail)
        try:
            # Use fastavro for local validation before serialization attempt
            # Use the parsed schema dictionary for validation
            validate(value, self.schema_for_validation)
        except Exception as e: # Catch broad validation errors from fastavro
            logger.error(f"Local schema validation failed for key {key}: {e}\nValue: {value}")
            raise SchemaViolation(f"Value does not match schema: {e}") from e

        # 2. Serialize and Produce (with retries handled by librdkafka)
        attempt = 0
        backoff_time = 1 # Initial backoff in seconds
        max_backoff = 30

        self.producer.begin_transaction() # Start transaction for this message

        while attempt <= self.retries:
            try:
                # Create SerializationContext for the value field
                from confluent_kafka.serialization import SerializationContext, MessageField
                serialization_context = SerializationContext(self.topic, MessageField.VALUE)
                serialized_value = self.avro_serializer(value, serialization_context)
                self.producer.produce(
                    topic=self.topic,
                    key=key.encode('utf-8'),
                    value=serialized_value,
                    callback=self._delivery_report
                )
                # Poll to trigger delivery report and handle callbacks
                # A short poll is okay here as we expect quick feedback or retries
                self.producer.poll(0.1)
                self.producer.commit_transaction() # Commit if produce successful
                logger.debug(f"Successfully produced message with key: {key}")
                return # Success

            except BufferError:
                # Producer queue is full, wait and retry
                logger.warning(f"Producer queue full for key {key}. Retrying in {backoff_time}s...")
                self.producer.poll(backoff_time) # Wait for queue space
                backoff_time = min(backoff_time * 2, max_backoff)
                attempt += 1
            except KafkaException as e:
                # Handle potentially retryable Kafka errors
                logger.error(f"KafkaException during produce for key {key} (attempt {attempt+1}/{self.retries+1}): {e}. Retrying in {backoff_time}s...")
                self.producer.poll(0) # Clear any immediate callbacks
                time.sleep(backoff_time)
                backoff_time = min(backoff_time * 2, max_backoff)
                attempt += 1
                if attempt > self.retries:
                    logger.critical(f"KafkaException persisted after {self.retries} retries for key {key}: {e}. Aborting transaction.")
                    try:
                        self.producer.abort_transaction()
                    except KafkaException as abort_e:
                        logger.error(f"Failed to abort transaction: {abort_e}")
                    # Synchronous flush on final failure to ensure any pending messages are sent/failed
                    logger.info("Attempting synchronous flush before raising exception...")
                    self.producer.flush(10) # 10 second timeout for flush
                    raise e # Re-raise the original exception
            except Exception as e: # Catch unexpected errors (e.g., serialization issues not caught locally)
                 logger.critical(f"Unexpected error during produce for key {key}: {e}. Aborting transaction.")
                 try:
                     self.producer.abort_transaction()
                 except KafkaException as abort_e:
                     logger.error(f"Failed to abort transaction: {abort_e}")
                 self.producer.flush(10)
                 raise SchemaViolation(f"Unexpected error during serialization/production: {e}") from e # Treat as schema issue if validation passed


        # If loop finishes without returning, it means retries were exhausted
        logger.critical(f"Failed to send message with key {key} after {self.retries} retries due to persistent BufferError or other retryable issue. Aborting transaction.")
        try:
            self.producer.abort_transaction()
        except KafkaException as abort_e:
            logger.error(f"Failed to abort transaction: {abort_e}")
        self.producer.flush(10)
        raise KafkaException(f"Failed to send message with key {key} after {self.retries} retries.")

    def close(self):
        """Flushes any outstanding messages and closes the producer."""
        logger.info("Flushing producer...")
        remaining = self.producer.flush(10) # 10 second timeout
        if remaining > 0:
            logger.warning(f"{remaining} messages still in queue after flush timeout.")
        logger.info("Producer closed.")

# Example Usage (Optional - for testing/demonstration)
if __name__ == "__main__":
    import os
    from dotenv import load_dotenv

    load_dotenv() # Load .env file for local testing

    logging.basicConfig(level=logging.INFO)

    KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    PROFILE_TOPIC = "profile_events"

    # Ensure topic exists (for local dev)
    admin_client = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    try:
        admin_client.create_topics([NewTopic(PROFILE_TOPIC, num_partitions=3, replication_factor=1)])
        logger.info(f"Topic {PROFILE_TOPIC} created or already exists.")
    except Exception as e:
         if "TOPIC_ALREADY_EXISTS" not in str(e):
              logger.error(f"Failed to create topic {PROFILE_TOPIC}: {e}")
              # Decide if you want to exit or continue
         else:
              logger.info(f"Topic {PROFILE_TOPIC} already exists.")


    # Load schema from file
    schema_path = os.path.join(os.path.dirname(__file__), 'schemas', 'profile_events_v1.avsc')
    try:
        with open(schema_path, 'r') as f:
            profile_schema_str = f.read()
    except FileNotFoundError:
        logger.error(f"Schema file not found at {schema_path}")
        exit(1)


    producer = None
    try:
        producer = KafkaEventProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            schema_registry_url=SCHEMA_REGISTRY_URL,
            topic=PROFILE_TOPIC,
            schema_str=profile_schema_str,
            retries=3 # Lower retries for example
        )

        # Example valid message
        valid_message = {
            "event_type": "CREATED",
            "profile_id": str(uuid.uuid4()),
            "user_id": str(uuid.uuid4()),
            "timestamp": int(time.time() * 1000),
            "payload": '{"details": "Profile setup complete."}'
        }
        producer.send(key=valid_message["profile_id"], value=valid_message)
        logger.info(f"Sent valid message: {valid_message['profile_id']}")

        # Example invalid message (missing required field)
        invalid_message = {
            "event_type": "UPDATED",
            # "profile_id": str(uuid.uuid4()), # Missing
            "user_id": str(uuid.uuid4()),
            "timestamp": int(time.time() * 1000),
            "payload": '{"update": "Preferences changed."}'
        }
        try:
            producer.send(key="invalid-key", value=invalid_message)
        except SchemaViolation as e:
            logger.info(f"Successfully caught expected SchemaViolation: {e}")
        except Exception as e:
             logger.error(f"Caught unexpected exception for invalid message: {e}")


    except KafkaException as e:
        logger.error(f"Kafka related error: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    finally:
        if producer:
            producer.close()