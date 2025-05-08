import logging
import time
import time
import uuid
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

import json # Import json for parsing schema string if needed by AvroSchema

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
# Import AvroSchema if needed for explicit parsing, though Deserializer might handle strings
from confluent_kafka.schema_registry.avro import AvroSchema, AvroDeserializer # Import AvroSchema
from confluent_kafka.serialization import SerializationContext, MessageField

# Assuming producer is in the same directory or accessible via PYTHONPATH
from .producer import KafkaEventProducer, SchemaViolation

logger = logging.getLogger(__name__)

class KafkaEventConsumer(ABC):
    """
    Abstract base class for Kafka consumers with Avro deserialization,
    error handling (DLQ), and manual commit strategy.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        schema_registry_url: str,
        topic: str,
        group_id: str,
        schema_str: str, # Schema for the *value* of the main topic
        auto_offset_reset: str = 'earliest',
        dlq_topic: Optional[str] = None,
        max_poll_records: int = 500,
        poll_timeout: float = 1.0,
        dlq_producer: Optional[KafkaEventProducer] = None, # Optional pre-configured producer
    ):
        """
        Initializes the KafkaEventConsumer.

        Args:
            bootstrap_servers: Kafka broker list.
            schema_registry_url: URL for the Confluent Schema Registry.
            topic: The Kafka topic to consume messages from.
            group_id: The consumer group ID.
            schema_str: The Avro schema string for message values.
            auto_offset_reset: 'earliest' or 'latest'.
            dlq_topic: Name of the Dead Letter Queue topic. If None, errors are logged but not forwarded.
            max_poll_records: Maximum number of records to fetch in one poll call.
            poll_timeout: Timeout in seconds for the poll call.
            dlq_producer: An optional pre-configured KafkaEventProducer for the DLQ.
                          If not provided and dlq_topic is set, a basic producer will be created.
        """
        self.bootstrap_servers = bootstrap_servers
        self.schema_registry_url = schema_registry_url
        self.topic = topic
        self.group_id = group_id
        self.auto_offset_reset = auto_offset_reset
        self.dlq_topic = dlq_topic or f"{topic}_dlq" # Default DLQ name convention
        self.max_poll_records = max_poll_records
        self.poll_timeout = poll_timeout
        self._running = True

        # --- Schema Registry Client & Deserializer ---
        schema_registry_conf = {"url": self.schema_registry_url}
        # Add retry logic for SchemaRegistryClient initialization
        sr_client = None
        for attempt in range(3): # Retry up to 3 times
            try:
                sr_client = SchemaRegistryClient(schema_registry_conf)
                logger.info("SchemaRegistryClient initialized successfully.")
                break
            except Exception as e:
                logger.warning(f"SchemaRegistryClient init attempt {attempt+1} failed: {e}. Retrying in 2s...")
                if attempt >= 2:
                    logger.error("Failed to initialize SchemaRegistryClient after multiple retries.")
                    raise # Re-raise the last exception
                time.sleep(2) # Need to import time
        if sr_client is None:
             raise RuntimeError("Failed to initialize SchemaRegistryClient.")
        self.schema_registry_client = sr_client


        # Simplify: Pass schema string directly to deserializer.
        # The deserializer will use this as the reader schema hint and fetch
        # the actual writer schema from the registry based on the message's schema ID.
        # Removed the problematic AvroSchema() validation call.
        self.avro_deserializer = AvroDeserializer(
            self.schema_registry_client,
            schema_str # Pass the raw schema string as reader schema hint
        )

        # --- Consumer Configuration ---
        consumer_conf = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "auto.offset.reset": self.auto_offset_reset,
            "enable.auto.commit": False, # Manual commits
            "partition.assignment.strategy": "cooperative-sticky", # Incremental rebalancing
            "isolation.level": "read_committed", # For EOS consumers if producer uses transactions
            # Fetch settings
            "max.poll.interval.ms": 300000, # Max time between polls before considered dead
            "fetch.max.bytes": 52428800, # 50MB
            "fetch.min.bytes": 1,
            # "fetch.max.wait.ms": 500, # Removed invalid property
        }
        self.consumer = Consumer(consumer_conf)

        # Ensure the consumer is ready even when clients call consume() directly,
        # as the test-suite does. Idempotent – Kafka allows re-subscribing later.
        try:
            self.consumer.subscribe([self.topic])
            logger.info("Subscribed to topic %s during __init__ for direct-poll support", self.topic)
        except KafkaException as e:
            logger.error(f"Failed to subscribe to topic {self.topic} during __init__: {e}")
            # Depending on requirements, might want to raise an error here
            # raise RuntimeError(f"Failed to subscribe to topic {self.topic}") from e

        # --- DLQ Producer ---
        self.dlq_producer = None
        if self.dlq_topic:
            if dlq_producer:
                self.dlq_producer = dlq_producer
                logger.info(f"Using provided DLQ producer for topic: {self.dlq_topic}")
            else:
                # Create a basic producer for the DLQ if none provided
                # Note: This DLQ producer won't have schema validation itself,
                # it just forwards the raw bytes or potentially re-serializes if needed.
                # For simplicity, we'll assume forwarding raw bytes is sufficient.
                # A more robust solution might involve a specific DLQ schema or handling.
                logger.warning(f"No DLQ producer provided. Creating a basic producer for DLQ: {self.dlq_topic}. Schema validation will not be applied by this basic DLQ producer.")
                # This basic producer doesn't need schema registry or complex settings
                dlq_producer_conf = {
                    "bootstrap.servers": self.bootstrap_servers,
                    "acks": "all",
                     # Basic producer doesn't need idempotence/transactions unless specifically required
                }
                # We'll create a simple confluent_kafka Producer, not our custom KafkaEventProducer
                # to avoid schema complexities for the DLQ forwarding.
                from confluent_kafka import Producer as BasicProducer # Avoid circular import if possible
                self.dlq_producer = BasicProducer(dlq_producer_conf)
                logger.info(f"Created basic DLQ producer for topic: {self.dlq_topic}")


    @abstractmethod
    def handle(self, message_key: Optional[str], message_value: Dict[str, Any]) -> None:
        """
        Process a single deserialized Kafka message.
        This method must be implemented by subclasses.

        Args:
            message_key: The deserialized message key (or None).
            message_value: The deserialized Avro message value as a dictionary.

        Raises:
            Exception: Any exception raised during handling will be caught and
                       potentially trigger DLQ processing.
        """
        pass

    def _dlq_delivery_report(self, err, msg):
        """Callback for DLQ produce requests."""
        if err is not None:
            logger.error(f"Failed to deliver message to DLQ topic {msg.topic()}: {err}")
        else:
            logger.info(f"Message successfully forwarded to DLQ topic {msg.topic()} partition {msg.partition()}")

    def process_messages(self, msgs: List[Any]) -> List[TopicPartition]:
        """
        Processes a list of pre-fetched Kafka messages.

        This method encapsulates the deserialization, handling, error processing,
        and DLQ logic for a batch of messages, similar to the core of consume_loop.
        It's designed to be called directly in scenarios like testing where
        the consume loop isn't run explicitly.

        Args:
            msgs: A list of raw message objects obtained from consumer.consume().

        Returns:
            A list of TopicPartition objects representing the offsets that were
            successfully processed (or sent to DLQ) and are ready to be committed.
            Returns an empty list if no messages were processed successfully.
        """
        if not msgs:
            return []

        last_processed_offsets: Dict[int, TopicPartition] = {} # Track last successful offset per partition

        for msg in msgs:
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug(f"Reached end of partition: {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
                elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                     logger.error(f"Fatal error: Unknown topic or partition for {self.topic}. Stopping processing.")
                     # In direct processing, we can't stop the loop, just stop processing this batch
                     return list(last_processed_offsets.values()) # Return offsets processed so far
                elif msg.error():
                    logger.error(f"Kafka consume error: {msg.error()}. Skipping message.")
                    # Don't advance offset for this partition on error
                continue # Skip processing this message

            # --- Message Deserialization ---
            try:
                message_key = msg.key().decode('utf-8') if msg.key() else None
                serialization_context = SerializationContext(msg.topic(), MessageField.VALUE)
                message_value = self.avro_deserializer(msg.value(), serialization_context)

                if message_value is None:
                     logger.warning(f"Deserialized value is None for key {message_key} at offset {msg.offset()}. Skipping.")
                     last_processed_offsets[msg.partition()] = TopicPartition(msg.topic(), msg.partition(), msg.offset() + 1)
                     continue

            except Exception as deser_error:
                logger.error(f"Deserialization failed for message at offset {msg.offset()}: {deser_error}. Attempting DLQ.", exc_info=True)
                if self.dlq_producer and self.dlq_topic:
                    try:
                        self.dlq_producer.produce(
                            topic=self.dlq_topic,
                            key=msg.key(), value=msg.value(), headers=msg.headers(),
                            callback=self._dlq_delivery_report
                        )
                        # Flush the basic DLQ producer to ensure message is sent
                        remaining = self.dlq_producer.flush(timeout=5.0) # 5 second timeout
                        if remaining > 0:
                             logger.warning(f"DLQ producer flush timed out, {remaining} messages may not have been sent.")
                             # Decide if this should prevent offset commit - likely yes
                             return list(last_processed_offsets.values()) # Return offsets processed *before* this failed DLQ send
                        logger.debug("DLQ producer flushed successfully after deserialization error.")
                        last_processed_offsets[msg.partition()] = TopicPartition(msg.topic(), msg.partition(), msg.offset() + 1)
                    except Exception as dlq_error:
                        logger.critical(f"Failed to produce message to DLQ topic {self.dlq_topic} after deserialization error: {dlq_error}", exc_info=True)
                        # Stop processing batch if DLQ fails
                        return list(last_processed_offsets.values()) # Return offsets processed so far
                else:
                     logger.error("DLQ not configured or producer unavailable. Message lost after deserialization error.")
                     # Stop processing batch if DLQ not available
                     return list(last_processed_offsets.values()) # Return offsets processed so far
                continue

            # --- Message Handling ---
            try:
                logger.debug(f"Handling message - Key: {message_key}, Offset: {msg.offset()}")
                self.handle(message_key, message_value)
                last_processed_offsets[msg.partition()] = TopicPartition(msg.topic(), msg.partition(), msg.offset() + 1)

            except Exception as handle_error:
                logger.error(f"Error handling message at offset {msg.offset()}: {handle_error}", exc_info=True)
                if self.dlq_producer and self.dlq_topic:
                    try:
                        logger.info(f"Forwarding message (offset {msg.offset()}) to DLQ: {self.dlq_topic}")
                        self.dlq_producer.produce(
                            topic=self.dlq_topic,
                            key=msg.key(), value=msg.value(), headers=msg.headers(),
                            callback=self._dlq_delivery_report
                        )
                        # Flush the basic DLQ producer to ensure message is sent
                        remaining = self.dlq_producer.flush(timeout=5.0) # 5 second timeout
                        if remaining > 0:
                             logger.warning(f"DLQ producer flush timed out, {remaining} messages may not have been sent.")
                             # Decide if this should prevent offset commit - likely yes
                             return list(last_processed_offsets.values()) # Return offsets processed *before* this failed DLQ send
                        logger.debug("DLQ producer flushed successfully after handling error.")
                        last_processed_offsets[msg.partition()] = TopicPartition(msg.topic(), msg.partition(), msg.offset() + 1)
                    except Exception as dlq_error:
                        logger.critical(f"Failed to produce message to DLQ topic {self.dlq_topic} after handling error: {dlq_error}", exc_info=True)
                        # Stop processing batch if DLQ fails
                        return list(last_processed_offsets.values()) # Return offsets processed so far
                else:
                    logger.error("DLQ not configured or producer unavailable. Message lost after handling error.")
                    # Stop processing batch if DLQ not available
                    return list(last_processed_offsets.values()) # Return offsets processed so far
                # Continue processing the rest of the batch even if one message fails and goes to DLQ

        return list(last_processed_offsets.values())


    def consume_loop(self):
        """Starts the main consumption loop."""
        logger.info(f"Subscribing to topic: {self.topic}")
        self.consumer.subscribe([self.topic])
        logger.info(f"Consumer loop started for group {self.group_id} on topic {self.topic}. Waiting for messages...")

        try:
            while self._running:
                # Poll for messages
                msgs = self.consumer.consume(num_messages=self.max_poll_records, timeout=self.poll_timeout)

                if not msgs:
                    # logger.debug("No messages received in this poll interval.")
                    continue # No messages, continue polling

                # Process the fetched messages using the dedicated method
                offsets_to_commit = self.process_messages(msgs)

                # --- Batch Commit ---
                if offsets_to_commit:
                    try:
                        logger.debug(f"Committing offsets: {offsets_to_commit}")
                        self.consumer.commit(offsets=offsets_to_commit, asynchronous=False) # Synchronous commit
                        logger.info(f"Successfully committed offsets up to: {[f'P{tp.partition}:{tp.offset}' for tp in offsets_to_commit]}")
                    except KafkaException as commit_error:
                        logger.error(f"Failed to commit offsets {offsets_to_commit}: {commit_error}")
                        # This is serious, likely requires intervention or a consumer restart strategy
                        # For now, log and continue, hoping the next poll/commit succeeds.
                        # Consider stopping the consumer if commits fail repeatedly.
                        self._running = False # Stop consumer on commit failure
                        logger.critical("Stopping consumer due to commit failure.")


        except KeyboardInterrupt:
            logger.info("Shutdown signal received.")
        except Exception as e:
            logger.critical(f"Unexpected error in consumer loop: {e}", exc_info=True)
        finally:
            self.close()

    def stop(self):
        """Signals the consumer loop to stop."""
        logger.info("Stop signal received. Consumer will shut down after current poll.")
        self._running = False

    def close(self):
        """Closes the Kafka consumer and DLQ producer."""
        logger.info("Closing Kafka consumer...")
        if self.consumer:
            try:
                # Attempt to commit final offsets before closing
                # Note: This might fail if called after an error that prevented commits
                # assigned_partitions = self.consumer.assignment()
                # if assigned_partitions:
                #    self.consumer.commit(asynchronous=False) # Commit current position
                self.consumer.close()
                logger.info("Kafka consumer closed.")
            except Exception as e:
                logger.error(f"Error closing Kafka consumer: {e}")
        if self.dlq_producer:
             logger.info("Flushing and closing DLQ producer...")
             try:
                 # Check if it's our custom producer or the basic one
                 if isinstance(self.dlq_producer, KafkaEventProducer):
                     self.dlq_producer.close()
                 elif hasattr(self.dlq_producer, 'flush'): # Basic confluent_kafka.Producer
                     self.dlq_producer.flush(10) # 10 second timeout
                 logger.info("DLQ producer closed.")
             except Exception as e:
                 logger.error(f"Error closing DLQ producer: {e}")

# Note: Concrete implementation (DummyConsumer) will be added in the test file later.