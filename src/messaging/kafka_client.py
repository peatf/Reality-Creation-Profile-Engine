import json
import logging
import os
import socket
from typing import Optional

from confluent_kafka import Producer
from confluent_kafka.error import KafkaError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global Kafka Producer instance
_producer_instance: Optional[Producer] = None

def _get_kafka_config() -> dict:
    """Builds the Kafka configuration dictionary."""
    bootstrap_servers_str = os.environ.get('KAFKA_BOOTSTRAP', 'localhost:9092') # Changed env var name to match docker-compose
    config = {
        'bootstrap.servers': bootstrap_servers_str,
        'client.id': socket.gethostname(),
        # Default retry config (can be tuned)
        'retries': 5,
        'message.timeout.ms': 10000, # 10 seconds per message attempt
        # 'linger.ms': 10 # Optional: Batch messages
        # Add security config here if needed (SASL, SSL)
    }
    logger.info(f"Kafka Producer config: {config}")
    return config

def _delivery_report(err: Optional[KafkaError], msg):
    """Callback function for Kafka message delivery reports."""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.debug( # Changed to debug to reduce noise on success
            f"Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}"
        )

def get_producer() -> Optional[Producer]:
    """Initializes and returns the Kafka Producer instance."""
    global _producer_instance
    if _producer_instance is None:
        try:
            config = _get_kafka_config()
            _producer_instance = Producer(config)
            logger.info("Confluent Kafka Producer initialized.")
            # Note: Connection is typically lazy with confluent-kafka
        except KafkaError as e:
            logger.error(f"Failed to initialize Confluent Kafka Producer (KafkaError): {e}")
            _producer_instance = None
        except Exception as e:
            logger.error(f"Failed to initialize Confluent Kafka Producer (Other Error): {e}")
            _producer_instance = None
    return _producer_instance

def send_event(topic: str, payload: dict, key: Optional[str] = None):
    """
    Sends an event payload to the specified Kafka topic using confluent-kafka.

    Args:
        topic: The Kafka topic to send the event to.
        payload: A dictionary representing the event data (will be JSON encoded).
        key: Optional message key (string, will be utf-8 encoded).
    """
    producer = get_producer()
    if producer is None:
        logger.error("Kafka producer is not initialized. Cannot send event.")
        return

    try:
        # Serialize payload to JSON bytes
        serialized_payload = json.dumps(payload).encode('utf-8')
        serialized_key = key.encode('utf-8') if key else None

        # Produce message asynchronously
        producer.produce(
            topic,
            value=serialized_payload,
            key=serialized_key,
            callback=_delivery_report
        )
        # Poll for delivery reports (or handle in background thread)
        # Polling with 0 timeout serves already-queued callbacks without blocking
        producer.poll(0)

    except BufferError:
        # This occurs if the internal producer queue is full
        logger.error(f"Kafka producer queue is full for topic '{topic}'. Flushing and retrying or handling error.")
        # Consider flushing and retrying, or implementing better backpressure handling
        producer.flush(5) # Wait up to 5 seconds to flush
        # Optionally retry: producer.produce(...)
    except Exception as e:
        logger.error(f"Error producing event to Kafka topic '{topic}': {e}")

def flush_producer(timeout: float = 10.0):
    """Flushes the producer queue, ensuring all messages are sent."""
    producer = get_producer()
    if producer:
        remaining = producer.flush(timeout)
        if remaining > 0:
            logger.warning(f"Producer flush timed out, {remaining} messages still in queue.")
        else:
            logger.info("Producer flushed successfully.")

# Example usage (optional, for testing)
# if __name__ == "__main__":
#     test_topic = "user_events"
#     test_payload = {"user_id": 456, "action": "update_profile", "timestamp": "2025-05-05T14:00:00Z"}
#     send_event(test_topic, test_payload, key=str(test_payload["user_id"]))
#     # Ensure messages are sent before the script exits
#     flush_producer()