import asyncio
import json
import os
from aiokafka import AIOKafkaProducer
from typing import Optional, Dict, Any

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CHART_CALCULATED_TOPIC = "CHART_CALCULATED"

# --- Kafka Producer Instance ---
kafka_producer: Optional[AIOKafkaProducer] = None

async def get_kafka_producer() -> AIOKafkaProducer:
    """
    Returns the AIOKafkaProducer instance, initializing and starting it if necessary.
    Manages lifecycle with FastAPI app events ideally.
    """
    global kafka_producer
    if kafka_producer is None:
        try:
            producer = AIOKafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                # Add other producer configurations as needed (acks, retries, etc.)
                # acks='all',
                # enable_idempotence=True, # Requires acks='all' and max_in_flight_requests_per_connection <= 5
            )
            await producer.start()
            kafka_producer = producer
            print(f"Successfully connected to Kafka and started producer: {KAFKA_BOOTSTRAP_SERVERS}")
        except Exception as e:
            print(f"Error connecting to Kafka or starting producer at {KAFKA_BOOTSTRAP_SERVERS}: {e}")
            kafka_producer = None # Ensure it's None if connection failed
            # App might proceed without Kafka if it's not critical, or raise error.
    return kafka_producer

async def stop_kafka_producer():
    """Stops the AIOKafkaProducer."""
    global kafka_producer
    if kafka_producer:
        try:
            await kafka_producer.stop()
            print("Kafka producer stopped.")
        except Exception as e:
            print(f"Error stopping Kafka producer: {e}")
        finally:
            kafka_producer = None


async def send_kafka_message(topic: str, message: Dict[str, Any], key: Optional[str] = None):
    """
    Sends a message to the specified Kafka topic.
    The message (value) will be JSON serialized.
    """
    producer = await get_kafka_producer()
    if not producer:
        print(f"Kafka message NOT sent to topic '{topic}': Producer not available. Message: {message}")
        return

    try:
        key_bytes = key.encode('utf-8') if key else None
        await producer.send_and_wait(topic, value=message, key=key_bytes)
        print(f"Message successfully sent to Kafka topic '{topic}'. Key: {key}. Message: {message}")
    except Exception as e:
        print(f"Error sending message to Kafka topic '{topic}': {e}. Message: {message}")
        # Implement retry logic or dead-letter queue if necessary for critical messages.

# --- FastAPI Event Handlers (to be registered in main.py) ---
async def startup_kafka_event():
    """FastAPI startup event to initialize Kafka producer."""
    await get_kafka_producer()

async def shutdown_kafka_event():
    """FastAPI shutdown event to stop Kafka producer."""
    # Give pending tasks a moment to run, especially in test scenarios
    await asyncio.sleep(0.01)
    await stop_kafka_producer()


# Example of how to use (for direct testing of this module)
if __name__ == "__main__":
    async def test_kafka_producer():
        print("Testing Kafka Producer Module...")
        # Ensure Kafka server is running for this test on localhost:9092
        # and topic CHART_CALCULATED exists (or auto-creation is enabled)

        await startup_kafka_event() # Initialize producer

        if kafka_producer: # Check if producer was successfully initialized
            test_payload = {
                "calculation_type": "test_event",
                "request_payload": {"birth_date": "2023-01-01", "birth_time": "12:00:00"},
                "result_summary": {"info": "This is a test"},
                "timestamp": "2023-01-01T12:00:00Z",
                "service_version": "0.0.test"
            }
            await send_kafka_message(CHART_CALCULATED_TOPIC, test_payload, key="test_key_123")
            
            # Send another message without a key
            await send_kafka_message(CHART_CALCULATED_TOPIC, {**test_payload, "info": "message 2, no key"})

        await shutdown_kafka_event() # Stop producer

    asyncio.run(test_kafka_producer())