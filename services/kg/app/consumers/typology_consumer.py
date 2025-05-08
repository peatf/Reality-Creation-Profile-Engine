import asyncio
import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime, timezone # For PersonNode creation/update

from ..core.config import kafka_settings, neo4j_settings # Import both settings objects
from config.kafka import TOPIC_TYPOLOGY_ASSESSED # Keep topic name from global config for now, or move it too
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError

# Import CRUD operations
from ..crud.person_crud import create_person, get_person_by_user_id, update_person
from ..crud.typology_crud import create_typology_result, get_typology_result_by_assessment_id
from ..crud.relationship_crud import link_person_to_typologyresult
from ..graph_schema.nodes import PersonNode, TypologyResultNode
from ..graph_schema.relationships import HasTypologyResultProperties
from ..crud.base_dao import NodeCreationError, UniqueConstraintViolationError, DAOException

# Schema for the event payload (simplified based on src/schemas/typology.py)
# The actual event might have more fields like assessment_id, user_id (if not key), timestamp
from src.schemas.typology import AssessmentResult as TypologyAssessmentEventPayload # Assuming this is the core part

logger = logging.getLogger(__name__)

EXPECTED_EVENT_TYPE_TYPOLOGY = "TYPOLOGY_ASSESSED"

class TypologyAssessedConsumer:
    def __init__(self, group_id="kg_typology_consumer_group"):
        self.group_id = group_id
        self.consumer = AIOKafkaConsumer(
            TOPIC_TYPOLOGY_ASSESSED,
            bootstrap_servers=kafka_settings.bootstrap_servers, # Use the new Kafka settings
            group_id=self.group_id,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')), # Assuming JSON messages
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        self._running = False
        self._consumer_task = None

    async def process_message(self, message_key: Optional[str], message_value: Dict[str, Any]):
        """
        Processes a single TYPOLOGY_ASSESSED event.
        """
        logger.info(f"Received TYPOLOGY_ASSESSED event. Key: {message_key}, Value (keys): {list(message_value.keys())}")

        user_id = message_key
        if not user_id:
            logger.error("TYPOLOGY_ASSESSED event missing user_id in Kafka message key. Skipping.")
            return

        try:
            # Extract core fields from the event value
            assessment_id = message_value.get("assessment_id")
            typology_name = message_value.get("typology_name")
            # 'score' might be named differently, e.g., 'result_label', 'type_determined', or inside 'trace'
            score = message_value.get("score") or message_value.get("result_label")
            confidence = message_value.get("confidence")
            # 'details' could be the 'trace' field or another specific field for raw results
            details_payload = message_value.get("details") or message_value.get("trace")

            if not all([assessment_id, typology_name, score is not None]): # score can be empty string but must exist
                logger.error(f"TYPOLOGY_ASSESSED event for user {user_id} is missing one or more required fields (assessment_id, typology_name, score). Value: {message_value}. Skipping.")
                return

            # 1. Ensure Person Node exists (create if not, using placeholder birth data if needed)
            person_node = await get_person_by_user_id(user_id)
            if not person_node:
                logger.info(f"PersonNode for user_id {user_id} not found. Creating a basic one.")
                person_payload = PersonNode(user_id=user_id, name=f"User {user_id}") # Basic info
                try:
                    person_node = await create_person(person_payload)
                    logger.info(f"Created placeholder PersonNode for user_id: {user_id}")
                except NodeCreationError as e: # Handles UniqueConstraintViolationError
                    logger.warning(f"Placeholder PersonNode creation failed for {user_id} (likely race or already exists): {e}. Fetching again.")
                    person_node = await get_person_by_user_id(user_id)
                    if not person_node:
                        logger.error(f"Failed to create or find PersonNode for {user_id} for typology event. Skipping.")
                        return

            # 2. Create TypologyResult Node
            typology_result_payload = TypologyResultNode(
                assessment_id=assessment_id,
                typology_name=typology_name,
                score=str(score), # Ensure score is string
                confidence=float(confidence) if confidence is not None else None,
                details=json.dumps(details_payload) if details_payload else None # Store as JSON string
            )

            # Idempotency: Check if this assessment result already exists
            existing_typology_result = await get_typology_result_by_assessment_id(assessment_id)
            if existing_typology_result:
                logger.info(f"TypologyResult with assessment_id '{assessment_id}' already exists. Skipping creation/linking.")
                typology_node = existing_typology_result
            else:
                try:
                    typology_node = await create_typology_result(typology_result_payload)
                    logger.info(f"Created TypologyResultNode for assessment_id: {assessment_id}")
                except NodeCreationError as e: # Handles UniqueConstraintViolationError
                    logger.warning(f"TypologyResultNode creation for {assessment_id} failed (likely race or already exists): {e}. Fetching.")
                    typology_node = await get_typology_result_by_assessment_id(assessment_id)
                    if not typology_node:
                        logger.error(f"Failed to create or find TypologyResultNode for {assessment_id}. Skipping linking.")
                        return

            # 3. Link Person to TypologyResult
            if person_node and typology_node:
                # Simplified: Assume link_person_to_typologyresult handles idempotency or it's acceptable for now.
                rel_props = HasTypologyResultProperties(assessment_date=message_value.get("timestamp")) # Assuming timestamp is assessment date
                await link_person_to_typologyresult(
                    user_id=user_id,
                    assessment_id=assessment_id,
                    props=rel_props
                )
                logger.info(f"Linked Person {user_id} to TypologyResult {assessment_id}")

            logger.info(f"Successfully processed TYPOLOGY_ASSESSED event for user_id: {user_id}, assessment_id: {assessment_id}")

        except Exception as e:
            logger.error(f"Error processing TYPOLOGY_ASSESSED message for user {user_id}: {e}", exc_info=True)
            # Implement DLQ or other error handling strategy here

    async def consume(self):
        logger.info(f"Starting TypologyAssessedConsumer for topic: {TOPIC_TYPOLOGY_ASSESSED}")
        self._running = True
        try:
            await self.consumer.start()
            logger.info(f"AIOKafkaConsumer started for topic {TOPIC_TYPOLOGY_ASSESSED}, group {self.group_id}")

            async for message in self.consumer:
                if not self._running:
                    break
                logger.debug(f"Received raw message: Topic={message.topic}, Partition={message.partition}, Offset={message.offset}, Key={message.key}, Value={message.value[:100]}...") # Log snippet
                try:
                    # Deserialization is handled by AIOKafkaConsumer config
                    key = message.key
                    value = message.value

                    # Optional: Check event type if it's in headers or payload
                    # event_type_field = value.get("event_type", "").upper()
                    # if event_type_field == EXPECTED_EVENT_TYPE_TYPOLOGY:
                    await self.process_message(key, value)
                    # else:
                    #    logger.debug(f"Skipping message with type '{event_type_field}' on topic {TOPIC_TYPOLOGY_ASSESSED}")

                except json.JSONDecodeError as e: # Should be handled by deserializer now
                    logger.error(f"Failed to decode JSON message from {TOPIC_TYPOLOGY_ASSESSED}: {e}. Value: {message.value[:200]}")
                except Exception as e:
                    logger.error(f"Unexpected error processing message in consumer loop for {TOPIC_TYPOLOGY_ASSESSED}: {e}", exc_info=True)

                if not self._running:
                    break
        except KafkaError as e:
             logger.error(f"Kafka error during consumption: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"TypologyAssessedConsumer run error: {e}", exc_info=True)
        finally:
            logger.info(f"Stopping TypologyAssessedConsumer for topic: {TOPIC_TYPOLOGY_ASSESSED}.")
            if self.consumer:
                 await self.consumer.stop()
            self._running = False

    async def start_consuming(self):
        """Starts the consumer loop as a background task."""
        if not self._consumer_task or self._consumer_task.done():
             self._consumer_task = asyncio.create_task(self.consume())
             logger.info("Typology consumer task created.")
        else:
             logger.warning("Typology consumer task already running.")

    async def stop(self):
        logger.info("TypologyAssessedConsumer stop requested.")
        self._running = False
        if self.consumer:
            await self.consumer.stop() # Ensure consumer is stopped
        if self._consumer_task:
            try:
                await asyncio.wait_for(self._consumer_task, timeout=5.0)
                logger.info("Typology consumer task finished.")
            except asyncio.TimeoutError:
                logger.warning("Typology consumer task did not finish within timeout during stop.")
            except asyncio.CancelledError:
                 logger.info("Typology consumer task was cancelled.")
            self._consumer_task = None

async def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    consumer = TypologyAssessedConsumer()
    consumer_task = None
    try:
        consumer_task = asyncio.create_task(consumer.consume())
        while True:
            await asyncio.sleep(1)
            if consumer_task.done():
                 logger.info("Consumer task finished unexpectedly.")
                 break
    except KeyboardInterrupt:
        logger.info("Typology consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Main loop error: {e}", exc_info=True)
    finally:
        if consumer:
            await consumer.stop()
        if consumer_task and not consumer_task.done():
             consumer_task.cancel()
             try:
                 await consumer_task
             except asyncio.CancelledError:
                 logger.info("Typology consumer task cancelled.")
        from ..core.db import Neo4jDatabase
        await Neo4jDatabase.close_async_driver()
        logger.info("Neo4j driver closed.")

if __name__ == "__main__":
    # This requires Kafka and Neo4j to be running.
    # And the TOPIC_TYPOLOGY_ASSESSED topic to exist, with messages.
    # The producer for these events (e.g., from src/events/producer.py with typology schema)
    # would need to be run to send test messages.
    asyncio.run(main())