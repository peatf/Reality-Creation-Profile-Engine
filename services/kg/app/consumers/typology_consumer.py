import asyncio
import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime, timezone # For PersonNode creation/update

from ..core.config import kafka_settings, neo4j_settings # Import both settings objects
from config.kafka import TOPIC_TYPOLOGY_ASSESSED # Keep topic name from global config for now, or move it too
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaError # Import only base KafkaError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from neo4j.exceptions import ServiceUnavailable as Neo4jServiceUnavailable
from pydantic import ValidationError

# Import CRUD operations
from ..crud.person_crud import upsert_person, get_person_by_user_id, update_person_properties # Corrected imports
from ..crud.typology_crud import upsert_typology_result, get_typology_result_by_assessment_id # Corrected import
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
            enable_auto_commit=False, # Manual commits
        )
        self.dlq_producer = AIOKafkaProducer(
            bootstrap_servers=kafka_settings.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self._running = False
        self._consumer_task = None
        self._dlq_producer_started = False

    async def _send_to_dlq(self, key: Optional[str], value: Any, error_type: str, error_message: str, original_topic: str):
        """Sends a message to the Dead-Letter Queue."""
        dlq_message = {
            "original_key": key,
            "original_value": value,
            "original_topic": original_topic,
            "error_type": error_type,
            "error_message": error_message,
            "dlq_timestamp": datetime.now(timezone.utc).isoformat()
        }
        try:
            logger.warning(f"Sending message for key {key} from topic {original_topic} to DLQ topic {kafka_settings.typology_consumer_dlq_topic} due to {error_type}")
            await self.dlq_producer.send_and_wait(kafka_settings.typology_consumer_dlq_topic, value=dlq_message, key=key)
            logger.info(f"Successfully sent message for key {key} to DLQ topic {kafka_settings.typology_consumer_dlq_topic}.")
        except KafkaError as e: # Catch base KafkaError
            logger.error(f"KafkaError sending message for key {key} to DLQ topic {kafka_settings.typology_consumer_dlq_topic}: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error sending message for key {key} to DLQ: {e}", exc_info=True)
            raise

    async def process_message_with_retry(self, message_key: Optional[str], message_value: Dict[str, Any], topic: str, partition: int, offset: int):
        """
        Processes a single message with retry logic for transient errors.
        If persistent error, sends to DLQ.
        """
        try:
            @retry(
                stop=stop_after_attempt(3),
                wait=wait_exponential(multiplier=1, min=2, max=10),
                retry=retry_if_exception_type(Neo4jServiceUnavailable),
                reraise=True
            )
            async def _process_logic():
                logger.info(f"Attempting to process TYPOLOGY_ASSESSED message for key {message_key}, attempt: {_process_logic.retry.statistics['attempt_number']}")
                await self.process_message(message_key, message_value)

            await _process_logic()
            logger.info(f"Successfully processed TYPOLOGY_ASSESSED message for key {message_key} after {_process_logic.retry.statistics.get('attempt_number', 1)} attempts.")

        except ValidationError as e:
            logger.error(f"ValidationError processing TYPOLOGY_ASSESSED message for key {message_key} from {topic} P:{partition} O:{offset}. Error: {e}", exc_info=True)
            await self._send_to_dlq(message_key, message_value, "validation_error", str(e), original_topic=topic)
        except (UniqueConstraintViolationError, NodeCreationError) as e:
            logger.error(f"Persistent DAO Error processing TYPOLOGY_ASSESSED message for key {message_key} from {topic} P:{partition} O:{offset}. Error: {e}", exc_info=True)
            await self._send_to_dlq(message_key, message_value, "persistent_dao_error", str(e), original_topic=topic)
        except Neo4jServiceUnavailable as e: # Retries exhausted
            logger.error(f"Neo4jServiceUnavailable after retries for TYPOLOGY_ASSESSED key {message_key} from {topic} P:{partition} O:{offset}. Error: {e}", exc_info=True)
            await self._send_to_dlq(message_key, message_value, "neo4j_unavailable_after_retries", str(e), original_topic=topic)
        except DAOException as e:
            logger.error(f"Unhandled DAOException processing TYPOLOGY_ASSESSED message for key {message_key} from {topic} P:{partition} O:{offset}. Error: {e}", exc_info=True)
            await self._send_to_dlq(message_key, message_value, "unhandled_dao_exception", str(e), original_topic=topic)
        except ValueError as e: # Custom value errors (e.g. missing key/payload)
            logger.error(f"ValueError (likely malformed TYPOLOGY_ASSESSED message) for key {message_key} from {topic} P:{partition} O:{offset}. Error: {e}", exc_info=True)
            await self._send_to_dlq(message_key, message_value, "value_error_malformed_message", str(e), original_topic=topic)
        except Exception as e:
            logger.error(f"Unexpected error in process_message_with_retry for TYPOLOGY_ASSESSED key {message_key} from {topic} P:{partition} O:{offset}: {e}", exc_info=True)
            await self._send_to_dlq(message_key, message_value, "unexpected_processing_error", str(e), original_topic=topic)

    async def process_message(self, message_key: Optional[str], message_value: Dict[str, Any]):
        """
        Core logic for processing a single TYPOLOGY_ASSESSED event.
        Raises exceptions for errors to be handled by process_message_with_retry.
        """
        # logger.info(f"Received TYPOLOGY_ASSESSED event. Key: {message_key}, Value (keys): {list(message_value.keys())}") # Moved to retry wrapper

        user_id = message_key
        if not user_id:
            logger.error("TYPOLOGY_ASSESSED event missing user_id in Kafka message key.")
            raise ValueError("TYPOLOGY_ASSESSED event missing user_id in Kafka message key.")

        # Pydantic validation for the event payload could be done here if a schema is defined for the whole event
        # For now, we validate fields manually.

        assessment_id = message_value.get("assessment_id")
        typology_name = message_value.get("typology_name")
        score = message_value.get("score") or message_value.get("result_label") # Adapt to actual field name
        confidence = message_value.get("confidence")
        details_payload = message_value.get("details") or message_value.get("trace") # Adapt
        timestamp_str = message_value.get("timestamp") # Expecting ISO format string

        if not all([assessment_id, typology_name, score is not None]):
            err_msg = f"TYPOLOGY_ASSESSED event for user {user_id} is missing one or more required fields (assessment_id, typology_name, score). Value: {message_value}."
            logger.error(err_msg)
            raise ValueError(err_msg)
        
        # Optional: Validate with Pydantic if a schema for the message_value exists
        # try:
        #     event_payload = TypologyAssessmentEventPayload(**message_value) # Or a more specific schema for the Kafka message
        # except ValidationError as e:
        #     logger.error(f"Pydantic ValidationError for TYPOLOGY_ASSESSED event user {user_id}: {e}")
        #     raise # Let retry handler catch this

        # 1. Ensure Person Node exists
        person_node = await get_person_by_user_id(user_id)
        if not person_node:
            logger.info(f"PersonNode for user_id {user_id} not found. Creating a basic one.")
            # Ensure a profile_id is assigned if creating a new person.
            # Using user_id as profile_id if no other logic dictates profile_id generation.
            # Alternatively, generate a new UUID: import uuid; new_profile_id = str(uuid.uuid4())
            new_profile_id_for_person = user_id # Or generate a new one
            person_payload = PersonNode(user_id=user_id, name=f"User {user_id}", profile_id=new_profile_id_for_person)
            person_node = await upsert_person(person_payload)
            logger.info(f"Upserted (created) placeholder PersonNode for user_id: {user_id} with profile_id: {new_profile_id_for_person}")
        elif not person_node.profile_id:
            # If person exists but profile_id is None, assign one.
            # This might indicate an older record or incomplete data.
            # Again, using user_id or generating a new UUID.
            logger.warning(f"PersonNode for user_id {user_id} found but missing profile_id. Assigning one.")
            updated_profile_id = user_id # Or generate new
            person_node = await update_person_properties(user_id, {"profile_id": updated_profile_id})
            if not person_node: # Should not happen if update_person_properties returns the updated node or raises
                 logger.error(f"Failed to update person {user_id} with a profile_id. Cannot proceed with TypologyResult.")
                 raise DAOException(f"Failed to update person {user_id} with profile_id.")
            logger.info(f"Updated PersonNode for user_id: {user_id} with profile_id: {updated_profile_id}")

        # Ensure person_node.profile_id is available now
        if not person_node.profile_id:
            logger.error(f"Critical: profile_id still missing for user {user_id} after upsert/update attempts.")
            raise ValueError(f"profile_id is missing for user {user_id}, cannot create TypologyResultNode.")

        # 2. Create TypologyResult Node
        typology_result_payload = TypologyResultNode(
            assessment_id=str(assessment_id), # Ensure assessment_id is string
            profile_id=person_node.profile_id, # Pass the profile_id from PersonNode
            typology_name=str(typology_name),
            score=str(score),
            confidence=float(confidence) if confidence is not None else None,
            details=json.dumps(details_payload) if details_payload else None
        )

        existing_typology_result = await get_typology_result_by_assessment_id(str(assessment_id))
        if existing_typology_result:
            logger.info(f"TypologyResult with assessment_id '{assessment_id}' already exists. Using existing node.")
            typology_node = existing_typology_result
        else:
            typology_node = await upsert_typology_result(typology_result_payload) # Use upsert_typology_result
            logger.info(f"Upserted (created) TypologyResultNode for assessment_id: {assessment_id}")

        # 3. Link Person to TypologyResult
        assessment_date = None
        if timestamp_str:
            if isinstance(timestamp_str, str):
                try:
                    assessment_date = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                except ValueError:
                    logger.warning(f"Could not parse timestamp string '{timestamp_str}' for assessment {assessment_id}. Using None for assessment_date.")
                    assessment_date = None # Ensure it's None on error
            else:
                logger.warning(f"Timestamp for assessment {assessment_id} is not a string (type: {type(timestamp_str)}). Value: {timestamp_str}. Using None for assessment_date.")
                assessment_date = None
        # If timestamp_str was initially None or empty, assessment_date remains its initial None value from line 171.

        rel_props = HasTypologyResultProperties(assessment_date=assessment_date)
        await link_person_to_typologyresult( # Can raise DAOException, Neo4jServiceUnavailable
            user_id=user_id,
            assessment_id=str(assessment_id),
            props=rel_props
        )
        logger.info(f"Linked Person {user_id} to TypologyResult {assessment_id}")
        # logger.info(f"Successfully processed TYPOLOGY_ASSESSED event for user_id: {user_id}, assessment_id: {assessment_id}") # Moved to retry wrapper

    async def consume(self):
        logger.info(f"Starting TypologyAssessedConsumer for topic: {TOPIC_TYPOLOGY_ASSESSED}")
        self._running = True
        try:
            await self.consumer.start()
            logger.info(f"AIOKafkaConsumer started for topic {TOPIC_TYPOLOGY_ASSESSED}, group {self.group_id}")
            await self.dlq_producer.start()
            self._dlq_producer_started = True
            logger.info(f"AIOKafkaProducer for DLQ started.")

            async for message in self.consumer:
                if not self._running:
                    break
                logger.debug(f"Received raw message: Topic={message.topic}, Partition={message.partition}, Offset={message.offset}, Key={message.key}, Value={message.value[:100]}...") # Log snippet
                processed_successfully = False
                try:
                    key = message.key # Already deserialized
                    value = message.value # Already deserialized

                    await self.process_message_with_retry(key, value, message.topic, message.partition, message.offset)
                    processed_successfully = True

                except json.JSONDecodeError as e: # Safeguard if deserializer fails
                    logger.error(f"FATAL: JSONDecodeError directly in consume loop for {message.topic} P:{message.partition} O:{message.offset}. Value (raw): {message.value}. Error: {e}", exc_info=True)
                    raw_value_for_dlq = message.value
                    if isinstance(raw_value_for_dlq, bytes):
                        try:
                            raw_value_for_dlq = raw_value_for_dlq.decode('utf-8', errors='replace')
                        except Exception: pass
                    try:
                        await self._send_to_dlq(message.key, raw_value_for_dlq, "json_decode_error_in_consume_loop", str(e), original_topic=message.topic)
                        processed_successfully = True
                    except Exception as dlq_e:
                        logger.critical(f"Failed to send undecodable TYPOLOGY_ASSESSED message to DLQ for key {message.key}. DLQ Error: {dlq_e}", exc_info=True)
                        processed_successfully = False
                except Exception as e: # Errors from process_message_with_retry (e.g., DLQ send failure)
                    logger.error(f"Outer exception processing TYPOLOGY_ASSESSED message for key {message.key} on {message.topic} P:{message.partition} O:{message.offset}: {e}", exc_info=True)
                    processed_successfully = False

                if processed_successfully:
                    await self.consumer.commit()
                    logger.debug(f"Committed offset {message.offset} for partition {message.partition} on topic {message.topic}")
                else:
                    logger.error(f"Message for key {message.key} on {message.topic} P:{message.partition} O:{message.offset} was not processed successfully and not sent to DLQ. Offset will not be committed.")

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
            if self._dlq_producer_started and self.dlq_producer:
                await self.dlq_producer.stop()
                logger.info("DLQ KafkaProducer stopped.")
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
        if self._dlq_producer_started and self.dlq_producer:
            await self.dlq_producer.stop()
            logger.info("DLQ KafkaProducer stopped during explicit stop call.")
            self._dlq_producer_started = False
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