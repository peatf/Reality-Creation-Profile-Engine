import asyncio
import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime, timezone

from ..core.config import kafka_settings, neo4j_settings # Import both settings objects
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaError # Import only base KafkaError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from neo4j.exceptions import ServiceUnavailable as Neo4jServiceUnavailable
from pydantic import ValidationError

# Import CRUD operations
from ..crud.person_crud import upsert_person, get_person_by_user_id, update_person_properties # Corrected imports
from ..crud.astrofeature_crud import upsert_astrofeature, get_astrofeature_by_name_and_type # Corrected imports
from ..crud.hdfeature_crud import upsert_hdfeature, get_hdfeature_by_name_and_type # Corrected imports
from ..crud.relationship_crud import (
    link_person_to_astrofeature,
    link_person_to_hdfeature
)
from ..graph_schema.nodes import PersonNode, AstroFeatureNode, HDFeatureNode
from ..graph_schema.relationships import HasFeatureProperties
from ..crud.base_dao import NodeCreationError, UniqueConstraintViolationError, DAOException

# Schemas for payload validation/parsing (adjust paths if these are not directly accessible)
from services.chart_calc.app.schemas.request_schemas import CalculationRequest
from services.chart_calc.app.schemas.astrology_schemas import AstrologyChartResponse
from services.chart_calc.app.schemas.human_design_schemas import HumanDesignChartResponse


logger = logging.getLogger(__name__)
# For structured logging, a handler would be configured at the application entry point.
# Example: from pythonjsonlogger import jsonlogger

CHART_CALCULATED_TOPIC = "CHART_CALCULATED" # From services/chart_calc/app/core/kafka_producer.py
EXPECTED_EVENT_TYPE = "CHART_CALCULATED" # Assuming event type is part of message or implied by topic

class ChartCalculatedConsumer:
    def __init__(self, group_id="kg_chart_consumer_group"):
        self.group_id = group_id
        self.consumer = AIOKafkaConsumer(
            CHART_CALCULATED_TOPIC,
            bootstrap_servers=kafka_settings.bootstrap_servers, # Use the new Kafka settings
            group_id=self.group_id,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')), # Assuming JSON messages
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset="earliest", # Or "latest" depending on requirement
            enable_auto_commit=False, # Manual commits
            # Add other aiokafka consumer configurations if needed
        )
        self.dlq_producer = AIOKafkaProducer(
            bootstrap_servers=kafka_settings.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8') # Assuming DLQ messages are JSON
        )
        self._running = False
        self._consumer_task = None
        self._dlq_producer_started = False

    async def _process_astro_chart(self, user_id: str, birth_data: CalculationRequest, astro_chart: AstrologyChartResponse):
        logger.info(f"Processing ASTROLOGY chart for user_id: {user_id}")
        # 1. Create/Update Person Node
        person_node = await get_person_by_user_id(user_id)
        if not person_node:
            # Ensure datetime objects are handled correctly
            birth_dt_utc = datetime.strptime(f"{birth_data.birth_date} {birth_data.birth_time}", "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            person_payload = PersonNode(
                user_id=user_id,
                birth_datetime_utc=birth_dt_utc,
                birth_latitude=birth_data.latitude,
                birth_longitude=birth_data.longitude
            )
            try:
                person_node = await upsert_person(person_payload) # Use upsert_person
                logger.info(f"Upserted (created) PersonNode for user_id: {user_id}")
            except NodeCreationError as e: # Handles UniqueConstraintViolationError from create_person
                logger.warning(f"PersonNode creation failed (likely already exists) for {user_id}: {e}. Fetching again.")
                person_node = await get_person_by_user_id(user_id)
                if not person_node:
                    logger.error(f"Failed to create or find PersonNode for {user_id} after creation attempt.")
                    return # Cannot proceed without person node
        else: # Update existing person if birth data changed (idempotency)
             # Ensure datetime objects are handled correctly
            birth_dt_utc = datetime.strptime(f"{birth_data.birth_date} {birth_data.birth_time}", "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            update_payload = {
                "birth_datetime_utc": birth_dt_utc,
                "birth_latitude": birth_data.latitude,
                "birth_longitude": birth_data.longitude
            }
            # Only update if different to avoid unnecessary writes
            # Handle potential None values in existing person_node
            needs_update = False
            if person_node.birth_datetime_utc != update_payload["birth_datetime_utc"]:
                needs_update = True
            if person_node.birth_latitude != update_payload["birth_latitude"]:
                 needs_update = True
            if person_node.birth_longitude != update_payload["birth_longitude"]:
                 needs_update = True

            if needs_update:
                try:
                    person_node = await update_person_properties(user_id, update_payload) # Use update_person_properties
                    logger.info(f"Updated PersonNode properties for user_id: {user_id}")
                except DAOException as e:
                     logger.error(f"Failed to update PersonNode for {user_id}: {e}")
                     # Continue with old person_node data if update fails

        # 2. Create AstroFeature Nodes and link to Person
        for planet_pos in astro_chart.planetary_positions:
            feature_name = f"{planet_pos.name} in {planet_pos.sign}"
            # Use model_dump for details, ensuring it's JSON serializable if needed by Neo4j driver/OGM
            details_dict = planet_pos.model_dump(exclude_none=True)
            feature_node_payload = AstroFeatureNode(
                name=feature_name,
                feature_type="planet_in_sign",
                details=json.dumps(details_dict) # Store as JSON string
            )
            try:
                # Use get_astrofeature_by_name_and_type with both name and type
                feature_node = await get_astrofeature_by_name_and_type(feature_node_payload.name, feature_node_payload.feature_type)
                if not feature_node:
                    feature_node = await upsert_astrofeature(feature_node_payload) # Use upsert_astrofeature
                # Ensure relationship properties are handled
                rel_props = HasFeatureProperties(source_calculation_id="chart_calc_event") # Add event ID if available
                # link_person_to_astrofeature needs name and type
                await link_person_to_astrofeature(user_id, feature_node_payload.name, feature_node_payload.feature_type, rel_props)
            except (NodeCreationError, DAOException) as e:
                logger.error(f"Error processing AstroFeature '{feature_name}' for user {user_id}: {e}")

        for aspect in astro_chart.aspects:
            feature_name = f"{aspect.planet1} {aspect.aspect_type} {aspect.planet2}"
            details_dict = aspect.model_dump(exclude_none=True)
            feature_node_payload = AstroFeatureNode(
                name=feature_name,
                feature_type="aspect",
                details=json.dumps(details_dict) # Store as JSON string
            )
            try:
                # Use get_astrofeature_by_name_and_type with both name and type
                feature_node = await get_astrofeature_by_name_and_type(feature_node_payload.name, feature_node_payload.feature_type)
                if not feature_node:
                    feature_node = await upsert_astrofeature(feature_node_payload) # Use upsert_astrofeature
                rel_props = HasFeatureProperties(source_calculation_id="chart_calc_event")
                # link_person_to_astrofeature needs name and type
                await link_person_to_astrofeature(user_id, feature_node_payload.name, feature_node_payload.feature_type, rel_props)
            except (NodeCreationError, DAOException) as e:
                logger.error(f"Error processing AstroFeature Aspect '{feature_name}' for user {user_id}: {e}")
        # Add more AstroFeatures: house cusps, North Node etc.

    async def _process_hd_chart(self, user_id: str, birth_data: CalculationRequest, hd_chart: HumanDesignChartResponse):
        logger.info(f"Processing HUMAN DESIGN chart for user_id: {user_id}")
        # PersonNode should exist from _process_astro_chart or be created/fetched here similarly if HD can come first/alone.
        # For this example, assume PersonNode is handled by the astro part or a shared pre-step.

        hd_features_to_create = []
        # Type
        hd_features_to_create.append({"name": hd_chart.type, "feature_type": "hd_type", "details": json.dumps({"strategy": hd_chart.strategy, "authority": hd_chart.authority})})
        # Authority
        hd_features_to_create.append({"name": hd_chart.authority, "feature_type": "hd_authority", "details": json.dumps({})})
        # Profile
        hd_features_to_create.append({"name": hd_chart.profile, "feature_type": "hd_profile", "details": json.dumps({})})
        # Definition
        hd_features_to_create.append({"name": hd_chart.definition, "feature_type": "hd_definition", "details": json.dumps({})})
        # Centers
        for center in hd_chart.defined_centers:
            hd_features_to_create.append({"name": f"Defined {center.name}", "feature_type": "hd_center_defined", "details": json.dumps(center.model_dump(exclude_none=True))})
        for center_name in hd_chart.open_centers:
             hd_features_to_create.append({"name": f"Open {center_name}", "feature_type": "hd_center_open", "details": json.dumps({"name": center_name})})
        # Channels
        for channel in hd_chart.channels:
            hd_features_to_create.append({"name": channel.channel_number, "feature_type": "hd_channel", "details": json.dumps(channel.model_dump(exclude_none=True))})
        # Gates
        for gate_act in hd_chart.gates:
            side = "Conscious" if gate_act.is_conscious else "Unconscious"
            gate_name = f"Gate {gate_act.gate_number}.{gate_act.line_number} ({gate_act.planet} {side})"
            hd_features_to_create.append({
                "name": gate_name,
                "feature_type": "hd_gate_activation",
                "details": json.dumps(gate_act.model_dump(exclude_none=True))
            })
        
        # G Center Access
        if hd_chart.g_center_access_type and hd_chart.g_center_access_definition:
            hd_features_to_create.append({
                "name": hd_chart.g_center_access_type, # e.g., "Generator Defined"
                "feature_type": "hd_g_center_access",
                "details": json.dumps({
                    "type": hd_chart.g_center_access_type,
                    "definition": hd_chart.g_center_access_definition
                })
            })

        for feature_info in hd_features_to_create:
            # Ensure details is a valid JSON string before creating node
            if 'details' in feature_info:
                if not isinstance(feature_info['details'], str):
                    feature_info['details'] = json.dumps(feature_info['details'])
                # Ensure details is not an empty string, which causes JSONDecodeError for Pydantic's Json type
                if feature_info['details'] == "":
                    feature_info['details'] = None # Convert empty string to None
            else: # If 'details' key is missing, set it to None to satisfy Optional field
                feature_info['details'] = None

            feature_node_payload = HDFeatureNode(**feature_info)
            try:
                # Use get_hdfeature_by_name_and_type with both name and type
                feature_node = await get_hdfeature_by_name_and_type(feature_node_payload.name, feature_node_payload.feature_type)
                if not feature_node:
                    feature_node = await upsert_hdfeature(feature_node_payload) # Use upsert_hdfeature
                rel_props = HasFeatureProperties(source_calculation_id="chart_calc_event")
                # link_person_to_hdfeature needs name and type
                await link_person_to_hdfeature(user_id, feature_node_payload.name, feature_node_payload.feature_type, rel_props)
            except (NodeCreationError, DAOException) as e:
                logger.error(f"Error processing HDFeature '{feature_node_payload.name}' for user {user_id}: {e}")


    async def _send_to_dlq(self, key: Optional[str], value: Any, error_type: str, error_message: str, original_topic: str):
        """Sends a message to the Dead-Letter Queue."""
        dlq_message = {
            "original_key": key,
            "original_value": value, # Value might not be serializable if it's already broken
            "original_topic": original_topic,
            "error_type": error_type,
            "error_message": error_message,
            "dlq_timestamp": datetime.now(timezone.utc).isoformat()
        }
        try:
            logger.warning(f"Sending message for key {key} from topic {original_topic} to DLQ topic {kafka_settings.chart_consumer_dlq_topic} due to {error_type}")
            await self.dlq_producer.send_and_wait(kafka_settings.chart_consumer_dlq_topic, value=dlq_message, key=key) # Use original key for DLQ partitioning if desired
            logger.info(f"Successfully sent message for key {key} to DLQ topic {kafka_settings.chart_consumer_dlq_topic}.")
        except KafkaError as e: # Catch base KafkaError which includes producer errors
            logger.error(f"KafkaError sending message for key {key} to DLQ topic {kafka_settings.chart_consumer_dlq_topic}: {e}", exc_info=True)
            # If DLQ send fails, the message will be reprocessed from the original topic if offset isn't committed.
            # This is a critical failure point. Consider alternative alerting or handling.
            raise # Re-raise to signal failure to the caller in consume loop
        except Exception as e:
            logger.error(f"Unexpected error sending message for key {key} to DLQ: {e}", exc_info=True)
            raise # Re-raise

    async def process_message_with_retry(self, message_key: Optional[str], message_value: Dict[str, Any], topic: str, partition: int, offset: int):
        """
        Processes a single message with retry logic for transient errors.
        If persistent error, sends to DLQ.
        """
        try:
            # Define retry strategy for transient errors (e.g., Neo4j connection issues)
            # DAOException can be broad; Neo4jServiceUnavailable is more specific for retries.
            # Other DAOExceptions might be persistent (e.g., data integrity) and should go to DLQ.
            @retry(
                stop=stop_after_attempt(3), # Max 3 attempts
                wait=wait_exponential(multiplier=1, min=2, max=10), # Exponential backoff: 2s, 4s, 8s
                retry=retry_if_exception_type(Neo4jServiceUnavailable), # Only retry for Neo4j service unavailable
                reraise=True # Reraise the exception if retries are exhausted
            )
            async def _process_logic():
                logger.info(f"Attempting to process message for key {message_key}, attempt: {_process_logic.retry.statistics['attempt_number']}")
                await self.process_message(message_key, message_value)

            await _process_logic()
            logger.info(f"Successfully processed message for key {message_key} after {_process_logic.retry.statistics.get('attempt_number', 1)} attempts.")

        except ValidationError as e: # Pydantic validation error (persistent)
            logger.error(f"ValidationError processing message for key {message_key} from {topic} P:{partition} O:{offset}. Error: {e}", exc_info=True)
            await self._send_to_dlq(message_key, message_value, "validation_error", str(e), original_topic=topic)
        except (UniqueConstraintViolationError, NodeCreationError) as e: # Specific DAO errors that are likely persistent
            logger.error(f"Persistent DAO Error (UniqueConstraintViolationError or NodeCreationError) processing message for key {message_key} from {topic} P:{partition} O:{offset}. Error: {e}", exc_info=True)
            await self._send_to_dlq(message_key, message_value, "persistent_dao_error", str(e), original_topic=topic)
        except Neo4jServiceUnavailable as e: # Retries exhausted for Neo4j
            logger.error(f"Neo4jServiceUnavailable after retries for key {message_key} from {topic} P:{partition} O:{offset}. Error: {e}", exc_info=True)
            await self._send_to_dlq(message_key, message_value, "neo4j_unavailable_after_retries", str(e), original_topic=topic)
        except DAOException as e: # Other DAO errors that might be persistent or unhandled by specific catches
            logger.error(f"Unhandled DAOException processing message for key {message_key} from {topic} P:{partition} O:{offset}. Error: {e}", exc_info=True)
            await self._send_to_dlq(message_key, message_value, "unhandled_dao_exception", str(e), original_topic=topic)
        except ValueError as e: # Custom value errors from process_message (e.g. missing key/payload)
            logger.error(f"ValueError (likely malformed message) processing message for key {message_key} from {topic} P:{partition} O:{offset}. Error: {e}", exc_info=True)
            await self._send_to_dlq(message_key, message_value, "value_error_malformed_message", str(e), original_topic=topic)
        except Exception as e: # Catch-all for other unexpected errors from process_message
            logger.error(f"Unexpected error in process_message_with_retry for key {message_key} from {topic} P:{partition} O:{offset}: {e}", exc_info=True)
            await self._send_to_dlq(message_key, message_value, "unexpected_processing_error", str(e), original_topic=topic)
            # If _send_to_dlq raises, that will propagate to the main consume loop.


    async def process_message(self, message_key: Optional[str], message_value: Dict[str, Any]):
        """
        Core logic for processing a single CHART_CALCULATED event.
        This method should raise exceptions for errors to be handled by process_message_with_retry.
        """
        # Logger call moved to _process_logic in process_message_with_retry to include attempt number
        # logger.info(f"Processing CHART_CALCULATED event. Key: {message_key}, Value (keys): {list(message_value.keys())}")

        user_id = message_key
        if not user_id:
            # This is a malformed message, should be caught by process_message_with_retry and sent to DLQ.
            logger.error("CHART_CALCULATED event missing user_id in Kafka message key.")
            raise ValueError("CHART_CALCULATED event missing user_id in Kafka message key.")

        request_payload_dict = message_value.get("request_payload")
        if not request_payload_dict:
            logger.error(f"Missing 'request_payload' in CHART_CALCULATED event for user {user_id}.")
            raise ValueError(f"Missing 'request_payload' in CHART_CALCULATED event for user {user_id}.") # Persistent error

        # Pydantic validation will occur here. If it fails, ValidationError will be raised.
        birth_data = CalculationRequest(**request_payload_dict)
        result_summary = message_value.get("result_summary", {})

        # Process Astrology Chart if present
        astro_chart_data = result_summary.get("astrology_chart")
        if astro_chart_data:
            if isinstance(astro_chart_data.get('planetary_positions'), list):
                 astro_chart_data['planetary_positions'] = [dict(p) for p in astro_chart_data['planetary_positions']]
            if isinstance(astro_chart_data.get('house_cusps'), list):
                 astro_chart_data['house_cusps'] = [dict(h) for h in astro_chart_data['house_cusps']]
            if isinstance(astro_chart_data.get('aspects'), list):
                 astro_chart_data['aspects'] = [dict(a) for a in astro_chart_data['aspects']]
            if astro_chart_data.get('north_node') and isinstance(astro_chart_data['north_node'], dict):
                 astro_chart_data['north_node'] = dict(astro_chart_data['north_node'])

            astro_chart_obj = AstrologyChartResponse(**astro_chart_data) # Can raise ValidationError
            await self._process_astro_chart(user_id, birth_data, astro_chart_obj) # Can raise DAOException, Neo4jServiceUnavailable
        else:
            logger.info(f"No astrology_chart data in CHART_CALCULATED event for user {user_id}.")

        # Process Human Design Chart if present
        hd_chart_data = result_summary.get("human_design_chart")
        if hd_chart_data:
            if isinstance(hd_chart_data.get('conscious_sun_gate'), dict):
                 hd_chart_data['conscious_sun_gate'] = dict(hd_chart_data['conscious_sun_gate'])
            if isinstance(hd_chart_data.get('unconscious_sun_gate'), dict):
                 hd_chart_data['unconscious_sun_gate'] = dict(hd_chart_data['unconscious_sun_gate'])
            if isinstance(hd_chart_data.get('defined_centers'), list):
                 hd_chart_data['defined_centers'] = [dict(c) for c in hd_chart_data['defined_centers']]
            if isinstance(hd_chart_data.get('channels'), list):
                 hd_chart_data['channels'] = [dict(c) for c in hd_chart_data['channels']]
            if isinstance(hd_chart_data.get('gates'), list):
                 hd_chart_data['gates'] = [dict(g) for g in hd_chart_data['gates']]

            hd_chart_obj = HumanDesignChartResponse(**hd_chart_data) # Can raise ValidationError
            await self._process_hd_chart(user_id, birth_data, hd_chart_obj) # Can raise DAOException, Neo4jServiceUnavailable
        else:
            logger.info(f"No human_design_chart data in CHART_CALCULATED event for user {user_id}.")

        # Logger call moved to process_message_with_retry upon successful completion
        # logger.info(f"Successfully processed CHART_CALCULATED event for user_id: {user_id}")

    async def consume(self):
        logger.info(f"Starting ChartCalculatedConsumer for topic: {CHART_CALCULATED_TOPIC}")
        self._running = True
        try:
            await self.consumer.start()
            logger.info(f"AIOKafkaConsumer started for topic {CHART_CALCULATED_TOPIC}, group {self.group_id}")
            await self.dlq_producer.start()
            self._dlq_producer_started = True
            logger.info(f"AIOKafkaProducer for DLQ started.")

            async for message in self.consumer:
                if not self._running:
                    break
                logger.debug(f"Received raw message: Topic={message.topic}, Partition={message.partition}, Offset={message.offset}, Key={message.key}, Value={message.value[:100]}...") # Log snippet
                processed_successfully = False
                try:
                    key = message.key # Already deserialized by AIOKafkaConsumer
                    value = message.value # Already deserialized by AIOKafkaConsumer

                    # process_message_with_retry will handle internal errors, retries, and DLQ.
                    # If it returns normally, the message is considered handled (either processed or DLQ'd).
                    # If it raises an exception (e.g., DLQ send failed), then processed_successfully remains False.
                    await self.process_message_with_retry(key, value, message.topic, message.partition, message.offset)
                    processed_successfully = True

                except json.JSONDecodeError as e: # Should be caught by deserializer, but as a safeguard
                    logger.error(f"FATAL: JSONDecodeError directly in consume loop for {message.topic} P:{message.partition} O:{message.offset}. Value (raw): {message.value}. Error: {e}", exc_info=True)
                    # This error means the message couldn't even be passed to process_message_with_retry
                    # Attempt to send to DLQ, but value might be problematic if not bytes/str
                    # The consumer's value_deserializer already tried json.loads(v.decode('utf-8'))
                    # So, message.value here is likely the raw bytes that failed decoding.
                    raw_value_for_dlq = message.value
                    if isinstance(raw_value_for_dlq, bytes):
                        try:
                            raw_value_for_dlq = raw_value_for_dlq.decode('utf-8', errors='replace') # Try to decode for DLQ
                        except Exception:
                            pass # Keep as bytes if decode fails

                    try:
                        await self._send_to_dlq(message.key, raw_value_for_dlq, "json_decode_error_in_consume_loop", str(e), original_topic=message.topic)
                        processed_successfully = True # DLQ attempt made
                    except Exception as dlq_e:
                        logger.critical(f"Failed to send undecodable message to DLQ for key {message.key}, topic {message.topic}. DLQ Error: {dlq_e}", exc_info=True)
                        processed_successfully = False # DLQ failed, do not commit
                except Exception as e: # Catch-all for errors from process_message_with_retry (e.g. DLQ send failure) or other unexpected issues
                    logger.error(f"Outer exception processing message for key {message.key} on {message.topic} P:{message.partition} O:{message.offset}: {e}", exc_info=True)
                    # If process_message_with_retry raised (e.g. DLQ send failed), processed_successfully will be False.
                    # No need to call _send_to_dlq here again as process_message_with_retry should have tried.
                    processed_successfully = False # Ensure it's false so offset is not committed

                if processed_successfully:
                    await self.consumer.commit() # Manually commit offset
                    logger.debug(f"Committed offset {message.offset} for partition {message.partition} on topic {message.topic}")
                else:
                    logger.error(f"Message for key {message.key} on {message.topic} P:{message.partition} O:{message.offset} was not processed successfully and not sent to DLQ. Offset will not be committed.")

                if not self._running: # Check again in case stop was called during processing
                    break
        except KafkaError as e:
             logger.error(f"Kafka error during consumption: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"ChartCalculatedConsumer run error: {e}", exc_info=True)
        finally:
            logger.info(f"Stopping ChartCalculatedConsumer for topic: {CHART_CALCULATED_TOPIC}.")
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
            logger.info("Chart consumer task created.")
        else:
            logger.warning("Chart consumer task already running.")

    async def stop(self):
        logger.info("ChartCalculatedConsumer stop requested.")
        self._running = False
        if self.consumer:
            await self.consumer.stop() # Ensure consumer is stopped
        if self._dlq_producer_started and self.dlq_producer:
            await self.dlq_producer.stop()
            logger.info("DLQ KafkaProducer stopped during explicit stop call.")
            self._dlq_producer_started = False
        if self._consumer_task:
            try:
                # Wait briefly for the task to finish after setting _running to False
                await asyncio.wait_for(self._consumer_task, timeout=5.0)
                logger.info("Chart consumer task finished.")
            except asyncio.TimeoutError:
                logger.warning("Chart consumer task did not finish within timeout during stop.")
            except asyncio.CancelledError:
                 logger.info("Chart consumer task was cancelled.")
            self._consumer_task = None


# Example of how to run the consumer (e.g., in a main script or managed service)
async def main():
    # Setup basic logging for testing
    # Note: In main.py, logging is already configured. This is for standalone testing.
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    consumer = ChartCalculatedConsumer()
    consumer_task = None
    try:
        # Start consumer in background
        consumer_task = asyncio.create_task(consumer.consume())
        # Keep main thread alive (or do other work)
        while True:
            await asyncio.sleep(1)
            if consumer_task.done():
                 logger.info("Consumer task finished unexpectedly.")
                 break
    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user.")
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
                 logger.info("Consumer task cancelled.")
        # Ensure Neo4j driver is closed if KG service manages its lifecycle globally
        from ..core.db import Neo4jDatabase
        await Neo4jDatabase.close_async_driver()
        logger.info("Neo4j driver closed.")

if __name__ == "__main__":
    # This requires Kafka and Neo4j to be running.
    # And the CHART_CALCULATED topic to exist, with messages being produced to it.
    # Example: Run services/chart_calc/app/core/kafka_producer.py in another terminal
    # to send test messages after starting this consumer.
    asyncio.run(main())