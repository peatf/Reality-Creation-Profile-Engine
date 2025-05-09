import asyncio
import json
import pytest
import uuid
from datetime import datetime, timezone

from aiokafka import AIOKafkaProducer
from neo4j import AsyncDriver

from services.kg.app.consumers.chart_consumer import ChartCalculatedConsumer, CHART_CALCULATED_TOPIC
from services.kg.app.core.config import KafkaSettings, Neo4jSettings
from services.kg.app.core.db import Neo4jDatabase

# Assuming these schemas are importable for constructing test data
# If not, define minimal dict structures for the test
try:
    from services.chart_calc.app.schemas.request_schemas import CalculationRequest
    from services.chart_calc.app.schemas.astrology_schemas import AstrologyChartResponse, PlanetPosition, Aspect, HouseCusp, NorthNodeData
    from services.chart_calc.app.schemas.human_design_schemas import HumanDesignChartResponse, HDGateActivation, HDCenter, HDChannel, VariableDetail
except ImportError:
    # Define minimal fallbacks if actual schemas are not easily importable for tests
    # This is not ideal but can unblock test writing.
    # For a real scenario, ensure Python path allows these imports.
    CalculationRequest = dict
    AstrologyChartResponse = dict
    PlanetPosition = dict
    Aspect = dict
    HouseCusp = dict
    NorthNodeData = dict
    HumanDesignChartResponse = dict
    HDGateActivation = dict
    HDCenter = dict
    HDChannel = dict
    VariableDetail = dict


@pytest.mark.integration
async def test_process_chart_calculated_event(
    integration_kafka_producer: AIOKafkaProducer,
    integration_neo4j_driver: AsyncDriver,
    integration_kafka_settings: KafkaSettings, # Fixture from conftest
    neo4j_container, # Fixture from conftest to get connection details
    monkeypatch
):
    """
    Integration test for the ChartCalculatedConsumer:
    1. Produces a CHART_CALCULATED message to Kafka.
    2. Runs the consumer to process the message.
    3. Verifies that the expected data is written to Neo4j.
    """
    # 0. Prepare Neo4j settings for the consumer
    test_neo4j_settings = Neo4jSettings(
        uri=f"bolt://{neo4j_container.get_container_host_ip()}:{neo4j_container.get_exposed_port(7687)}",
        user="neo4j", # Default user for testcontainers Neo4j
        password="neo4j"  # Default password
    )
    monkeypatch.setattr('services.kg.app.core.config.neo4j_settings', test_neo4j_settings)
    monkeypatch.setattr('services.kg.app.crud.base_dao.neo4j_settings', test_neo4j_settings) # Ensure DAOs also get it
    # Reset driver instance in Neo4jDatabase to force reinitialization with new settings
    Neo4jDatabase._driver = None

    # Prepare Kafka settings for the consumer
    # The consumer uses kafka_settings from its own module's import
    monkeypatch.setattr('services.kg.app.consumers.chart_consumer.kafka_settings', integration_kafka_settings)


    # 1. Prepare sample data
    user_id = f"test_user_{uuid.uuid4()}"
    chart_id = f"chart_{uuid.uuid4()}"
    profile_id = user_id # Assuming profile_id is the same as user_id for this event

    birth_date_str = "1990-05-15"
    birth_time_str = "10:30:00"

    # Ensure datetime objects are timezone-aware if your models expect them
    birth_datetime_obj = datetime.strptime(f"{birth_date_str} {birth_time_str}", "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)


    # Sample CalculationRequest (as dict for message)
    request_payload_data = {
        "birth_date": birth_date_str,
        "birth_time": birth_time_str,
        "latitude": 34.0522,
        "longitude": -118.2437,
        "utc_offset": -7.0,
        "city": "Los Angeles", # Optional, add if schema requires
        "country_code": "US" # Optional
    }

    # Sample AstrologyChartResponse (as dict for message)
    astrology_chart_data = {
        "chart_type": "Natal",
        "planetary_positions": [
            {"name": "Sun", "sign": "Taurus", "degrees": 24.5, "house": "10", "is_retrograde": False, "full_degree": 54.5, "speed": 0.98},
            {"name": "Moon", "sign": "Scorpio", "degrees": 10.2, "house": "4", "is_retrograde": False, "full_degree": 220.2, "speed": 13.2}
        ],
        "house_cusps": [
            {"house": "1", "sign": "Leo", "degrees": 15.0},
            {"house": "2", "sign": "Virgo", "degrees": 12.0}
        ],
        "aspects": [
            {"planet1": "Sun", "aspect_type": "Opposition", "planet2": "Moon", "orb": 5.7, "aspect_degrees": 180.0}
        ],
        "ascendant_sign": "Leo",
        "midheaven_sign": "Taurus",
        "north_node": {"name": "North Node", "sign": "Capricorn", "degrees": 5.0, "house": "6", "is_retrograde": True, "full_degree": 275.0, "speed": -0.05}
    }

    # Sample HumanDesignChartResponse (as dict for message)
    human_design_chart_data = {
        "type": "Generator",
        "strategy": "To Wait to Respond",
        "authority": "Sacral",
        "profile": "5/1 Heretic/Investigator",
        "definition": "Single Definition",
        "incarnation_cross": "Right Angle Cross of The Sphinx (13/7 | 1/2)",
        "conscious_sun_gate": {"gate_number": "1", "line_number": "1", "description": "The Creative", "is_conscious": True, "planet": "Sun"},
        "unconscious_sun_gate": {"gate_number": "2", "line_number": "2", "description": "The Receptive", "is_conscious": False, "planet": "Earth"},
        "defined_centers": [{"name": "Sacral", "description": "Life force energy."}],
        "open_centers": ["Head", "Ajna"],
        "channels": [{"channel_number": "34-20", "name": "Charisma", "description": "The Channel of Power."}],
        "gates": [
            {"gate_number": "34", "line_number": "5", "is_conscious": True, "planet": "Sun", "description": "Power"},
            {"gate_number": "20", "line_number": "3", "is_conscious": False, "planet": "Earth", "description": "The Now"}
        ],
        "variables": { # Assuming this is a dict of VariableDetail or similar structure
            "determination": {"name": "Consecutive Appetite", "color": 1, "tone": 1, "base": 1, "orientation": "Left"},
            "cognition": {"name": "Smell", "color": 1, "tone": 2, "base": 1, "orientation": "Left"}
        },
        "g_center_access_type": "Generator Defined",
        "g_center_access_definition": "Access to identity through response."
    }

    message_value = {
        "event_type": "CALCULATED", # Matches Avro enum
        "chart_id": chart_id,
        "profile_id": profile_id,
        "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000), # Millis
        "payload": json.dumps({ # The payload string itself contains JSON
            "request_payload": request_payload_data,
            "result_summary": {
                "astrology_chart": astrology_chart_data,
                "human_design_chart": human_design_chart_data
            }
        })
    }
    
    # The consumer's value_deserializer is json.loads(v.decode('utf-8')).
    # It expects the *entire message value* to be a JSON string that, when parsed,
    # has keys like 'request_payload' and 'result_summary' directly.
    # This means the Avro 'payload' field's content needs to be at the top level
    # of the object that gets JSON serialized for the Kafka message value.

    # Corrected message_value structure based on consumer's direct access:
    # The consumer's process_message accesses message_value.get("request_payload"), etc.
    # This implies the ChartEvent's payload content is effectively the top-level message_value for the consumer.
    # So, the Kafka message value should be the JSON string of the *inner* payload.
    # The Avro envelope (event_type, chart_id, etc.) is metadata, and the consumer's
    # `process_message` seems to expect the *content* of the Avro `payload` field.

    # Let's re-evaluate: The Avro schema is for the event. The consumer deserializes the *entire event*.
    # Then `process_message` gets this deserialized event.
    # `message_value.get("request_payload")` would fail if `message_value` is the Avro `ChartEvent` structure.
    # It must be that the `payload` string from Avro is *further parsed* before `process_message`.
    # The consumer code: `value = message.value` (already deserialized by aiokafka's json.loads)
    # Then `process_message(key, value)`.
    # `request_payload_dict = message_value.get("request_payload")`
    # This is the critical part. If `message_value` is the ChartEvent, then it should be:
    # `inner_payload_str = message_value.get("payload")`
    # `inner_payload_dict = json.loads(inner_payload_str)`
    # `request_payload_dict = inner_payload_dict.get("request_payload")`

    # Given the current consumer code, it seems it expects the *flattened* structure.
    # This means the producer might be doing this flattening.
    # For this test, we will construct the message value as the consumer *expects it in process_message*.
    # This implies the Kafka message value (after initial JSON deserialization by aiokafka) is:
    final_kafka_message_value = {
        "request_payload": request_payload_data,
        "result_summary": {
            "astrology_chart": astrology_chart_data,
            "human_design_chart": human_design_chart_data
        }
        # Other fields from ChartEvent like event_type, chart_id, profile_id, timestamp
        # might also be present here if the producer flattens the Avro event.
        # For the consumer's process_message, only request_payload and result_summary are strictly used from value.
    }


    # 2. Produce message to Kafka
    await integration_kafka_producer.send_and_wait(
        CHART_CALCULATED_TOPIC, # Topic defined in consumer
        key=user_id.encode('utf-8'),
        value=json.dumps(final_kafka_message_value).encode('utf-8')
    )
    await asyncio.sleep(1) # Brief pause for producer

    # 3. Run the consumer
    consumer = ChartCalculatedConsumer(group_id=f"test_kg_consumer_group_{uuid.uuid4()}")
    
    consumer_task = asyncio.create_task(consumer.consume())
    await asyncio.sleep(8) # Allow time for consumption and processing (adjust as needed)

    # 4. Stop the consumer
    await consumer.stop()
    try:
        await asyncio.wait_for(consumer_task, timeout=5.0)
    except asyncio.TimeoutError:
        consumer_task.cancel() # Force cancel if it didn't stop gracefully
        await asyncio.sleep(0.1) # Allow cancellation to process


    # 5. Verify data in Neo4j
    async with integration_neo4j_driver.session() as session:
        # Verify Person node
        person_result = await session.run(
            "MATCH (p:Person {user_id: $user_id}) RETURN p.user_id as user_id, p.birth_datetime_utc as birth_datetime_utc",
            user_id=user_id
        )
        person_record = await person_result.single()
        assert person_record is not None
        assert person_record["user_id"] == user_id
        # Convert Neo4j datetime to comparable Python datetime
        db_birth_datetime = person_record["birth_datetime_utc"].to_native()
        assert db_birth_datetime.year == birth_datetime_obj.year
        assert db_birth_datetime.month == birth_datetime_obj.month
        assert db_birth_datetime.day == birth_datetime_obj.day
        assert db_birth_datetime.hour == birth_datetime_obj.hour
        assert db_birth_datetime.minute == birth_datetime_obj.minute


        # Verify AstroFeature node (e.g., Sun in Taurus)
        sun_feature_name = "Sun in Taurus"
        astro_feature_result = await session.run(
            "MATCH (p:Person {user_id: $user_id})-[:HAS_ASTROFEATURE]->(af:AstroFeature {name: $name}) RETURN af.name as name, af.feature_type as type",
            user_id=user_id, name=sun_feature_name
        )
        astro_feature_record = await astro_feature_result.single()
        assert astro_feature_record is not None
        assert astro_feature_record["name"] == sun_feature_name
        assert astro_feature_record["type"] == "planet_in_sign"

        # Verify HDFeature node (e.g., Generator type)
        hd_type_name = "Generator"
        hd_feature_result = await session.run(
            "MATCH (p:Person {user_id: $user_id})-[:HAS_HDFEATURE]->(hf:HDFeature {name: $name}) RETURN hf.name as name, hf.feature_type as type",
            user_id=user_id, name=hd_type_name
        )
        hd_feature_record = await hd_feature_result.single()
        assert hd_feature_record is not None
        assert hd_feature_record["name"] == hd_type_name
        assert hd_feature_record["type"] == "hd_type"
        
        # Verify a specific HD channel
        hd_channel_name = "34-20"
        hd_channel_result = await session.run(
            "MATCH (p:Person {user_id: $user_id})-[:HAS_HDFEATURE]->(hf:HDFeature {name: $name, feature_type: 'hd_channel'}) RETURN hf.name as name",
            user_id=user_id, name=hd_channel_name
        )
        hd_channel_record = await hd_channel_result.single()
        assert hd_channel_record is not None
        assert hd_channel_record["name"] == hd_channel_name

    # 6. Cleanup (optional, as testcontainers provide fresh DBs per session)
    # For function-scoped tests that might share a session DB, cleanup is good.
    async with integration_neo4j_driver.session() as session:
        await session.run("MATCH (p:Person {user_id: $user_id}) DETACH DELETE p", user_id=user_id)
        # Could also delete related features if they are uniquely tied to this test user
        # and not meant to be shared dictionary nodes.
        # For this example, features like "Sun in Taurus" are dictionary nodes.

    # Ensure the driver is closed by the fixture, not here.