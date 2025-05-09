import pytest
import asyncio
from httpx import AsyncClient
from fastapi import status
from unittest.mock import AsyncMock, patch # For mocking kafka

# Assuming conftest.py is in the same directory or accessible in PYTHONPATH
from .fixtures.sample_chart_payloads import payload_standard
# from app.core.kafka_producer import CHART_CALCULATED_TOPIC # If running pytest from services/chart_calc
import json # For creating cache key
from ..app.core.cache import invalidate_cache_key # Use relative import
from ..app.schemas.request_schemas import CalculationRequest # Use relative import

# If running pytest from project root:
# from services.chart_calc.app.core.kafka_producer import CHART_CALCULATED_TOPIC
# For simplicity, we can redefine it here or ensure the import path is robust.
CHART_CALCULATED_TOPIC = "CHART_CALCULATED" # Defined in kafka_producer.py

@pytest.mark.asyncio
async def test_astrology_calculation_emits_kafka_event(test_client: AsyncClient, mocker):
    """
    Test that a successful astrology calculation emits a Kafka event.
    Mocks the send_kafka_message function.
    """
    payload = payload_standard.copy() # Use a standard valid payload

    # Mock the send_kafka_message function in app.main where it's called
    # or directly in app.core.kafka_producer if preferred.
    # Patching where it's looked up: services.chart_calc.app.main.send_kafka_message
    mocked_send_kafka = mocker.patch(
        "services.chart_calc.app.main.send_kafka_message", # Corrected path
        new_callable=AsyncMock # Use AsyncMock for async functions
    )
    # If send_kafka_message is directly from app.core.kafka_producer in main.py, then:
    # mocked_send_kafka = mocker.patch("app.core.kafka_producer.send_kafka_message", new_callable=AsyncMock)


    # Ensure cache miss for this test
    request_obj = CalculationRequest(**payload)
    request_dict = request_obj.model_dump()
    cache_key = f"astrology:{json.dumps(request_dict, sort_keys=True)}"
    await invalidate_cache_key(cache_key)

    # Make the request
    response = await test_client.post("/calculate/astrology", json=payload)
    assert response.status_code == status.HTTP_200_OK
    
    # Allow asyncio tasks (like the one created for send_kafka_message) to run
    await asyncio.sleep(0.01) # Small delay to ensure the task gets scheduled and executed

    # Assert that send_kafka_message was called
    mocked_send_kafka.assert_called_once()
    
    # Get the arguments it was called with
    args, kwargs = mocked_send_kafka.call_args
    
    # Args: (topic, message_payload)
    # Kwargs: {key: ...}
    
    assert args[0] == CHART_CALCULATED_TOPIC # topic
    message_payload = args[1]
    
    assert message_payload["calculation_type"] == "astrology"
    assert message_payload["request_payload"]["birth_date"] == payload["birth_date"]
    assert "result_summary" in message_payload
    assert "timestamp" in message_payload
    assert "service_version" in message_payload # Comes from app.version
    
    # Check key if it's passed (e.g., birth_date)
    assert kwargs.get("key") == payload["birth_date"]

    mocker.stopall()

@pytest.mark.asyncio
async def test_human_design_calculation_emits_kafka_event(test_client: AsyncClient, mocker):
    """
    Test that a successful Human Design calculation emits a Kafka event.
    """
    payload = payload_standard.copy()

    mocked_send_kafka = mocker.patch(
        "services.chart_calc.app.main.send_kafka_message", # Corrected path
        new_callable=AsyncMock
    )

    # Ensure cache miss for this test
    request_obj = CalculationRequest(**payload)
    request_dict = request_obj.model_dump()
    cache_key = f"humandesign:{json.dumps(request_dict, sort_keys=True)}"
    await invalidate_cache_key(cache_key)
    
    response = await test_client.post("/calculate/human_design", json=payload)
    assert response.status_code == status.HTTP_200_OK

    await asyncio.sleep(0.01)

    mocked_send_kafka.assert_called_once()
    args, kwargs = mocked_send_kafka.call_args
    
    assert args[0] == CHART_CALCULATED_TOPIC
    message_payload = args[1]
    
    assert message_payload["calculation_type"] == "human_design"
    assert message_payload["request_payload"]["birth_date"] == payload["birth_date"]
    assert "result_summary" in message_payload
    assert "type" in message_payload["result_summary"] # Check specific HD summary fields
    assert "profile" in message_payload["result_summary"]
    assert "timestamp" in message_payload
    assert "service_version" in message_payload

    assert kwargs.get("key") == payload["birth_date"]
    
    mocker.stopall()

@pytest.mark.asyncio
async def test_kafka_emission_failure_does_not_fail_request(test_client: AsyncClient, mocker):
    """
    Test that if Kafka emission fails, the main request still succeeds.
    (Assuming Kafka is non-critical for the response to the user).
    """
    payload = payload_standard.copy()
    payload["birth_date"] = "2000-01-01" # Ensure unique payload for cache miss

    # Mock send_kafka_message to raise an exception
    mocked_send_kafka = mocker.patch(
        "services.chart_calc.app.main.send_kafka_message", # Corrected path
        new_callable=AsyncMock,
        side_effect=Exception("Kafka is down!") # Simulate Kafka error
    )
    # Also mock print in app.core.kafka_producer to suppress error output during test
    mocker.patch("app.core.kafka_producer.print")


    # Ensure cache miss for this test
    request_obj = CalculationRequest(**payload)
    request_dict = request_obj.model_dump()
    cache_key = f"astrology:{json.dumps(request_dict, sort_keys=True)}"
    await invalidate_cache_key(cache_key)

    response = await test_client.post("/calculate/astrology", json=payload)
    
    # Request should still succeed even if Kafka emission fails in the background task
    assert response.status_code == status.HTTP_200_OK
    
    await asyncio.sleep(0.01) # Allow task to run

    mocked_send_kafka.assert_called_once() # Ensure it was attempted

    # Further checks: Logged error (if logging is implemented and mockable)
    # For now, we just check the endpoint's success.
    mocker.stopall()

# Note: These tests mock the `send_kafka_message` function.
# To test the `AIOKafkaProducer` itself, you'd typically need:
# 1. A running Kafka instance (e.g., via testcontainers).
# 2. A Kafka consumer to verify messages arrive on the topic.
# This is more of an integration test for the Kafka interaction part.
# The current tests focus on whether the application *attempts* to send the correct message.