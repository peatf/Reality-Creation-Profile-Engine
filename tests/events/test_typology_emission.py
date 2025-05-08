import pytest
from fastapi.testclient import TestClient
from confluent_kafka import KafkaError
import logging
from datetime import datetime, timezone

# Assuming your FastAPI app instance is created in main.py or similar
# and can be imported. If not, this needs adjustment.
# For this example, let's assume `main.app` is your FastAPI application.
# If your app structure is different, you might need to adjust how `app` is imported/created.
# For instance, if your router is added to an app in main.py:
# from main import app # Or wherever your FastAPI app is initialized
# client = TestClient(app)
# For now, let's mock the client and necessary parts if app is not directly available.

# Mocking the main app and client for standalone test execution
# In a real project, TestClient would wrap your FastAPI app.
# We need to test the router's endpoint.
from src.routers import typology as typology_router
from fastapi import FastAPI, Depends
from src.schemas.typology import AssessmentRequest, AssessmentResult
from services.typology_engine.engine import TypologyEngine # For mocking

# Create a minimal app to test the router
app = FastAPI()

# Mock get_current_user and load_spec as they are dependencies in the router
def mock_get_current_user() -> str:
    return "test_user_123"

class MockSpec:
    version: str = "1.0.0" # Align with the version in src/routers/typology.py

def mock_load_spec() -> MockSpec:
    return MockSpec()

# Override dependencies for the test app
app.dependency_overrides[typology_router.get_current_user] = mock_get_current_user
app.dependency_overrides[typology_router.load_spec] = mock_load_spec

app.include_router(typology_router.router)
client = TestClient(app)


# Configuration constants that would be in your app
TOPIC_TYPOLOGY_ASSESSED = "TYPOLOGY_ASSESSED" # Re-define or import from config.kafka if possible

# Sample valid answers for requests
valid_answers = {"q1": "a", "q2": "b"}

# Mocked AssessmentResult structure for TypologyEngine
class MockTypologyEngineResult:
    def __init__(self, typology_name="TestTypology", confidence=0.95, trace=None, other_data=None):
        self.typology_name = typology_name
        self.confidence = confidence
        self.trace = trace or {"detail": "mock trace"} # Add trace
        self.other_data = other_data or {}

    def to_dict(self): # Assuming the engine's calculate_scores returns a dict
        return {
            "typology_name": self.typology_name,
            "confidence": self.confidence,
            "trace": self.trace, # Include trace
            **self.other_data
        }

@pytest.fixture
def mock_typology_engine(mocker):
    mock_engine_instance = mocker.MagicMock(spec=TypologyEngine)
    mock_engine_instance.calculate_scores.return_value = MockTypologyEngineResult(
        typology_name="SuperAnalyst", confidence=0.88, trace={"steps": "calculated"}
    ).to_dict() # Ensure it returns a dict as expected by the router
    
    mock_engine_class = mocker.patch("src.routers.typology.TypologyEngine", return_value=mock_engine_instance)
    # Also patch it where get_typology_engine might be called if it's not fully Depends-based
    mocker.patch("services.typology_engine.engine.TypologyEngine", return_value=mock_engine_instance)
    # Patch the dependency getter if it instantiates TypologyEngine directly
    mocker.patch("src.routers.typology.get_typology_engine", return_value=mock_engine_instance)
    return mock_engine_instance


def test_emit_typology_assessed_success(mocker, mock_typology_engine, caplog):
    caplog.set_level(logging.INFO)
    mock_avro_producer_instance = mocker.MagicMock()
    mock_get_producer = mocker.patch("src.routers.typology.get_typology_producer", return_value=mock_avro_producer_instance)

    # Simulate a valid result from the (mocked) engine
    # This is already configured in mock_typology_engine fixture
    engine_result_dict = mock_typology_engine.calculate_scores.return_value

    response = client.post("/typology/assess", json={"answers": valid_answers})

    assert response.status_code == 200
    mock_get_producer.assert_called_once()
    mock_avro_producer_instance.produce.assert_called_once()
    
    # Check call arguments for produce
    # call_args is a tuple: (args, kwargs) or call_args[0] for args, call_args[1] for kwargs
    # produce_args_kwargs = mock_avro_producer_instance.produce.call_args.kwargs # if args are named
    # produce_args_tuple = mock_avro_producer_instance.produce.call_args[0] # if args are positional
    
    # Assuming produce is called with topic and value as keyword arguments
    # produce(topic=TOPIC_TYPOLOGY_ASSESSED, value=event)
    called_kwargs = mock_avro_producer_instance.produce.call_args[1]
    sent_topic = called_kwargs["topic"]
    sent_value = called_kwargs["value"]

    assert sent_topic == TOPIC_TYPOLOGY_ASSESSED
    assert sent_value["user_id"] == "test_user_123" # From mock_get_current_user
    assert sent_value["typology_name"] == engine_result_dict["typology_name"]
    assert sent_value["confidence"] == engine_result_dict["confidence"]
    assert "timestamp" in sent_value and isinstance(sent_value["timestamp"], str)
    # Validate timestamp format (basic check for ISO format)
    try:
        datetime.fromisoformat(sent_value["timestamp"].replace("Z", "+00:00"))
    except ValueError:
        pytest.fail("Timestamp is not in valid ISO format")
    
    assert sent_value["spec_version"] == mock_load_spec().version
    mock_avro_producer_instance.flush.assert_called_once_with(timeout=5)
    assert f"Kafka event emitted for user test_user_123, typology {engine_result_dict['typology_name']}" in caplog.text


def test_emit_typology_assessed_failure_logs_error(mocker, mock_typology_engine, caplog):
    caplog.set_level(logging.ERROR) # Ensure ERROR level logs are captured
    
    mock_avro_producer_instance = mocker.MagicMock()
    
    # Define a specific KafkaError instance to be raised
    kafka_error_instance = KafkaError(KafkaError._MSG_TIMED_OUT, "Test Kafka Error: Broker down")

    # Use a helper function for side_effect to ensure the error is raised
    def raise_kafka_error(*args, **kwargs):
        raise kafka_error_instance

    mock_avro_producer_instance.produce.side_effect = raise_kafka_error
    # mock_avro_producer_instance.flush.side_effect = raise_kafka_error # If testing flush failure

    mocker.patch("src.routers.typology.get_typology_producer", return_value=mock_avro_producer_instance)

    response = client.post("/typology/assess", json={"answers": valid_answers})

    assert response.status_code == 200 # Should not fail the HTTP request
    mock_avro_producer_instance.produce.assert_called_once()
    # Flush might not be called if produce fails, or it might be called and also fail.
    # Depending on the exact logic in the router, adjust assertion for flush.
    # If produce raises, flush in the same try block might be skipped.
    # The current router code calls flush after produce. If produce fails, flush is called.
    # If flush is what fails, then produce would have been called.
    # Given `producer.produce(...)` then `producer.flush()`, if `produce` fails, `flush` is not reached in that `try`
    # The `except KafkaError` catches it.
    # The provided router code has produce and flush in the same try block for Kafka emission.
    # If produce() raises KafkaError, flush() won't be called.

    # Check logs for the new detailed message
    expected_log_message_part_code = "Code: _MSG_TIMED_OUT" # Corrected: KafkaError.name() includes underscore
    expected_log_message_part_text = "Message: Test Kafka Error: Broker down"
    expected_log_prefix = f"Kafka emission failed (caught as KafkaError). User: {mock_get_current_user()}."
    
    found_log = False
    for record in caplog.records:
        if record.levelname == "ERROR" and \
           expected_log_prefix in record.message and \
           expected_log_message_part_code in record.message and \
           expected_log_message_part_text in record.message:
            found_log = True
            break
    assert found_log, f"Expected Kafka failure log message not found. Logs: {caplog.text}"

    # Ensure no KafkaError was re-raised to break the request
    for record in caplog.records:
        if record.exc_info and isinstance(record.exc_info[1], KafkaError):
            pytest.fail("KafkaError was unexpectedly re-raised or propagated.")

def test_kafka_producer_not_available(mocker, mock_typology_engine, caplog):
    caplog.set_level(logging.ERROR)
    # Simulate get_typology_producer returning None
    mocker.patch("src.routers.typology.get_typology_producer", return_value=None)

    response = client.post("/typology/assess", json={"answers": valid_answers})

    assert response.status_code == 200
    assert "Kafka producer is not available. Event not emitted." in caplog.text