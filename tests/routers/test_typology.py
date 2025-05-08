import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unittest.mock import MagicMock

from src.routers.typology import router as typology_router, get_typology_engine
from src.schemas.typology import AssessmentResult # AssessmentRequest is used implicitly by client.post
from services.typology_engine.engine import TypologyEngine # For spec in MagicMock
from services.typology_engine.models import IncompleteAssessmentError, InvalidSubmissionError

# Create a FastAPI app instance and include the router for testing
app = FastAPI()
app.include_router(typology_router, prefix="/api/v1")

client = TestClient(app)

# Mock data
VALID_ANSWERS = {"q1": "a", "q2": "c", "q3": "e"}
VALID_RESULT_PAYLOAD = {
    "typology_name": "Innovator",
    "confidence": 0.85,
    "trace": {"score": 100, "details": "some trace data"}
}

# --- Test Cases ---

def test_assess_typology_valid_submission():
    """Test valid full submission → 200 OK"""
    mock_engine_instance = MagicMock(spec=TypologyEngine)
    mock_engine_instance.calculate_scores.return_value = VALID_RESULT_PAYLOAD

    app.dependency_overrides[get_typology_engine] = lambda: mock_engine_instance
    response = client.post("/api/v1/typology/assess", json={"answers": VALID_ANSWERS})
    app.dependency_overrides.clear()

    assert response.status_code == 200
    result = response.json()
    assert result["typology_name"] == VALID_RESULT_PAYLOAD["typology_name"]
    assert result["confidence"] == VALID_RESULT_PAYLOAD["confidence"]
    assert result["trace"] == VALID_RESULT_PAYLOAD["trace"]
    mock_engine_instance.calculate_scores.assert_called_once_with(VALID_ANSWERS)

def test_assess_typology_missing_answers():
    """Test missing answers → 422 Unprocessable Entity"""
    mock_engine_instance = MagicMock(spec=TypologyEngine)
    mock_engine_instance.calculate_scores.side_effect = IncompleteAssessmentError("Missing required answers: q3")

    app.dependency_overrides[get_typology_engine] = lambda: mock_engine_instance
    incomplete_answers = {"q1": "a", "q2": "c"} # Missing q3
    response = client.post("/api/v1/typology/assess", json={"answers": incomplete_answers})
    app.dependency_overrides.clear()

    assert response.status_code == 422
    assert "Missing required answers: q3" in response.json()["detail"]
    mock_engine_instance.calculate_scores.assert_called_once_with(incomplete_answers)

def test_assess_typology_invalid_answer_key():
    """Test invalid answer key → 400 Bad Request"""
    mock_engine_instance = MagicMock(spec=TypologyEngine)
    mock_engine_instance.calculate_scores.side_effect = InvalidSubmissionError("Invalid answer key 'x' for question 'q1'")

    app.dependency_overrides[get_typology_engine] = lambda: mock_engine_instance
    invalid_answers = {"q1": "x", "q2": "c", "q3": "e"} # Invalid answer 'x' for q1
    response = client.post("/api/v1/typology/assess", json={"answers": invalid_answers})
    app.dependency_overrides.clear()

    assert response.status_code == 400
    assert "Invalid answer key 'x' for question 'q1'" in response.json()["detail"]
    mock_engine_instance.calculate_scores.assert_called_once_with(invalid_answers)

def test_assess_typology_engine_unexpected_error():
    """Test unexpected error from TypologyEngine → 500 Internal Server Error"""
    mock_engine_instance = MagicMock(spec=TypologyEngine)
    mock_engine_instance.calculate_scores.side_effect = Exception("A critical engine failure occurred")

    app.dependency_overrides[get_typology_engine] = lambda: mock_engine_instance
    response = client.post("/api/v1/typology/assess", json={"answers": VALID_ANSWERS})
    app.dependency_overrides.clear()

    assert response.status_code == 500
    assert response.json()["detail"] == "Internal Server Error"
    mock_engine_instance.calculate_scores.assert_called_once_with(VALID_ANSWERS)

def test_assess_typology_profile_mismatch_as_generic_error():
    """
    Test Typology profile mismatch → 500 or default fallback.
    Assuming this is an unexpected error if not caught as Incomplete/Invalid.
    """
    mock_engine_instance = MagicMock(spec=TypologyEngine)
    # Simulate an error that isn't IncompleteAssessmentError or InvalidSubmissionError
    mock_engine_instance.calculate_scores.side_effect = ValueError("Profile mismatch configuration error")

    app.dependency_overrides[get_typology_engine] = lambda: mock_engine_instance
    response = client.post("/api/v1/typology/assess", json={"answers": VALID_ANSWERS})
    app.dependency_overrides.clear()

    assert response.status_code == 500 # Caught by the generic Exception handler
    assert response.json()["detail"] == "Internal Server Error"
    mock_engine_instance.calculate_scores.assert_called_once_with(VALID_ANSWERS)

def test_assess_typology_edge_case_ties_in_ranking():
    """Test edge case: ties in ranking → 200 OK with specific trace"""
    tied_result_payload = {
        "typology_name": "BalancedType",
        "confidence": 0.70,
        "trace": {
            "scores": {"TypeA": 0.7, "TypeB": 0.7},
            "ranking": ["TypeA", "TypeB"],
            "details": "Tie detected between TypeA and TypeB"
        }
    }
    mock_engine_instance = MagicMock(spec=TypologyEngine)
    mock_engine_instance.calculate_scores.return_value = tied_result_payload

    app.dependency_overrides[get_typology_engine] = lambda: mock_engine_instance
    response = client.post("/api/v1/typology/assess", json={"answers": VALID_ANSWERS})
    app.dependency_overrides.clear()

    assert response.status_code == 200
    result = response.json()
    assert result["typology_name"] == tied_result_payload["typology_name"]
    assert result["confidence"] == tied_result_payload["confidence"]
    assert "Tie detected" in result["trace"]["details"]
    mock_engine_instance.calculate_scores.assert_called_once_with(VALID_ANSWERS)

# It's also important to ensure the router is added to the main application.
# The router file has a comment:
# # In main.py, you would add:
# # from src.routers import typology
# # app.include_router(typology.router, prefix="/api/v1", tags=["typology"])
# This step is outside the scope of this file but crucial for the app to work.