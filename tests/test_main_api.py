# tests/test_main_api.py
import pytest
import json
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock, AsyncMock # Import AsyncMock
from rdflib import Graph, URIRef # Import URIRef

# Import the FastAPI app instance from main
from main import app

# Import models and sample data
from src.models.input_models import ProfileCreateRequest, BirthData, AssessmentResponses
from datetime import date, time

# --- Test Client Setup ---
client = TestClient(app)

# --- Sample Data for API Tests ---
TEST_PROFILE_ID = "api_test_user_1"
TEST_BIRTH_DATA_DICT = {
    "birth_date": "1992-11-25",
    "birth_time": "09:15:00",
    "latitude": 51.5074, # London
    "longitude": -0.1278, # London
    "timezone": "Europe/London",
    "city": "London",
    "country": "UK"
}
TEST_ASSESSMENT_TYPOLOGY_DICT = {
    'cognitive-q1': 'left', 'cognitive-q2': 'left',
    'perceptual-q1': 'balanced', 'perceptual-q2': 'balanced',
    'kinetic-q1': 'right', 'kinetic-q2': 'right',
    'choice-q1': 'left', 'choice-q2': 'balanced',
    'resonance-q1': 'right', 'resonance-q2': 'right',
    'rhythm-q1': 'balanced', 'rhythm-q2': 'left'
}
TEST_ASSESSMENT_MASTERY_DICT = {
    'core-q1': 'personal-autonomy',
    'growth-q1': 'clarity-challenge',
}
TEST_REQUEST_DATA = {
    "birth_data": TEST_BIRTH_DATA_DICT,
    "assessment_responses": {
        "typology": TEST_ASSESSMENT_TYPOLOGY_DICT,
        "mastery": TEST_ASSESSMENT_MASTERY_DICT
    }
}

# Sample data returned by mocked services
MOCK_ASSESSMENT_RESULTS = {"typologyPair": {"key": "mock-typology"}}
MOCK_ASTRO_CHART = {"objects": {"Sun": {"sign": "Sagittarius"}}}
MOCK_ASTRO_FACTORS = {"Sun": {"sign": "Sagittarius"}}
MOCK_HD_RAW_DATA = [{"type": "Generator"}]
MOCK_HD_INTERPRETED = {"type": "Generator"}
MOCK_GRAPH = Graph()
# Add a dummy triple so the graph evaluates to True
# Need RDF and URIRef imported for this line
from rdflib.namespace import RDF # Ensure RDF is imported if not already
MOCK_GRAPH.add((URIRef("http://example.com/subject"), RDF.type, URIRef("http://example.com/Type")))
MOCK_SYNTHESIS_RESULTS = {"synthesized_insights": [{"id": "SYN_MOCK", "text": "Mock insight"}]}
MOCK_FORMATTED_OUTPUT = {"profile_id": TEST_PROFILE_ID, "summary": {"typology_name": "Mock Typology"}}

# --- API Tests ---

def test_read_root():
    """Test the health check endpoint."""
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "message": "Reality Creation Profile Engine is running."}

@patch('main.assessment_scorer.generate_complete_results', return_value=MOCK_ASSESSMENT_RESULTS)
@patch('main.astro_calc.calculate_chart', return_value=MOCK_ASTRO_CHART)
@patch('main.astro_parser.get_relevant_factors', return_value=MOCK_ASTRO_FACTORS)
@patch('main.hd_client.get_human_design_chart', new_callable=AsyncMock, return_value=MOCK_HD_RAW_DATA) # Mock async function
@patch('main.hd_interpreter.interpret_human_design_chart', return_value=MOCK_HD_INTERPRETED)
@patch('main.kg_population.create_user_profile_graph', new_callable=AsyncMock, return_value=MOCK_GRAPH) # Mock async function
@patch('main.synthesis_engine.find_user_uri', return_value=f"http://example.com/rcpe#{TEST_PROFILE_ID}") # Correct target module
@patch('main.user_profile_data', {}) # Ensure clean storage for test
def test_create_profile_success(mock_find_uri, mock_create_graph, mock_interpret_hd, mock_get_hd, mock_astro_factors, mock_astro_calc, mock_assess_score):
    """Test successful profile creation via POST /profile/create."""
    response = client.post("/profile/create", json=TEST_REQUEST_DATA)

    assert response.status_code == 201
    assert "profile_id" in response.json()
    profile_id = response.json()["profile_id"]
    assert profile_id == TEST_PROFILE_ID
    assert response.json()["status"] == "created"

    # Check mocks were called
    mock_assess_score.assert_called_once()
    mock_astro_calc.assert_called_once()
    mock_get_hd.assert_called_once()
    mock_interpret_hd.assert_called_once_with(MOCK_HD_RAW_DATA)
    mock_create_graph.assert_called_once()
    mock_find_uri.assert_called_once()

    # Check data was stored (using the mock patch)
    from main import user_profile_data # Re-import to access patched version
    assert profile_id in user_profile_data
    assert user_profile_data[profile_id]["graph"] == MOCK_GRAPH
    assert user_profile_data[profile_id]["assessment_results"] == MOCK_ASSESSMENT_RESULTS
    assert user_profile_data[profile_id]["hd_interpreted"] == MOCK_HD_INTERPRETED

@patch('main.kg_population.create_user_profile_graph', new_callable=AsyncMock, return_value=None) # Simulate graph creation failure
@patch('main.assessment_scorer.generate_complete_results', return_value=MOCK_ASSESSMENT_RESULTS)
@patch('main.astro_calc.calculate_chart', return_value=MOCK_ASTRO_CHART)
@patch('main.hd_client.get_human_design_chart', new_callable=AsyncMock, return_value=MOCK_HD_RAW_DATA)
@patch('main.hd_interpreter.interpret_human_design_chart', return_value=MOCK_HD_INTERPRETED)
def test_create_profile_graph_failure(mock_interpret_hd, mock_get_hd, mock_astro_calc, mock_assess_score, mock_create_graph):
    """Test POST /profile/create when graph population fails."""
    response = client.post("/profile/create", json=TEST_REQUEST_DATA)
    assert response.status_code == 500
    assert "Failed to create profile graph" in response.json()["detail"]

@patch('main.assessment_scorer.generate_complete_results', side_effect=Exception("Assessment Error")) # Simulate calculation error
def test_create_profile_calculation_error(mock_assess_score):
    """Test POST /profile/create when an internal calculation fails."""
    response = client.post("/profile/create", json=TEST_REQUEST_DATA)
    assert response.status_code == 500
    assert "Internal server error" in response.json()["detail"]
    assert "Assessment Error" in response.json()["detail"] # Check error message propagation

def test_get_profile_not_found():
    """Test GET /profile/{profile_id} for a non-existent ID."""
    response = client.get(f"/profile/non_existent_id")
    assert response.status_code == 404
    assert "not found" in response.json()["detail"]

# Use mocker.patch.dict for GET tests to ensure data is seen by the endpoint
@patch('main.synthesis_engine.generate_synthesized_insights', return_value=MOCK_SYNTHESIS_RESULTS)
@patch('main.synthesis_narrator.format_synthesized_output', return_value=MOCK_FORMATTED_OUTPUT)
def test_get_profile_success(mock_format, mock_synthesize, mocker): # Add mocker fixture
    """Test successful profile retrieval via GET /profile/{profile_id}."""
    # Patch the dictionary within the test function's scope
    mocker.patch.dict('main.user_profile_data', {
        TEST_PROFILE_ID: {
            "graph": MOCK_GRAPH,
            "assessment_results": MOCK_ASSESSMENT_RESULTS,
            "astro_factors": MOCK_ASTRO_FACTORS,
            "hd_interpreted": MOCK_HD_INTERPRETED
        }
    }, clear=True) # clear=True ensures we start with only this entry

    response = client.get(f"/profile/{TEST_PROFILE_ID}")

    assert response.status_code == 200
    assert response.json() == MOCK_FORMATTED_OUTPUT

    # Check mocks were called
    # mock_storage_get.assert_called_once_with(TEST_PROFILE_ID) # Remove this check
    mock_synthesize.assert_called_once_with(MOCK_GRAPH)
    mock_format.assert_called_once_with(
        profile_id=TEST_PROFILE_ID,
        synthesis_results=MOCK_SYNTHESIS_RESULTS,
        assessment_results=MOCK_ASSESSMENT_RESULTS,
        astro_factors=MOCK_ASTRO_FACTORS,
        hd_interpreted=MOCK_HD_INTERPRETED
    )

@patch('main.synthesis_engine.generate_synthesized_insights') # We don't need to mock return value here
def test_get_profile_missing_graph_data(mock_synthesize, mocker): # Add mocker, remove unused mock_format
    """Test GET /profile/{profile_id} when graph data is missing internally."""
    mocker.patch.dict('main.user_profile_data', {
        TEST_PROFILE_ID: { # Simulate missing graph key
            "assessment_results": MOCK_ASSESSMENT_RESULTS,
            "astro_factors": MOCK_ASTRO_FACTORS,
            "hd_interpreted": MOCK_HD_INTERPRETED
        }
    }, clear=True)
    response = client.get(f"/profile/{TEST_PROFILE_ID}")
    assert response.status_code == 500
    assert "Profile graph data missing" in response.json()["detail"]
    mock_synthesize.assert_not_called() # Synthesis shouldn't be called if graph is missing


@patch('main.synthesis_engine.generate_synthesized_insights', return_value={"error": "Synthesis failed"}) # Simulate synthesis error
def test_get_profile_synthesis_error(mock_synthesize, mocker): # Add mocker
    """Test GET /profile/{profile_id} when synthesis engine returns an error."""
    mocker.patch.dict('main.user_profile_data', {
        TEST_PROFILE_ID: {"graph": MOCK_GRAPH} # Provide graph data
    }, clear=True)
    response = client.get(f"/profile/{TEST_PROFILE_ID}")
    assert response.status_code == 500
    assert "Error generating insights" in response.json()["detail"]
    assert "Synthesis failed" in response.json()["detail"]
    mock_synthesize.assert_called_once_with(MOCK_GRAPH)


@patch('main.synthesis_engine.generate_synthesized_insights', side_effect=Exception("Unexpected synthesis crash")) # Simulate synthesis crash
def test_get_profile_synthesis_exception(mock_synthesize, mocker): # Add mocker
    """Test GET /profile/{profile_id} when synthesis engine raises an exception."""
    mocker.patch.dict('main.user_profile_data', {
        TEST_PROFILE_ID: {"graph": MOCK_GRAPH} # Provide graph data
    }, clear=True)
    response = client.get(f"/profile/{TEST_PROFILE_ID}")
    assert response.status_code == 500
    assert "Internal server error" in response.json()["detail"]
    assert "Unexpected synthesis crash" in response.json()["detail"]
    mock_synthesize.assert_called_once_with(MOCK_GRAPH)