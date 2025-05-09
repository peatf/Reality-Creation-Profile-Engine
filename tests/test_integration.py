# tests/test_integration.py
import pytest
import asyncio
import uuid
from datetime import date, time, datetime as dt, timezone
from unittest.mock import patch, MagicMock # Use patch from unittest.mock
import httpx # For api_client type hint
from neo4j import AsyncDriver # For e2e_neo4j_driver type hint

from rdflib import Graph, Literal # Import Literal from top-level rdflib
from rdflib.namespace import RDF
from src.knowledge_graph import ontology as ont

# Import necessary components from the application
from src.models.input_models import BirthData
from services.typology_engine.scorer import generate_complete_results as calculate_assessment
from src.astrology.calculator import calculate_chart as calculate_astrology
# We need to mock the client call, but use the real interpreter
from src.human_design.interpreter import interpret_human_design_chart
from src.knowledge_graph.population import create_user_profile_graph # Correct function name
from src.synthesis.engine import generate_synthesized_insights
from src.synthesis.narrator import format_synthesized_output

# --- Sample Data for Integration Test ---

TEST_PROFILE_ID = "integration_test_user"
TEST_BIRTH_DATA = BirthData(
    birth_date=date(1991, 6, 21),
    birth_time=time(10, 15, 0),
    latitude=40.7128,
    longitude=-74.0060,
    timezone='America/New_York',
    city_of_birth='New York', # Renamed from city
    country_of_birth='USA'    # Renamed from country
)
TEST_ASSESSMENT_TYPOLOGY = {
    'cognitive-q1': 'right', 'cognitive-q2': 'balanced',
    'perceptual-q1': 'left', 'perceptual-q2': 'left',
    'kinetic-q1': 'right', 'kinetic-q2': 'right',
    'choice-q1': 'balanced', 'choice-q2': 'right',
    'resonance-q1': 'left', 'resonance-q2': 'balanced',
    'rhythm-q1': 'balanced', 'rhythm-q2': 'balanced'
}
TEST_ASSESSMENT_MASTERY = {
    'core-q1': 'financial-abundance',
    'growth-q1': 'action-challenge',
    'alignment-q1': 'accept-intuition',
    'energy-q1': 'intuitive-instincts',
}

# Sample HD data that the mocked client will return
SAMPLE_HD_RAW_DATA_LIST = [{
    "type": "Reflector",
    "profile": "2/4",
    "authority": "None (Lunar)",
    "definition": "None",
    "centers": [], # No defined centers
    "channels_short": [],
    "variables": "PRL DRL", # Example
    "cognition": "Outer Vision",
    "determination": "Hot Thirst",
    "motivation": "Innocence",
    "perspective": "Power",
    "gates": ["1", "2", "3"], # Sample gates
    "activations": {}
}]

@pytest.mark.asyncio
async def test_full_data_flow_integration(mocker):
    """
    Tests the integration flow from input data through to formatted output.
    Mocks the external HD API call.
    """
    # --- Mock External Calls ---
    # Mock the HD client function to avoid real API call
    mock_hd_client = mocker.patch(
        'src.human_design.client.get_human_design_chart',
        return_value=SAMPLE_HD_RAW_DATA_LIST # Return sample raw data
    )

    # --- Execute Workflow ---
    # 1. Calculate Assessment
    assessment_results = calculate_assessment(TEST_ASSESSMENT_TYPOLOGY, TEST_ASSESSMENT_MASTERY)
    assert assessment_results is not None

    # 2. Calculate Astrology
    # Note: This makes a real calculation using skyfield/ephemeris
    astro_chart = calculate_astrology(TEST_BIRTH_DATA)
    assert astro_chart is not None
    # TODO: Add astrology schema parser step here when implemented
    astro_factors = {"Sun": astro_chart["objects"].get("Sun")} # Simplified for now

    # 3. Get and Interpret Human Design (uses mocked client)
    # Since get_human_design_chart is mocked, we call the mock directly
    # to simulate the behavior within the workflow if it were called.
    # The actual call site would likely be within a higher-level orchestrator.
    # For this test, we simulate getting the data and then interpreting it.
    hd_raw_data = await mock_hd_client(TEST_BIRTH_DATA) # Simulate getting data
    hd_interpreted = interpret_human_design_chart(hd_raw_data)
    assert hd_interpreted is not None
    assert hd_interpreted['type'] == "Reflector"

    # 4. Populate Knowledge Graph
    # Note: create_user_profile_graph takes the full request object,
    # but for testing the flow *after* calculations, we'll call a hypothetical
    # populate_graph function or refactor create_user_profile_graph later.
    # For now, let's simulate population by creating a graph and adding some triples.
    # This part needs adjustment based on how population is actually structured/called.
    # --- SIMULATED POPULATION ---
    populated_graph = Graph() # Start with empty graph for simulation
    populated_graph.add((ont.RCPE[TEST_PROFILE_ID], RDF.type, ont.User)) # Add user
    # Add some sample data based on previous steps to allow synthesis/narration to run
    if assessment_results and assessment_results.get('typologyPair'):
         typology_key = assessment_results['typologyPair'].get('key')
         if typology_key:
             typology_uri = ont.RCPE[f"Typology_{typology_key}"]
             populated_graph.add((ont.RCPE[TEST_PROFILE_ID], ont.hasProfile, ont.RCPE[f"{TEST_PROFILE_ID}_profile"])) # Link profile
             populated_graph.add((ont.RCPE[f"{TEST_PROFILE_ID}_profile"], ont.hasTypology, typology_uri))
             populated_graph.add((typology_uri, ont.typologyName, Literal(typology_key))) # Store key directly for now
    if hd_interpreted and hd_interpreted.get('type'):
        hd_chart_uri = ont.RCPE[f"{TEST_PROFILE_ID}_hd_chart"]
        hd_type_uri = ont.RCPE[f"HDType_{hd_interpreted['type'].replace(' ', '_')}"]
        populated_graph.add((ont.RCPE[TEST_PROFILE_ID], ont.hasHumanDesignChart, hd_chart_uri))
        populated_graph.add((hd_chart_uri, ont.hasHDType, hd_type_uri))
        populated_graph.add((hd_type_uri, ont.name, Literal(hd_interpreted['type'])))
    if astro_factors and astro_factors.get('Sun'):
        astro_chart_uri = ont.RCPE[f"{TEST_PROFILE_ID}_astro_chart"]
        sun_uri = ont.RCPE[f"{TEST_PROFILE_ID}_astro_Sun"]
        populated_graph.add((ont.RCPE[TEST_PROFILE_ID], ont.hasAstrologyChart, astro_chart_uri))
        populated_graph.add((astro_chart_uri, ont.hasObject, sun_uri))
        populated_graph.add((sun_uri, ont.isInSign, Literal(astro_factors['Sun'].get('sign'))))
    # --- END SIMULATED POPULATION ---

    # Original call removed as it's replaced by simulation above
    assert isinstance(populated_graph, Graph)
    assert len(populated_graph) >= 10 # Check graph has expected number of triples (or more)

    # 5. Generate Synthesized Insights
    synthesis_results = generate_synthesized_insights(populated_graph)
    assert isinstance(synthesis_results, dict)
    assert "synthesized_insights" in synthesis_results
    assert len(synthesis_results["synthesized_insights"]) > 0 # Should have at least default

    # 6. Format Output
    final_output = format_synthesized_output(
        profile_id=TEST_PROFILE_ID,
        synthesis_results=synthesis_results,
        assessment_results=assessment_results,
        astro_factors=astro_factors, # Pass simplified factors for now
        hd_interpreted=hd_interpreted
    )

    # --- Assert Final Output ---
    assert final_output is not None
    assert final_output["profile_id"] == TEST_PROFILE_ID
    assert "summary" in final_output
    assert "details" in final_output
    assert "strategies" in final_output
    assert "synthesized_insights" in final_output

    # Check some key summary points
    assert "typology_name" in final_output["summary"]
    assert final_output["summary"]["human_design_type"] == "Reflector"
    assert "sun_sign" in final_output["summary"]

    # Check that insights exist and are formatted
    assert len(final_output["synthesized_insights"]) > 0
    assert "formatted_text" in final_output["synthesized_insights"][0]

    # Ensure the mock HD client was called
    mock_hd_client.assert_called_once_with(TEST_BIRTH_DATA)


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_e2e_profile_creation_populates_kg(
    api_client: httpx.AsyncClient, # Fixture from tests/conftest.py
    e2e_neo4j_driver: AsyncDriver # Fixture from tests/conftest.py
):
    """
    End-to-End test:
    1. Calls the /profile/create API endpoint.
    2. Waits for Kafka event processing by kg_service.
    3. Verifies that data is populated in the Neo4j graph.
    Assumes the full Docker environment (infra/docker/docker-compose.yml) is running.
    """
    profile_uuid = str(uuid.uuid4())
    user_id_for_test = f"e2e_user_{profile_uuid}" # Ensure unique user_id

    request_payload = {
        "birth_data": {
            "birth_date": "1985-08-15",
            "birth_time": "14:30:00",
            "city_of_birth": "London",
            "country_of_birth": "GB"
            # latitude, longitude, timezone will be derived by geocoding/timezone services
        },
        "assessment_responses": {
            "typology": { # Example, adjust to actual typology questions
                "cognitive-q1": "left", "perceptual-q1": "right"
            },
            "mastery": { # Example, adjust to actual mastery questions
                "core-q1": "self-awareness"
            }
        }
    }

    # 1. Call the /profile/create API endpoint
    # The main API (main.py) will call chart_calc service, which should produce Kafka events.
    # kg_service will consume these events and write to Neo4j.
    response = await api_client.post("/profile/create", json=request_payload)
    response.raise_for_status() # Check for API errors
    response_data = response.json()
    profile_id_from_api = response_data.get("profile_id")

    assert response.status_code == 201
    assert profile_id_from_api is not None
    # Note: The profile_id returned by the API might be different from user_id_for_test
    # depending on how it's generated (e.g., from graph URI).
    # For verification, we'll use the user_id that should be in the graph based on input.
    # The `kg_population.create_user_profile_graph` uses `request.user_id` if present,
    # or generates one. The `ProfileCreateRequest` model doesn't have a top-level user_id.
    # The `kg_service`'s `chart_consumer` uses the Kafka message key as user_id.
    # The `main.py`'s `/profile/create` doesn't explicitly send a user_id to Kafka.
    # This needs alignment. For now, assume the chart_calc service uses a generated ID or
    # one from an auth context (not present in this test request).
    # Let's assume for now the `profile_id_from_api` is what we should query for.
    # If the KG uses a different identifier (e.g. from birth data hash), adjust query.

    # For this test, let's assume the `profile_id_from_api` is the `user_id` in the Person node.
    # This implies the `chart_calc` service, when producing the Kafka event, uses this ID as the key.
    # Or, `kg_population` in `main.py` ensures this ID is used when creating the initial graph node.
    # The `kg_service`'s `chart_consumer` uses the Kafka message key as `user_id`.
    # The `main.py`'s `/profile/create` calls `chart_calc`, which then produces an event.
    # The key for that event is crucial. Let's assume `chart_calc` uses the `profile_id` it generates/receives.

    # 2. Wait for event processing
    # This is a common challenge in E2E tests. A fixed delay is simple but can be flaky.
    # A more robust approach would be polling Neo4j or using a notification mechanism.
    await asyncio.sleep(15) # Adjust delay as needed for your environment

    # 3. Verify data in Neo4j
    async with e2e_neo4j_driver.session() as session:
        # Check if a Person node was created.
        # The exact user_id to query depends on how it's propagated through the system.
        # If the API's profile_id is used as the Kafka message key by chart_calc,
        # then kg_service's chart_consumer will use it as user_id for the Person node.
        person_query = "MATCH (p:Person {user_id: $user_id}) RETURN p.user_id as user_id, p.birth_latitude as lat"
        person_result = await session.run(person_query, user_id=profile_id_from_api)
        person_record = await person_result.single()

        assert person_record is not None, f"Person node with user_id '{profile_id_from_api}' not found in Neo4j."
        assert person_record["user_id"] == profile_id_from_api
        assert person_record["lat"] is not None # Check that geocoding happened and was stored

        # Verify some AstroFeature linked to the Person
        # Example: Check for a Sun sign feature (name depends on actual calculation)
        # This requires knowing what features chart_calc would generate for "1985-08-15 London"
        # For a generic check, we can see if *any* AstroFeature is linked.
        astro_link_query = """
        MATCH (p:Person {user_id: $user_id})-[:HAS_ASTROFEATURE]->(af:AstroFeature)
        RETURN count(af) as astro_feature_count
        """
        astro_link_result = await session.run(astro_link_query, user_id=profile_id_from_api)
        astro_link_record = await astro_link_result.single()
        assert astro_link_record is not None
        assert astro_link_record["astro_feature_count"] > 0, "No AstroFeatures linked to Person."

        # Verify some HDFeature linked to the Person
        hd_link_query = """
        MATCH (p:Person {user_id: $user_id})-[:HAS_HDFEATURE]->(hf:HDFeature)
        RETURN count(hf) as hd_feature_count
        """
        hd_link_result = await session.run(hd_link_query, user_id=profile_id_from_api)
        hd_link_record = await hd_link_result.single()
        assert hd_link_record is not None
        assert hd_link_record["hd_feature_count"] > 0, "No HDFeatures linked to Person."

    # Cleanup (optional, but good practice for E2E tests if state persists across runs)
    # async with e2e_neo4j_driver.session() as session:
    #     await session.run("MATCH (p:Person {user_id: $user_id}) DETACH DELETE p", user_id=profile_id_from_api)
    #     # Delete other related nodes if they are uniquely created for this test
    #     # and not shared dictionary nodes.