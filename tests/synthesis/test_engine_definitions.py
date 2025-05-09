# tests/synthesis/test_engine_definitions.py

import pytest
# import os # No longer needed for ENGINE_DEF_PATH
from src.human_design.interpreter import transform_knowledge_base, interpret_human_design_chart, AUTHORITY_MAPPING
# HD_KNOWLEDGE_BASE import removed from here, will be accessed via fixture or reloaded interpreter module
from src.constants import KnowledgeBaseKeys
# import json # No longer needed for loading enginedef.json
 
# ENGINE_DEF_PATH is no longer needed
 
@pytest.fixture(scope="module")
def loaded_engine_definitions():
    """
    Fixture to provide a freshly loaded and transformed Human Design knowledge base for tests.
    """
    print("Fixture 'loaded_engine_definitions': Calling transform_knowledge_base() directly.")
    kb = transform_knowledge_base()
    # Perform a very basic check that kb is a dictionary and seems populated
    if not isinstance(kb, dict) or not kb: # Check if it's a non-empty dict
        pytest.fail(f"transform_knowledge_base() did not return a valid, non-empty dictionary. Type: {type(kb)}")
    if not kb.get(KnowledgeBaseKeys.TYPES.value): # Specifically check for types, as it's fundamental
         pytest.fail(f"Knowledge base from transform_knowledge_base() is missing '{KnowledgeBaseKeys.TYPES.value}'. KB keys: {list(kb.keys())}")
    print(f"Fixture 'loaded_engine_definitions': transform_knowledge_base() returned KB with keys: {list(kb.keys())}")
    return kb


def test_hd_knowledge_base_centers_structure(loaded_engine_definitions):
    """
    Tests that the HD_KNOWLEDGE_BASE has the correct nested structure for centers
    after processing enginedef.json (i.e., "Defined" and "Undefined" states).
    """
    centers_data = loaded_engine_definitions.get(KnowledgeBaseKeys.CENTERS.value)
    assert centers_data is not None, "Centers data is missing from HD_KNOWLEDGE_BASE"
    assert isinstance(centers_data, dict), "Centers data should be a dictionary"

    # List of expected center keys (without " Center" suffix)
    expected_center_keys = ["Head", "Ajna", "Throat", "G", "Heart", "Solar Plexus", "Sacral", "Spleen", "Root"]

    for center_key in expected_center_keys:
        assert center_key in centers_data, f"Center key '{center_key}' is missing from centers_data"
        
        center_entry = centers_data[center_key]
        assert isinstance(center_entry, dict), \
            f"Entry for center '{center_key}' should be a dictionary, got {type(center_entry)}"
        
        assert "Defined" in center_entry, \
            f"'Defined' state missing for center '{center_key}'"
        assert isinstance(center_entry["Defined"], dict), \
            f"'Defined' state for center '{center_key}' should be a dictionary"
        assert "definition" in center_entry["Defined"], \
            f"'definition' missing in 'Defined' state for center '{center_key}'"
        assert "role_in_manifestation" in center_entry["Defined"], \
            f"'role_in_manifestation' missing in 'Defined' state for center '{center_key}'"

        assert "Undefined" in center_entry, \
            f"'Undefined' state missing for center '{center_key}'"
        assert isinstance(center_entry["Undefined"], dict), \
            f"'Undefined' state for center '{center_key}' should be a dictionary"
        assert "definition" in center_entry["Undefined"], \
            f"'definition' missing in 'Undefined' state for center '{center_key}'"
        assert "role_in_manifestation" in center_entry["Undefined"], \
            f"'role_in_manifestation' missing in 'Undefined' state for center '{center_key}'"

def test_specific_center_content(loaded_engine_definitions):
    """
    Tests the content of a specific center (e.g., Head Center) for both states.
    This confirms that the data from enginedef.json is correctly loaded.
    """
    head_center_data = loaded_engine_definitions.get(KnowledgeBaseKeys.CENTERS.value, {}).get("Head")
    assert head_center_data is not None, "Head Center data not found"

    # Check Defined state for Head Center
    defined_head = head_center_data.get("Defined")
    assert defined_head is not None, "Defined state for Head Center not found"
    # Updated expected definition to match centers.json content for the first "Head Center" entry
    assert defined_head.get("definition") == "A fixed point of inspiration and mental pressure, generating consistent questions and ideas."
    assert isinstance(defined_head.get("role_in_manifestation"), str), "Role in manifestation for Defined Head should be a string"
    # Updated expected role to match centers.json content for the first "Head Center" entry
    assert defined_head.get("role_in_manifestation") == "You emit a stable field of inquiry and abstract ideation. Reality reflects the nature of what you're consistently contemplating. Let thoughts arise from stillness rather than search. Your questions broadcast—they do not need validation. Ideas may manifest long after inception. Trust that the field holds what you've seeded. Let your steady curiosity shape the questions reality answers."
 
    # Check Undefined state for Head Center
    undefined_head = head_center_data.get("Undefined")
    assert undefined_head is not None, "Undefined state for Head Center not found"
    # Updated expected definition to match centers.json content for the second "Head Center" entry
    assert undefined_head.get("definition") == "An open portal for mental pressure, amplifying collective questions and inspirations."
    assert isinstance(undefined_head.get("role_in_manifestation"), str), "Role in manifestation for Undefined Head should be a string"
    # Updated expected role to match centers.json content for the second "Head Center" entry
    assert undefined_head.get("role_in_manifestation") == "You reflect and amplify the thoughts of others. Your field is impressionable to collective mental focus. Clarity comes from detaching from the need to answer every question. Let thoughtforms pass through. Not all inspirations are yours to act on. The delay reveals which ideas are aligned. Master discernment. Let thought move through you like wind, not fire."

# You can add more tests for other centers or other parts of HD_KNOWLEDGE_BASE if needed.
# For example, testing that other categories like Gates, Channels, Types are still populated.

def test_other_hd_categories_populated(loaded_engine_definitions):
    """Checks if other HD categories like types, gates, channels are still populated."""
    assert loaded_engine_definitions.get(KnowledgeBaseKeys.TYPES.value), "Types data is missing"
    assert len(loaded_engine_definitions.get(KnowledgeBaseKeys.TYPES.value, {})) > 0, "Types data is empty"
    
    assert loaded_engine_definitions.get(KnowledgeBaseKeys.GATES.value), "Gates data is missing"
    assert len(loaded_engine_definitions.get(KnowledgeBaseKeys.GATES.value, {})) > 0, "Gates data is empty"

    assert loaded_engine_definitions.get(KnowledgeBaseKeys.CHANNELS.value), "Channels data is missing"
    assert len(loaded_engine_definitions.get(KnowledgeBaseKeys.CHANNELS.value, {})) > 0, "Channels data is empty"

    assert loaded_engine_definitions.get(KnowledgeBaseKeys.PROFILES.value), "Profiles data is missing"
    assert len(loaded_engine_definitions.get(KnowledgeBaseKeys.PROFILES.value, {})) > 0, "Profiles data is empty"
    
    assert loaded_engine_definitions.get(KnowledgeBaseKeys.AUTHORITIES.value), "Authorities data is missing"
    assert len(loaded_engine_definitions.get(KnowledgeBaseKeys.AUTHORITIES.value, {})) > 0, "Authorities data is empty"


def test_g_center_access_details_populated(loaded_engine_definitions):
    """
    Tests that the G_CENTER_ACCESS_DETAILS are correctly populated in HD_KNOWLEDGE_BASE.
    """
    g_access_data = loaded_engine_definitions.get(KnowledgeBaseKeys.G_CENTER_ACCESS_DETAILS.value)
    assert g_access_data is not None, "G Center Access Details data is missing from HD_KNOWLEDGE_BASE"
    assert isinstance(g_access_data, dict), "G Center Access Details data should be a dictionary"
    # g_center_access.json contains 9 entries.
    assert len(g_access_data) == 9, f"Expected 9 G Center Access detail entries, found {len(g_access_data)}"
 
    # Un-comment and adapt checks for specific IDs based on g_center_access.json
    test_id_defined = "g_center_access_generator_defined"
    assert test_id_defined in g_access_data, f"ID '{test_id_defined}' missing from G Center Access Details"
    
    item_data_defined = g_access_data[test_id_defined]
    assert isinstance(item_data_defined, dict), f"Data for '{test_id_defined}' should be a dictionary"
    assert "subtype" in item_data_defined, f"'subtype' missing for '{test_id_defined}'"
    assert item_data_defined["subtype"] == "Generator Defined", f"Incorrect subtype for '{test_id_defined}'"
    assert "definition" in item_data_defined, f"'definition' missing for '{test_id_defined}'"
    assert "role_in_manifestation" in item_data_defined, f"'role_in_manifestation' missing for '{test_id_defined}'"
    assert item_data_defined.get("weighted_importance") == "contextual", f"Incorrect weighted_importance for '{test_id_defined}'"
    
    test_id_undefined = "g_center_access_projector_undefined"
    assert test_id_undefined in g_access_data, f"ID '{test_id_undefined}' missing from G Center Access Details"
    item_data_undefined = g_access_data[test_id_undefined]
    assert isinstance(item_data_undefined, dict), f"Data for '{test_id_undefined}' should be a dictionary"
    assert item_data_undefined.get("subtype") == "Projector Undefined", f"Incorrect subtype for '{test_id_undefined}'"
    
    # Check another one for good measure
    test_id_reflector = "g_center_access_reflector_undefined"
    assert test_id_reflector in g_access_data, f"ID '{test_id_reflector}' missing from G Center Access Details"
    item_data_reflector = g_access_data[test_id_reflector]
    assert item_data_reflector.get("subtype") == "Reflector Undefined", f"Incorrect subtype for '{test_id_reflector}'"


def test_new_hd_categories_populated(loaded_engine_definitions):
    """
    Tests that the new HD categories (Strategies, specific Motivations, specific Perspectives)
    are populated correctly in the HD_KNOWLEDGE_BASE.
    """
    # Test Types (ensure all new ones are present)
    types_data = loaded_engine_definitions.get(KnowledgeBaseKeys.TYPES.value)
    assert types_data, "Types data is missing"
    expected_types = ["Manifestor", "Generator", "Manifesting Generator", "Projector", "Reflector"]
    for hd_type in expected_types:
        assert hd_type in types_data, f"Type '{hd_type}' missing from HD_KNOWLEDGE_BASE"
        assert "definition" in types_data[hd_type], f"Definition missing for Type '{hd_type}'"

    # Test Strategies
    strategies_data = loaded_engine_definitions.get(KnowledgeBaseKeys.STRATEGIES.value)
    assert strategies_data, "Strategies data is missing"
    expected_strategies = ["Informing", "Responding", "Waiting for the Invitation", "Waiting a Lunar Cycle"]
    for strategy in expected_strategies:
        assert strategy in strategies_data, f"Strategy '{strategy}' missing from HD_KNOWLEDGE_BASE"
        assert "definition" in strategies_data[strategy], f"Definition missing for Strategy '{strategy}'"

    # Test Authorities (ensure all mapped keys are present)
    authorities_data = loaded_engine_definitions.get(KnowledgeBaseKeys.AUTHORITIES.value)
    assert authorities_data, "Authorities data is missing"
    # AUTHORITY_MAPPING maps JSON term_name to internal keys
    for mapped_auth_key in AUTHORITY_MAPPING.values():
        assert mapped_auth_key in authorities_data, f"Authority key '{mapped_auth_key}' missing from HD_KNOWLEDGE_BASE"
        assert "definition" in authorities_data[mapped_auth_key], f"Definition missing for Authority '{mapped_auth_key}'"
    
    # Test Variables - Motivation
    motivation_data = loaded_engine_definitions.get(KnowledgeBaseKeys.VARIABLES.value, {}).get(KnowledgeBaseKeys.MOTIVATION.value)
    assert motivation_data, "Motivation data is missing under Variables"
    expected_motivations = [
        "Fear – Communalist (Left / Strategic)", "Hope – Theist (Left / Strategic)", "Desire – Leader (Left / Strategic)",
        "Need – Master (Left / Strategic)", "Guilt – Conditioner (Left / Strategic)", "Innocence – Observer (Left / Strategic)"
    ]
    for mot_term in expected_motivations:
        assert mot_term in motivation_data, f"Motivation term '{mot_term}' missing"
        assert "definition" in motivation_data[mot_term], f"Definition missing for Motivation '{mot_term}'"
        assert len(motivation_data) >= 12, "Expected at least 12 motivation entries (6 pairs)"


    # Test Variables - Perspective
    perspective_data = loaded_engine_definitions.get(KnowledgeBaseKeys.VARIABLES.value, {}).get(KnowledgeBaseKeys.PERSPECTIVE.value)
    assert perspective_data, "Perspective data is missing under Variables"
    expected_perspectives = [
        "Survival (Left – Focused)", "Possibility (Right – Peripheral)", "Perspective (Left – Focused)",
        "Understanding (Right – Peripheral)", "Evaluation (Left – Focused)", "Judgment (Right – Peripheral)"
    ]
    for persp_term in expected_perspectives:
        assert persp_term in perspective_data, f"Perspective term '{persp_term}' missing"
        assert "definition" in perspective_data[persp_term], f"Definition missing for Perspective '{persp_term}'"
        assert len(perspective_data) >= 12, "Expected at least 12 perspective entries (6 pairs)"


def test_interpret_human_design_chart_with_new_fields(loaded_engine_definitions):
    """
    Tests that interpret_human_design_chart correctly processes and includes
    definitions for strategy, motivation, and perspective from the knowledge base.
    """
    sample_chart_data = {
        "type": "Manifestor",
        "strategy": "Informing", # New field
        "authority": "Emotional", # Uses mapped key
        "profile": "1/3",
        "definition": "Single Definition",
        "centers": ["Throat", "Heart", "Solar Plexus"], # Defined centers
        "channels_short": ["21-45"],
        "gates": ["21", "45"],
        "variables": "PLL DLL", # P(L) M(L) | D(L) E(L) - Example
        "cognition": "Inner Vision", # Example, not directly tested for definition here
        "determination": "Cold Thirst", # Example, not directly tested for definition here
        "motivation": "Fear – Communalist (Left / Strategic)", # New field - full term name
        "perspective": "Survival (Left – Focused)", # New field - full term name
        "environment_type": "Caves" # Example
    }

    # Ensure HD_KNOWLEDGE_BASE used by interpreter is the one from our fixture for consistency
    # This is implicitly handled as interpret_human_design_chart uses the globally loaded HD_KNOWLEDGE_BASE
    # which should be populated by the transform_knowledge_base call in the fixture.

    interpreted_results = interpret_human_design_chart(sample_chart_data)
    assert interpreted_results is not None, "Interpretation failed"
    
    insights = interpreted_results.get("insights", {})
    assert insights, "Insights section is missing"

    # Check Strategy
    strategy_info = insights.get("strategy_info", {})
    assert strategy_info, "Strategy info missing from insights"
    expected_strategy_def = loaded_engine_definitions.get(KnowledgeBaseKeys.STRATEGIES.value, {}).get("Informing", {}).get("definition")
    assert strategy_info.get("definition") == expected_strategy_def, "Incorrect strategy definition"

    # Check Motivation
    variable_info = insights.get("variable_info", {})
    assert variable_info, "Variable info missing from insights"
    motivation_insight = variable_info.get("motivation", {})
    assert motivation_insight, "Motivation insight missing from variable_info"
    expected_motivation_def = loaded_engine_definitions.get(KnowledgeBaseKeys.VARIABLES.value, {}).get(KnowledgeBaseKeys.MOTIVATION.value, {}).get("Fear – Communalist (Left / Strategic)", {}).get("definition")
    assert motivation_insight.get("definition") == expected_motivation_def, "Incorrect motivation definition"

    # Check Perspective
    perspective_insight = variable_info.get("perspective", {})
    assert perspective_insight, "Perspective insight missing from variable_info"
    expected_perspective_def = loaded_engine_definitions.get(KnowledgeBaseKeys.VARIABLES.value, {}).get(KnowledgeBaseKeys.PERSPECTIVE.value, {}).get("Survival (Left – Focused)", {}).get("definition")
    assert perspective_insight.get("definition") == expected_perspective_def, "Incorrect perspective definition"

    # Check Authority (already somewhat covered, but good to confirm in this context)
    authority_info = insights.get("authority_info", {})
    assert authority_info, "Authority info missing"
    # 'Emotional' is the mapped key for 'Emotional Authority'
    expected_auth_def = loaded_engine_definitions.get(KnowledgeBaseKeys.AUTHORITIES.value, {}).get("Emotional", {}).get("definition")
    assert authority_info.get("definition") == expected_auth_def, "Incorrect authority definition"