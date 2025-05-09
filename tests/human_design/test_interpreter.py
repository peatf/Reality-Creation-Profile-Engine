# tests/human_design/test_interpreter.py
import pytest
import json # Keep for sample data if needed, but not for mocking file content
from unittest.mock import patch # mock_open is no longer needed for these tests
# We still import the function, and HD_KNOWLEDGE_BASE for direct patching.
from src.human_design.interpreter import interpret_human_design_chart, HD_KNOWLEDGE_BASE
from src.constants import KnowledgeBaseKeys # Import for structuring mock KB

# --- Mock Data for HD_KNOWLEDGE_BASE ---
# This data should reflect the structure after transform_knowledge_base()
# has processed individual JSON files.

# Sample data for a successful interpretation test
MOCK_KB_SUCCESS_CASE = {
    KnowledgeBaseKeys.TYPES.value: {
        "Generator": {"definition": "A being designed to respond to life.", "strategy_term": "To Wait to Respond"}
    },
    KnowledgeBaseKeys.STRATEGIES.value: { # Assuming strategies are loaded
        "To Wait to Respond": {"definition": "Wait for something in your outer reality to respond to."}
    },
    KnowledgeBaseKeys.AUTHORITIES.value: {
        "Sacral": {"definition": "Decision comes from gut response.", "role_in_manifestation": "Follow the 'uh-huh' or 'uhn-uh'."}
    },
    KnowledgeBaseKeys.PROFILES.value: {
        "2/4": {"name": "Hermit/Opportunist", "definition": "A profile of natural talent needing interaction."}
    },
    KnowledgeBaseKeys.CENTERS.value: {
        "Sacral": {"Defined": {"theme": "Life Force, Work", "definition": "The engine of vitality."}, "Undefined": {"definition": "Open to others' life force."}},
        "Solar Plexus": {"Defined": {"theme": "Emotional Wave"}, "Undefined": {"theme": "Open to others' emotions"}},
        "Throat": {"Defined": {"theme": "Manifestation, Communication"}, "Undefined": {"theme": "Open to others' expression"}}
    },
    KnowledgeBaseKeys.GATES.value: { # Simplified
        "34": {"name": "Power", "definition": "Gate of Power"},
        "20": {"name": "The Now", "definition": "Gate of the Now"},
        "59": {"name": "Sexuality", "definition": "Gate of Sexuality"},
        "6": {"name": "Friction", "definition": "Gate of Friction"}
    },
    KnowledgeBaseKeys.CHANNELS.value: {
        "34-20": {"name": "Charisma", "definition": "Busy, busy, busy."},
        "59-6": {"name": "Mating", "definition": "Channel of Mating"}
    },
    KnowledgeBaseKeys.VARIABLES.value: {
        KnowledgeBaseKeys.DETERMINATION.value: {
            "Cold Thirst": {"definition": "Prefers cooler foods/drinks.", "keywords": "cool, thirst"}
        },
        KnowledgeBaseKeys.COGNITION.value: {
            "Inner Vision": {"definition": "Processes information internally.", "keywords": "internal, visual"}
        },
        KnowledgeBaseKeys.MOTIVATION.value: {
            "Fear": {"definition": "Driven by the need for security.", "keywords": "security, foundation"}
        },
        KnowledgeBaseKeys.PERSPECTIVE.value: {
            "Power": {"definition": "Sees the world through power dynamics.", "keywords": "control, influence"}
        },
        KnowledgeBaseKeys.ENVIRONMENT.value: {} # Assuming empty or loaded from environment.json
    },
    KnowledgeBaseKeys.MANIFESTATION_MECHANICS.value: {}, # Assuming empty or loaded
    KnowledgeBaseKeys.G_CENTER_ACCESS_DETAILS.value: {} # Assuming empty or loaded
}

# Sample data for a test with missing fields in API response
MOCK_KB_MISSING_FIELDS_CASE = {
    KnowledgeBaseKeys.TYPES.value: {
        "Manifestor": {"definition": "A being designed to initiate.", "strategy_term": "To Inform before acting"}
    },
    KnowledgeBaseKeys.STRATEGIES.value: {
        "To Inform before acting": {"definition": "Inform those impacted before taking action."}
    },
    KnowledgeBaseKeys.AUTHORITIES.value: {
        "Splenic": {"definition": "Decision comes from intuition.", "role_in_manifestation": "Trust the initial hit."}
    },
    KnowledgeBaseKeys.PROFILES.value: {
        "4/1": {"name": "Opportunist/Investigator", "definition": "A profile needing a strong foundation."}
    },
    KnowledgeBaseKeys.CENTERS.value: {
        "Spleen": {"Defined": {"theme": "Intuition, Health"}, "Undefined": {}},
        "Throat": {"Defined": {"theme": "Communication"}, "Undefined": {}},
        "Heart": {"Defined": {"theme": "Ego, Willpower"}, "Undefined": {}}
    },
    KnowledgeBaseKeys.GATES.value: {},
    KnowledgeBaseKeys.CHANNELS.value: {
        "21-45": {"name": "Money Line", "definition": "Channel of Materialism"}
    },
    KnowledgeBaseKeys.VARIABLES.value: { # Empty as not provided in sample missing data
        KnowledgeBaseKeys.DETERMINATION.value: {}, KnowledgeBaseKeys.COGNITION.value: {},
        KnowledgeBaseKeys.MOTIVATION.value: {}, KnowledgeBaseKeys.PERSPECTIVE.value: {},
        KnowledgeBaseKeys.ENVIRONMENT.value: {}
    },
    KnowledgeBaseKeys.MANIFESTATION_MECHANICS.value: {},
    KnowledgeBaseKeys.G_CENTER_ACCESS_DETAILS.value: {}
}


# --- Sample API Data ---

# Sample valid API response structure (list containing one dict)
SAMPLE_CHART_DATA_LIST = [{
    "type": "Generator",
    "profile": "2/4",
    "channels_short": ["34-20", "59-6"],
    "centers": ["Sacral", "Solar Plexus", "Throat"], # Example defined centers
    "strategy": "To Wait to Respond", # Often included, though interpreter uses KB
    "authority": "Sacral",
    "definition": "Single Definition",
    "variables": "PLL DLL", # Pers(L) Motiv(L) | Determ(L) Env(L)
    "cognition": "Inner Vision", # Example
    "determination": "Cold Thirst", # Example
    "motivation": "Fear", # Example
    "perspective": "Power", # Example
    "gates": ["34", "20", "59", "6"], # Example
    "activations": {"design": {}, "personality": {}} # Placeholder
    # Other fields might exist but aren't directly used by interpreter logic yet
}]

# Sample data with missing optional fields
SAMPLE_CHART_MISSING_FIELDS = [{
    "type": "Manifestor",
    "profile": "4/1",
    "authority": "Splenic",
    "centers": ["Spleen", "Throat", "Heart"], # Example
    "channels_short": ["21-45"], # Example
    # Missing definition, variables, gates, etc.
}]

# --- Test Cases ---

# Remove patches for open/json.load as KB is loaded at module level
def test_interpret_human_design_chart_success(mocker): # Add mocker fixture
    """Test successful interpretation using mocked knowledge base."""
    # Mock the HD_KNOWLEDGE_BASE dictionary directly for this test
    mocker.patch.dict('src.human_design.interpreter.HD_KNOWLEDGE_BASE', MOCK_KB_SUCCESS_CASE, clear=True)

    # Call the function
    result = interpret_human_design_chart(SAMPLE_CHART_DATA_LIST[0]) # Pass the dict directly

    # Remove assertion for json.load call

    assert result is not None
    assert isinstance(result, dict)

    # Check basic extracted fields
    assert result['type'] == "Generator"
    assert result['profile'] == "2/4"
    assert result['authority'] == "Sacral"
    assert result['definition'] == "Single Definition"
    assert result['defined_centers'] == ["Sacral", "Solar Plexus", "Throat"]
    assert result['active_channels'] == ["34-20", "59-6"]
    assert result['active_gates'] == ["34", "20", "59", "6"]

    # Check parsed variables
    assert "variables" in result
    assert result['variables']['perspective_orientation'] == "Left"
    assert result['variables']['motivation_orientation'] == "Left"
    assert result['variables']['determination_orientation'] == "Left"
    assert result['variables']['environment_orientation'] == "Left"
    assert result['variables']['determination_type'] == "Cold Thirst"
    assert result['variables']['cognition_type'] == "Inner Vision"
    assert result['variables']['perspective_type'] == "Power"
    assert result['variables']['motivation_type'] == "Fear"
    assert result['variables']['environment_type'] is None # Not provided by API

    # Check insights structure
    assert "insights" in result
    insights = result['insights']
    assert "type_info" in insights
    assert "authority_info" in insights
    assert "profile_info" in insights
    assert "center_info" in insights
    assert "channel_info" in insights
    assert "variable_info" in insights
    assert "manifestation_notes" in insights

    # Check specific insights *based on the MOCK data*
    assert insights['type_info']['strategy_term'] == "To Wait to Respond" # Check strategy_term from mock
    assert insights['authority_info']['definition'] == "Decision comes from gut response."
    assert insights['profile_info']['name'] == "Hermit/Opportunist"
    assert "Sacral" in insights['center_info']
    assert insights['center_info']['Sacral']['theme'] == "Life Force, Work" # Accessing Defined part
    assert "34-20" in insights['channel_info']
    assert insights['channel_info']['34-20']['name'] == "Charisma"
    assert insights['variable_info']['determination']['definition'] == "Prefers cooler foods/drinks."
    assert insights['variable_info']['cognition']['definition'] == "Processes information internally."
    assert insights['variable_info']['motivation']['definition'] == "Driven by the need for security."
    assert insights['variable_info']['perspective']['definition'] == "Sees the world through power dynamics."
    assert len(insights['manifestation_notes']) > 0
    assert "Strategy (To Wait to Respond)" in insights['manifestation_notes'][0]
    assert "Authority (Sacral)" in insights['manifestation_notes'][0]

def test_interpret_human_design_chart_missing_fields(mocker):
    """Test interpretation with missing optional fields using mocked KB."""
    mocker.patch.dict('src.human_design.interpreter.HD_KNOWLEDGE_BASE', MOCK_KB_MISSING_FIELDS_CASE, clear=True)

    result = interpret_human_design_chart(SAMPLE_CHART_MISSING_FIELDS[0]) # Pass dict directly

    assert result is not None
    assert result['type'] == "Manifestor"
    assert result['profile'] == "4/1"
    assert result['authority'] == "Splenic"
    assert result['definition'] is None
    assert result['variables'] == {}
    assert result['active_gates'] == [] # API data for this case has no gates

    insights = result['insights']
    assert insights['type_info']['strategy_term'] == "To Inform before acting"
    assert insights['authority_info']['definition'] == "Decision comes from intuition."
    assert insights['profile_info']['name'] == "Opportunist/Investigator"
    assert "Spleen" in insights['center_info']
    assert insights['center_info']['Spleen']['theme'] == "Intuition, Health" # Accessing Defined part
    assert insights['variable_info'].get(KnowledgeBaseKeys.DETERMINATION.value) == {} # Check for empty dict
    assert insights['variable_info'].get(KnowledgeBaseKeys.COGNITION.value) == {}
    assert "Strategy (To Inform before acting)" in insights['manifestation_notes'][0]
    assert "Authority (Splenic)" in insights['manifestation_notes'][0]


def test_interpret_human_design_chart_invalid_input_not_dict(mocker): # Renamed, was not_list
    """Test with input that is not a dictionary."""
    # Minimal mock for KB, as this test focuses on input validation prior to KB use
    mocker.patch.dict('src.human_design.interpreter.HD_KNOWLEDGE_BASE', {}, clear=True)
    result = interpret_human_design_chart(["not a dict"]) # Pass a list instead of dict
    assert result is None

def test_interpret_human_design_chart_empty_input_dict(mocker):
    """Test with an empty dictionary input."""
    mocker.patch.dict('src.human_design.interpreter.HD_KNOWLEDGE_BASE', {}, clear=True)
    result = interpret_human_design_chart({})
    assert result is None # Or specific behavior for empty dict if defined

def test_interpret_human_design_chart_none_input(mocker):
    """Test with None input."""
    mocker.patch.dict('src.human_design.interpreter.HD_KNOWLEDGE_BASE', {}, clear=True)
    result = interpret_human_design_chart(None)
    assert result is None


def test_interpret_human_design_chart_variables_parsing_edge_cases(mocker):
    """Test edge cases for variable string parsing."""
    # Minimal mock for KB, as variable parsing is independent of KB content here
    mocker.patch.dict('src.human_design.interpreter.HD_KNOWLEDGE_BASE', MOCK_KB_SUCCESS_CASE, clear=True) # Use a valid KB

    # Malformed string
    chart_data_malformed = {**SAMPLE_CHART_DATA_LIST[0], "variables": "PR DRL"} # Too short
    result_malformed = interpret_human_design_chart(chart_data_malformed)
    assert result_malformed['variables'] == {}

    # Missing string
    chart_data_missing = {**SAMPLE_CHART_DATA_LIST[0]}
    if 'variables' in chart_data_missing: del chart_data_missing['variables']
    result_missing = interpret_human_design_chart(chart_data_missing)
    assert result_missing['variables'] == {}

    # None value
    chart_data_none = {**SAMPLE_CHART_DATA_LIST[0], "variables": None}
    result_none = interpret_human_design_chart(chart_data_none)
    assert result_none['variables'] == {}

def test_interpret_human_design_chart_authority_parsing(mocker):
    """Test parsing of authority string with variations using mocked KB."""
    mock_kb_data_authority = {
        KnowledgeBaseKeys.TYPES.value: {},
        KnowledgeBaseKeys.STRATEGIES.value: {},
        KnowledgeBaseKeys.AUTHORITIES.value: {
            "Emotional": {"definition": "Decision comes from emotional wave.", "role_in_manifestation": "Wait for clarity over time."},
            "Sacral": {"definition": "Decision comes from gut response.", "role_in_manifestation": "Follow the 'uh-huh' or 'uhn-uh'."}
        },
        KnowledgeBaseKeys.PROFILES.value: {}, KnowledgeBaseKeys.CENTERS.value: {},
        KnowledgeBaseKeys.GATES.value: {}, KnowledgeBaseKeys.CHANNELS.value: {},
        KnowledgeBaseKeys.VARIABLES.value: {
            KnowledgeBaseKeys.DETERMINATION.value: {}, KnowledgeBaseKeys.COGNITION.value: {},
            KnowledgeBaseKeys.MOTIVATION.value: {}, KnowledgeBaseKeys.PERSPECTIVE.value: {},
            KnowledgeBaseKeys.ENVIRONMENT.value: {}
        },
        KnowledgeBaseKeys.MANIFESTATION_MECHANICS.value: {},
        KnowledgeBaseKeys.G_CENTER_ACCESS_DETAILS.value: {}
    }
    mocker.patch.dict('src.human_design.interpreter.HD_KNOWLEDGE_BASE', mock_kb_data_authority, clear=True)

    # Authority with hyphen
    chart_data_hyphen = {**SAMPLE_CHART_DATA_LIST[0], "authority": "Emotional - Solar Plexus"}
    result_hyphen = interpret_human_design_chart(chart_data_hyphen)
    assert "authority_info" in result_hyphen['insights']
    assert result_hyphen['insights']['authority_info']['definition'] == "Decision comes from emotional wave."

    # Authority without hyphen
    chart_data_no_hyphen = {**SAMPLE_CHART_DATA_LIST[0], "authority": "Sacral"}
    result_no_hyphen = interpret_human_design_chart(chart_data_no_hyphen)
    assert "authority_info" in result_no_hyphen['insights']
    assert result_no_hyphen['insights']['authority_info']['definition'] == "Decision comes from gut response."


# The tests below were duplicated or tested input validation which is now covered above.
# Removing them to avoid redundancy and focus on KB interaction.

def test_interpret_human_design_chart_authority_parsing_direct_kb_access(mocker):
    """Test parsing of authority string with direct KB access assertion."""
    mock_kb_data_auth_desc = {
        KnowledgeBaseKeys.TYPES.value: {},
        KnowledgeBaseKeys.STRATEGIES.value: {},
        KnowledgeBaseKeys.AUTHORITIES.value: {
            "Emotional": {"definition": "Decision comes from emotional wave.", "description": "Emotional wave description."},
            "Sacral": {"definition": "Decision comes from gut response.", "description": "Sacral response description."}
        },
        KnowledgeBaseKeys.PROFILES.value: {}, KnowledgeBaseKeys.CENTERS.value: {},
        KnowledgeBaseKeys.GATES.value: {}, KnowledgeBaseKeys.CHANNELS.value: {},
        KnowledgeBaseKeys.VARIABLES.value: {
             KnowledgeBaseKeys.DETERMINATION.value: {}, KnowledgeBaseKeys.COGNITION.value: {},
             KnowledgeBaseKeys.MOTIVATION.value: {}, KnowledgeBaseKeys.PERSPECTIVE.value: {},
             KnowledgeBaseKeys.ENVIRONMENT.value: {}
        },
        KnowledgeBaseKeys.MANIFESTATION_MECHANICS.value: {},
        KnowledgeBaseKeys.G_CENTER_ACCESS_DETAILS.value: {}
    }
    mocker.patch.dict('src.human_design.interpreter.HD_KNOWLEDGE_BASE', mock_kb_data_auth_desc, clear=True)

    # Authority with hyphen
    chart_data_hyphen = {**SAMPLE_CHART_DATA_LIST[0], "authority": "Emotional - Solar Plexus"}
    result_hyphen = interpret_human_design_chart(chart_data_hyphen)
    assert "authority_info" in result_hyphen['insights']
    assert result_hyphen['insights']['authority_info']['description'] == HD_KNOWLEDGE_BASE[KnowledgeBaseKeys.AUTHORITIES.value]['Emotional']['description']

    # Authority without hyphen
    chart_data_no_hyphen = {**SAMPLE_CHART_DATA_LIST[0], "authority": "Sacral"}
    result_no_hyphen = interpret_human_design_chart(chart_data_no_hyphen)
    assert "authority_info" in result_no_hyphen['insights']
    assert result_no_hyphen['insights']['authority_info']['description'] == HD_KNOWLEDGE_BASE[KnowledgeBaseKeys.AUTHORITIES.value]['Sacral']['description']