# tests/synthesis/test_narrator.py
import pytest
from unittest.mock import patch as unittest_patch # For use in pytest if mocker isn't ideal for a case
from src.synthesis.narrator import format_synthesized_output, HD_KNOWLEDGE_BASE, get_astro_definition # Import for patching
from src.constants import KnowledgeBaseKeys # For structuring mock HD KB
# Import maps needed for verification (assuming they exist and are populated)
from services.typology_engine.results_generator import TYPOLOGY_PAIRS, IDEAL_APPROACHES, COMMON_MISALIGNMENTS

# --- Sample Data ---

PROFILE_ID = "narrator_test_1"

# Minimal synthesis results for structure test
SYNTHESIS_RESULTS_BASIC = {
    "synthesized_insights": []
}
# Sample synthesis results *including* text that simulates embedded definitions
SYNTHESIS_RESULTS_WITH_DEFINITIONS = {
    "synthesized_insights": [
        {
            "id": "SYN001",
            "text": "As a Generator, you are designed to respond to life. This core mechanism is key.",
            "category": "Core",
            "derived_from": ["Typology: X", "HD Type: Generator"]
        },
        {
            "id": "SYN002",
            "text": "Your Leo Sun indicates your core identity shines brightly, seeking recognition.",
            "category": "Potential",
            "derived_from": ["Sun Sign: Leo"]
        },
    ]
}

ASSESSMENT_RESULTS_SAMPLE = {
    "typologyPair": {"key": "structured-fluid", "primary": {}, "secondary": {}}, # Key for lookup
    "spectrumPlacements": {"cognitive-alignment": "structured", "kinetic-drive": "fluid"},
    # Other assessment data...
}

ASTRO_FACTORS_SAMPLE = {
    "Sun": {"sign": "Leo", "house": 1},
    "Moon": {"sign": "Pisces", "house": 7},
}

HD_INTERPRETED_SAMPLE = {
    "type": "Manifesting Generator",
    "strategy": "To Respond then Inform", # Added for completeness
    "authority": "Sacral",
    "profile": "1/3",
    "variables": { # Added for completeness
        "motivation_type": "Hope",
        "perspective_type": "Possibility"
    }
    # Other HD data...
}

# --- Mock Knowledge Base Data ---
MOCK_HD_KB_NARRATOR = {
    KnowledgeBaseKeys.TYPES.value: {
        "Manifesting Generator": {"definition": "MG Type Def", "strategy_term": "To Respond then Inform"}
    },
    KnowledgeBaseKeys.STRATEGIES.value: {
        "To Respond then Inform": {"definition": "MG Strategy Def"}
    },
    KnowledgeBaseKeys.AUTHORITIES.value: {
        "Sacral": {"definition": "Sacral Authority Def"}
    },
    KnowledgeBaseKeys.VARIABLES.value: {
        KnowledgeBaseKeys.MOTIVATION.value: {
            "Hope": {"definition": "Hope Motivation Def"}
        },
        KnowledgeBaseKeys.PERSPECTIVE.value: {
            "Possibility": {"definition": "Possibility Perspective Def"}
        }
    }
    # Add other necessary keys if narrator uses them (e.g., PROFILES)
}

MOCK_ASTRO_DEFS_NARRATOR = {
    "Leo": {"definition": "Leo Sun Sign Def"},
    # Add other signs/terms if needed by tests
}


# --- Test Cases ---

def test_format_synthesized_output_structure():
    """Test the basic output structure with minimal input."""
    result = format_synthesized_output(PROFILE_ID, SYNTHESIS_RESULTS_BASIC)

    assert result["profile_id"] == PROFILE_ID
    assert "summary" in result
    assert "details" in result
    assert "strategies" in result
    assert "synthesized_insights" in result
    assert isinstance(result["summary"], dict)
    assert isinstance(result["details"], dict)
    assert isinstance(result["strategies"], dict)
    assert isinstance(result["synthesized_insights"], list)

def test_format_synthesized_output_insights_formatting_with_definitions():
    """Test that synthesized insights are formatted and include definition text."""
    # Use the sample data that includes simulated definition snippets
    result = format_synthesized_output(PROFILE_ID, SYNTHESIS_RESULTS_WITH_DEFINITIONS)

    assert len(result["synthesized_insights"]) == len(SYNTHESIS_RESULTS_WITH_DEFINITIONS["synthesized_insights"])

    # Check first insight (SYN001)
    first_insight_input = SYNTHESIS_RESULTS_WITH_DEFINITIONS['synthesized_insights'][0]
    first_insight_output = result["synthesized_insights"][0]

    assert "formatted_text" in first_insight_output
    # Check basic formatting (category bolded)
    assert first_insight_output["formatted_text"].startswith(f"**{first_insight_input['category']}:**")
    # Check that the original text (including definition snippet) is present
    assert first_insight_input["text"] in first_insight_output["formatted_text"]
    # Check specific definition snippet presence
    assert "designed to respond" in first_insight_output["formatted_text"]
    # Check other fields are passed through
    assert first_insight_output["id"] == first_insight_input['id']
    assert first_insight_output["category"] == first_insight_input['category']
    assert first_insight_output["derived_from"] == first_insight_input['derived_from']

    # Check second insight (SYN002)
    second_insight_input = SYNTHESIS_RESULTS_WITH_DEFINITIONS['synthesized_insights'][1]
    second_insight_output = result["synthesized_insights"][1]
    assert "formatted_text" in second_insight_output
    assert second_insight_output["formatted_text"].startswith(f"**{second_insight_input['category']}:**")
    assert second_insight_input["text"] in second_insight_output["formatted_text"]
    assert "shines brightly" in second_insight_output["formatted_text"] # Check definition snippet
    assert second_insight_output["id"] == second_insight_input['id']

@unittest_patch('src.synthesis.narrator.get_astro_definition')
def test_format_synthesized_output_summary_population(mock_get_astro_def, mocker):
    """Test population of the summary section with mocked KBs."""
    # Mock HD_KNOWLEDGE_BASE
    mocker.patch.dict('src.synthesis.narrator.HD_KNOWLEDGE_BASE', MOCK_HD_KB_NARRATOR, clear=True)

    # Setup mock for get_astro_definition
    def side_effect_get_astro_def(term_name):
        return MOCK_ASTRO_DEFS_NARRATOR.get(term_name)
    mock_get_astro_def.side_effect = side_effect_get_astro_def

    result = format_synthesized_output(
        PROFILE_ID,
        SYNTHESIS_RESULTS_WITH_DEFINITIONS,
        assessment_results=ASSESSMENT_RESULTS_SAMPLE,
        astro_factors=ASTRO_FACTORS_SAMPLE,
        hd_interpreted=HD_INTERPRETED_SAMPLE
    )

    summary = result["summary"]
    typology_key = ASSESSMENT_RESULTS_SAMPLE["typologyPair"]["key"]
    expected_typology_name = TYPOLOGY_PAIRS.get(typology_key, {}).get("name", "Unknown Typology")

    assert summary.get("typology_name") == expected_typology_name
    assert summary.get("human_design_type") == HD_INTERPRETED_SAMPLE["type"]
    assert summary.get("human_design_type_definition") == MOCK_HD_KB_NARRATOR[KnowledgeBaseKeys.TYPES.value]["Manifesting Generator"]["definition"]
    assert summary.get("human_design_strategy") == HD_INTERPRETED_SAMPLE["strategy"]
    assert summary.get("human_design_strategy_definition") == MOCK_HD_KB_NARRATOR[KnowledgeBaseKeys.STRATEGIES.value]["To Respond then Inform"]["definition"]
    assert summary.get("human_design_authority") == HD_INTERPRETED_SAMPLE["authority"]
    assert summary.get("human_design_authority_definition") == MOCK_HD_KB_NARRATOR[KnowledgeBaseKeys.AUTHORITIES.value]["Sacral"]["definition"]
    
    assert summary.get("human_design_motivation") == HD_INTERPRETED_SAMPLE["variables"]["motivation_type"]
    assert summary.get("human_design_motivation_definition") == MOCK_HD_KB_NARRATOR[KnowledgeBaseKeys.VARIABLES.value][KnowledgeBaseKeys.MOTIVATION.value]["Hope"]["definition"]
    assert summary.get("human_design_perspective") == HD_INTERPRETED_SAMPLE["variables"]["perspective_type"]
    assert summary.get("human_design_perspective_definition") == MOCK_HD_KB_NARRATOR[KnowledgeBaseKeys.VARIABLES.value][KnowledgeBaseKeys.PERSPECTIVE.value]["Possibility"]["definition"]

    assert summary.get("sun_sign") == ASTRO_FACTORS_SAMPLE["Sun"]["sign"]
    assert summary.get("sun_sign_definition") == MOCK_ASTRO_DEFS_NARRATOR["Leo"]["definition"]
    mock_get_astro_def.assert_called_with("Leo")

def test_format_synthesized_output_strategies_population():
    """Test population of the strategies section."""
    result = format_synthesized_output(
        PROFILE_ID,
        SYNTHESIS_RESULTS_WITH_DEFINITIONS, # Use updated sample
        assessment_results=ASSESSMENT_RESULTS_SAMPLE
    )

    strategies = result["strategies"]
    typology_key = ASSESSMENT_RESULTS_SAMPLE["typologyPair"]["key"]
    # Handle potential KeyErrors for missing typology_key
    expected_ideal = IDEAL_APPROACHES.get(typology_key, {})
    expected_misalign = COMMON_MISALIGNMENTS.get(typology_key, [])


    assert "ideal_approaches" in strategies
    assert "common_misalignments" in strategies
    assert strategies["ideal_approaches"].get("strengths_summary") == expected_ideal.get("strengths", "")
    assert strategies["ideal_approaches"].get("list") == expected_ideal.get("approaches", [])
    assert strategies["common_misalignments"] == expected_misalign

def test_format_synthesized_output_details_population():
    """Test population of the details section (example: spectrums)."""
    result = format_synthesized_output(
        PROFILE_ID,
        SYNTHESIS_RESULTS_WITH_DEFINITIONS, # Use updated sample
        assessment_results=ASSESSMENT_RESULTS_SAMPLE
    )

    details = result["details"]
    assert "assessment_spectrums" in details
    assert details["assessment_spectrums"] == ASSESSMENT_RESULTS_SAMPLE["spectrumPlacements"]

def test_format_synthesized_output_missing_inputs():
    """Test behavior when optional input dictionaries are None or missing keys."""
    # Only synthesis results provided (use updated sample)
    result_only_synth = format_synthesized_output(PROFILE_ID, SYNTHESIS_RESULTS_WITH_DEFINITIONS)
    assert result_only_synth["summary"] == {}
    assert result_only_synth["strategies"] == {}
    # Check insights are still processed
    assert len(result_only_synth["synthesized_insights"]) == 2
    assert "designed to respond" in result_only_synth["synthesized_insights"][0]["formatted_text"]
    assert result_only_synth["details"] == {}

    # Assessment results missing typologyPair key
    assessment_missing_key = {"spectrumPlacements": {"cognitive-alignment": "structured"}}
    result_missing_key = format_synthesized_output(PROFILE_ID, SYNTHESIS_RESULTS_WITH_DEFINITIONS, assessment_results=assessment_missing_key)
    assert result_missing_key["summary"] == {} # Can't get typology name
    assert result_missing_key["strategies"] == {} # Can't get strategies
    # Check insights are still processed
    assert len(result_missing_key["synthesized_insights"]) == 2
    assert "shines brightly" in result_missing_key["synthesized_insights"][1]["formatted_text"]

    # Astro factors missing Sun sign
    astro_missing_sun = {"Moon": {"sign": "Pisces"}}
    result_missing_sun = format_synthesized_output(PROFILE_ID, SYNTHESIS_RESULTS_WITH_DEFINITIONS, astro_factors=astro_missing_sun)
    assert "sun_sign" not in result_missing_sun["summary"]
    # Check insights are still processed
    assert len(result_missing_sun["synthesized_insights"]) == 2