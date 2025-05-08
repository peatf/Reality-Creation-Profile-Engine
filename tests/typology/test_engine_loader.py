import pytest
import yaml
from pathlib import Path
from pydantic import ValidationError

# Assuming the engine is importable relative to the tests directory
# Adjust the import path if your project structure differs
from services.typology_engine.engine import TypologyEngine
from services.typology_engine.models import TypologyConfig

# Define the path to the valid config file relative to the project root
VALID_CONFIG_PATH = "assets/typology_questions.yml"
TEST_DIR = Path("tests/typology/test_data")

# Helper function to create temporary YAML files for testing
def create_temp_yaml(tmp_path: Path, filename: str, content: dict):
    """Creates a temporary YAML file in the specified path."""
    filepath = tmp_path / filename
    with open(filepath, 'w') as f:
        yaml.dump(content, f)
    return str(filepath) # Return path as string for TypologyEngine

# --- Fixtures ---

@pytest.fixture(scope="module")
def valid_config_data():
    """Loads the raw data from the valid config file."""
    with open(VALID_CONFIG_PATH, 'r') as f:
        return yaml.safe_load(f)

@pytest.fixture
def minimal_valid_config():
    """Provides a minimal but structurally valid config dictionary."""
    return {
        "version": "0.1.0",
        "released_at": "2024-01-01",
        "meta": {"minimum_items_per_category": 1, "cronbach_alpha_threshold": 0.7},
        "spectrums": [
            {
                "id": "spec1", "name": "Spectrum 1", "description": "Desc 1",
                "left_label": "Left", "right_label": "Right",
                "questions": [
                    {
                        "id": "q1", "text": "Question 1?",
                        "answers": {
                            "left": {"label": "L", "value": -1},
                            "balanced": {"label": "B", "value": 0},
                            "right": {"label": "R", "value": 1}
                        }
                    }
                ]
            }
        ],
        "mastery_dimensions": [
             {
                "id": "mastery1", "name": "Mastery 1", "description": "Mastery Desc 1",
                "questions": [
                    {
                        "id": "mq1", "text": "Mastery Q1?",
                        "answers": [{"id": "ma1", "label": "Ans 1", "value": "val1"}]
                    }
                ]
            }
        ],
        "scoring_rules": {
            "spectrum_priority_order": ["spec1"],
            "placement_mapping": {"balanced": "balanced"},
            "mastery_spectrum_influence": {
                "mastery1": {
                    "val1": {"spec1": 0.5}
                }
            }
        },
        "results": {
            "typology_profiles": [
                {
                    "id": "type1", "typology_name": "Type 1", "headline": "Headline 1",
                    "description": {"essence": "e", "strength": "s", "challenge": "c", "approach": "a", "growth": "g"},
                    "tips": {
                        "ideal_approaches": {"strengths": "s", "approaches": ["a1"]},
                        "common_misalignments": ["m1"],
                        "shifts_needed": ["sn1"],
                        "acceptance_permissions": ["ap1"],
                        "energy_support_tools": ["est1"]
                    }
                }
            ]
        }
    }

# --- Test Cases ---

def test_successful_load_valid_config():
    """Tests loading the actual valid configuration file."""
    try:
        engine = TypologyEngine(config_path=VALID_CONFIG_PATH)
        assert isinstance(engine.config, TypologyConfig)
        assert engine.config.version is not None
        assert len(engine.config.spectrums) > 0
        assert len(engine.config.mastery_dimensions) > 0
        assert len(engine.config.results.typology_profiles) > 0
        # Check if lookups were built
        assert "cognitive_q1" in engine.spectrum_questions # Example question ID
        assert "core_q1" in engine.mastery_questions # Example question ID
    except Exception as e:
        pytest.fail(f"Loading valid config failed: {e}")

def test_load_non_existent_file():
    """Tests that FileNotFoundError is raised for a missing config file."""
    with pytest.raises(FileNotFoundError):
        TypologyEngine(config_path="tests/typology/non_existent_config.yml")

def test_load_invalid_yaml_syntax(tmp_path):
    """Tests that ValueError is raised for invalid YAML syntax."""
    invalid_yaml_content = "key: value\n  bad_indent: oops"
    filepath = tmp_path / "invalid_syntax.yaml"
    with open(filepath, 'w') as f:
        f.write(invalid_yaml_content)

    with pytest.raises(ValueError, match="Error parsing YAML file"):
        TypologyEngine(config_path=str(filepath))

def test_load_missing_required_key(tmp_path, minimal_valid_config):
    """Tests that ValueError (from Pydantic) is raised for missing required keys."""
    invalid_data = minimal_valid_config.copy()
    del invalid_data["spectrums"] # Remove a required top-level key
    filepath = create_temp_yaml(tmp_path, "missing_key.yaml", invalid_data)

    with pytest.raises(ValueError, match="Error validating config file"):
         TypologyEngine(config_path=filepath)

def test_load_incorrect_data_type(tmp_path, minimal_valid_config):
    """Tests that ValueError (from Pydantic) is raised for incorrect data types."""
    invalid_data = minimal_valid_config.copy()
    # Make a value the wrong type (e.g., string instead of int)
    invalid_data["meta"]["minimum_items_per_category"] = "should_be_int"
    filepath = create_temp_yaml(tmp_path, "wrong_type.yaml", invalid_data)

    with pytest.raises(ValueError, match="Error validating config file"):
        TypologyEngine(config_path=filepath)

def test_load_invalid_spectrum_answer_value(tmp_path, minimal_valid_config):
    """Tests validation for spectrum answer values (-1, 0, 1)."""
    invalid_data = minimal_valid_config.copy()
    invalid_data["spectrums"][0]["questions"][0]["answers"]["left"]["value"] = -2 # Invalid value
    filepath = create_temp_yaml(tmp_path, "invalid_answer_val.yaml", invalid_data)

    with pytest.raises(ValueError, match="Error validating config file"):
        TypologyEngine(config_path=filepath)

def test_load_invalid_mastery_influence_value(tmp_path, minimal_valid_config):
    """Tests validation for mastery influence values (should be float)."""
    invalid_data = minimal_valid_config.copy()
    invalid_data["scoring_rules"]["mastery_spectrum_influence"]["mastery1"]["val1"]["spec1"] = "should_be_float"
    filepath = create_temp_yaml(tmp_path, "invalid_influence_val.yaml", invalid_data)

    with pytest.raises(ValueError, match="Error validating config file"):
        TypologyEngine(config_path=filepath)

def test_load_missing_spectrum_answer_key(tmp_path, minimal_valid_config):
    """Tests validation fails if a spectrum question misses left/balanced/right."""
    invalid_data = minimal_valid_config.copy()
    # Delete one of the required keys
    del invalid_data["spectrums"][0]["questions"][0]["answers"]["balanced"]
    filepath = create_temp_yaml(tmp_path, "missing_answer_key.yaml", invalid_data)

    # This should be caught by Pydantic validation
    with pytest.raises(ValueError, match="Error validating config file"):
        TypologyEngine(config_path=filepath)

# --- Duplicate ID Tests (using the custom validation in __init__) ---

def test_load_duplicate_spectrum_id(tmp_path, minimal_valid_config):
    """Tests ValueError is raised for duplicate spectrum IDs."""
    invalid_data = minimal_valid_config.copy()
    invalid_data["spectrums"].append(invalid_data["spectrums"][0].copy()) # Duplicate spec1
    filepath = create_temp_yaml(tmp_path, "duplicate_spec_id.yaml", invalid_data)

    with pytest.raises(ValueError, match="Duplicate Spectrum ID found: spec1"):
        TypologyEngine(config_path=filepath)

def test_load_duplicate_mastery_id(tmp_path, minimal_valid_config):
    """Tests ValueError is raised for duplicate mastery dimension IDs."""
    invalid_data = minimal_valid_config.copy()
    invalid_data["mastery_dimensions"].append(invalid_data["mastery_dimensions"][0].copy()) # Duplicate mastery1
    filepath = create_temp_yaml(tmp_path, "duplicate_mastery_id.yaml", invalid_data)

    with pytest.raises(ValueError, match="Duplicate Mastery Dimension ID found: mastery1"):
        TypologyEngine(config_path=filepath)

def test_load_duplicate_question_id(tmp_path, minimal_valid_config):
    """Tests ValueError is raised for duplicate question IDs across types."""
    invalid_data = minimal_valid_config.copy()
    # Make mastery question ID same as spectrum question ID
    invalid_data["mastery_dimensions"][0]["questions"][0]["id"] = "q1"
    filepath = create_temp_yaml(tmp_path, "duplicate_question_id.yaml", invalid_data)

    with pytest.raises(ValueError, match="Duplicate Question ID found: q1"):
        TypologyEngine(config_path=filepath)

def test_load_duplicate_mastery_answer_id(tmp_path, minimal_valid_config):
    """Tests ValueError is raised for duplicate answer IDs within a mastery question."""
    invalid_data = minimal_valid_config.copy()
    # Add a duplicate answer ID within the first mastery question
    invalid_data["mastery_dimensions"][0]["questions"][0]["answers"].append(
        {"id": "ma1", "label": "Duplicate Ans", "value": "val_dup"}
    )
    filepath = create_temp_yaml(tmp_path, "duplicate_answer_id.yaml", invalid_data)

    with pytest.raises(ValueError, match="Duplicate Answer ID 'ma1' found within Question 'mq1'"):
        TypologyEngine(config_path=filepath)

def test_load_duplicate_typology_profile_id(tmp_path, minimal_valid_config):
    """Tests ValueError is raised for duplicate typology profile IDs."""
    invalid_data = minimal_valid_config.copy()
    invalid_data["results"]["typology_profiles"].append(invalid_data["results"]["typology_profiles"][0].copy()) # Duplicate type1
    filepath = create_temp_yaml(tmp_path, "duplicate_profile_id.yaml", invalid_data)

    with pytest.raises(ValueError, match="Duplicate Typology Profile ID found: type1"):
        TypologyEngine(config_path=filepath)