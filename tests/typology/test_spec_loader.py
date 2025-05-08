import pytest
import pytest
import yaml
import copy # Import deepcopy
from pydantic import ValidationError
from pathlib import Path

from services.typology_engine.loader import load_typology_spec_from_file, load_typology_spec_data, SpecValidationError
from services.typology_engine.models import TypologyConfig # Ensure models are accessible

# Minimal valid structure for testing
MINIMAL_VALID_SPEC = {
    "version": "1.0.0",
    "released_at": "2023-01-01",
    "meta": {
        "minimum_items_per_category": 1,
        "cronbach_alpha_threshold": 0.7
    },
    "spectrums": [
        {
            "id": "spec1",
            "name": "Spectrum One",
            "description": "Desc for Spec One",
            "left_label": "Left",
            "right_label": "Right",
            "questions": [
                {
                    "id": "q1_spec1",
                    "text": "Question 1 for Spec One?",
                    "answers": {
                        "left": {"label": "Left Answer", "value": -1},
                        "balanced": {"label": "Balanced Answer", "value": 0},
                        "right": {"label": "Right Answer", "value": 1}
                    }
                }
            ]
        }
    ],
    "mastery_dimensions": [
        {
            "id": "mastery1",
            "name": "Mastery Dimension One",
            "description": "Desc for Mastery One",
            "questions": [
                {
                    "id": "mq1_mastery1",
                    "text": "Mastery Question 1?",
                    "answers": [
                        {"id": "ma1_mq1", "label": "Mastery Answer 1", "value": "val1"},
                        {"id": "ma2_mq1", "label": "Mastery Answer 2", "value": "val2"}
                    ]
                }
            ]
        }
    ],
    "scoring_rules": {
        "spectrum_priority_order": ["spec1"],
        "placement_mapping": {"spec1_Left": "ProfileA"},
        "mastery_spectrum_influence": {
            "mastery1": {
                "val1": {"spec1": 0.5}
            }
        }
    },
    "results": {
        "typology_profiles": [
            {
                "id": "ProfileA",
                "typology_name": "Profile A Name",
                "headline": "Profile A Headline",
                "description": {
                    "essence": "Essence A",
                    "strength": "Strength A",
                    "challenge": "Challenge A",
                    "approach": "Approach A",
                    "growth": "Growth A"
                },
                "tips": {
                    "ideal_approaches": {"key": "value"},
                    "common_misalignments": ["mis1"],
                    "shifts_needed": ["shift1"],
                    "acceptance_permissions": ["perm1"],
                    "energy_support_tools": ["tool1"]
                }
            }
        ]
    }
}

@pytest.fixture
def valid_spec_file(tmp_path: Path) -> str:
    file_path = tmp_path / "valid_spec.yml"
    with open(file_path, "w", encoding="utf-8") as f:
        yaml.dump(MINIMAL_VALID_SPEC, f)
    return str(file_path)

@pytest.fixture
def minimal_valid_data() -> dict:
    # Return a deep copy to prevent modification across tests
    return copy.deepcopy(MINIMAL_VALID_SPEC)

def test_successful_parse_from_file(valid_spec_file: str):
    """Test successful parsing of a valid YAML file."""
    config = load_typology_spec_from_file(valid_spec_file)
    assert isinstance(config, TypologyConfig)
    assert config.version == "1.0.0"
    assert len(config.spectrums) == 1
    assert config.spectrums[0].id == "spec1"
    assert len(config.mastery_dimensions) == 1
    assert config.mastery_dimensions[0].id == "mastery1"

def test_successful_parse_from_data(minimal_valid_data: dict):
    """Test successful parsing of valid dictionary data."""
    config = load_typology_spec_data(minimal_valid_data)
    assert isinstance(config, TypologyConfig)
    assert config.version == "1.0.0"

def test_duplicate_spectrum_id(minimal_valid_data: dict):
    """Test detection of duplicate spectrum IDs."""
    minimal_valid_data["spectrums"].append(minimal_valid_data["spectrums"][0].copy()) # Duplicate spec1
    with pytest.raises(SpecValidationError, match="Duplicate spectrum ID found: spec1"):
        load_typology_spec_data(minimal_valid_data)

def test_duplicate_question_id_in_spectrum(minimal_valid_data: dict):
    """Test detection of duplicate question IDs within the same spectrum."""
    minimal_valid_data["spectrums"][0]["questions"].append(
        minimal_valid_data["spectrums"][0]["questions"][0].copy() # Duplicate q1_spec1
    )
    with pytest.raises(SpecValidationError, match="Duplicate question ID 'q1_spec1' in spectrum 'spec1'"):
        load_typology_spec_data(minimal_valid_data)

def test_duplicate_mastery_dimension_id(minimal_valid_data: dict):
    """Test detection of duplicate mastery_dimension IDs."""
    minimal_valid_data["mastery_dimensions"].append(minimal_valid_data["mastery_dimensions"][0].copy())
    with pytest.raises(SpecValidationError, match="Duplicate mastery_dimension ID found: mastery1"):
        load_typology_spec_data(minimal_valid_data)

def test_duplicate_question_id_in_mastery_dimension(minimal_valid_data: dict):
    """Test detection of duplicate question IDs within the same mastery dimension."""
    minimal_valid_data["mastery_dimensions"][0]["questions"].append(
        minimal_valid_data["mastery_dimensions"][0]["questions"][0].copy()
    )
    with pytest.raises(SpecValidationError, match="Duplicate question ID 'mq1_mastery1' in mastery_dimension 'mastery1'"):
        load_typology_spec_data(minimal_valid_data)

def test_duplicate_answer_id_in_mastery_question(minimal_valid_data: dict):
    """Test detection of duplicate answer IDs within the same mastery question."""
    minimal_valid_data["mastery_dimensions"][0]["questions"][0]["answers"].append(
        minimal_valid_data["mastery_dimensions"][0]["questions"][0]["answers"][0].copy()
    )
    with pytest.raises(SpecValidationError, match="Duplicate answer ID 'ma1_mq1' in mastery question 'mq1_mastery1'"):
        load_typology_spec_data(minimal_valid_data)

def test_missing_spectrum_answer_keys(minimal_valid_data: dict):
    """Test Pydantic validation for missing answer keys in spectrum questions."""
    del minimal_valid_data["spectrums"][0]["questions"][0]["answers"]["right"]
    # Match the specific field path and "Field required" on the next line
    with pytest.raises(ValidationError, match=r"spectrums\.0\.questions\.0\.answers\.right\s+Field required"):
        load_typology_spec_data(minimal_valid_data)

def test_invalid_spectrum_answer_weight(minimal_valid_data: dict):
    """Test Pydantic validation for invalid answer weights in spectrum questions."""
    minimal_valid_data["spectrums"][0]["questions"][0]["answers"]["left"]["value"] = 5
    with pytest.raises(ValidationError, match="Input should be -1, 0 or 1"):
        # Match based on Pydantic's Literal validation error
        load_typology_spec_data(minimal_valid_data)

def test_missing_required_top_level_field(minimal_valid_data: dict):
    """Test Pydantic validation for missing top-level required fields (e.g., 'spectrums')."""
    del minimal_valid_data["spectrums"]
    # Match "spectrums" and "Field required" on the next line
    with pytest.raises(ValidationError, match=r"spectrums\s+Field required"):
        load_typology_spec_data(minimal_valid_data)

def test_invalid_data_type_for_field(minimal_valid_data: dict):
    """Test Pydantic validation for incorrect data types (e.g., meta.minimum_items_per_category as string)."""
    minimal_valid_data["meta"]["minimum_items_per_category"] = "not-an-int"
    with pytest.raises(ValidationError, match="Input should be a valid integer"):
        load_typology_spec_data(minimal_valid_data)

def test_load_non_existent_file():
    """Test loading a non-existent YAML file."""
    with pytest.raises(SpecValidationError, match="File not found: non_existent_spec.yml"):
        load_typology_spec_from_file("non_existent_spec.yml")

def test_load_invalid_yaml_file(tmp_path: Path):
    """Test loading a file with invalid YAML syntax."""
    file_path = tmp_path / "invalid_syntax.yml"
    with open(file_path, "w", encoding="utf-8") as f:
        f.write("spectrums: [id: spec1\n  name: Test") # Intentionally malformed YAML
    with pytest.raises(SpecValidationError, match="Error parsing YAML file"):
        load_typology_spec_from_file(str(file_path))

def test_load_empty_yaml_file(tmp_path: Path):
    """Test loading an empty YAML file."""
    file_path = tmp_path / "empty.yml"
    with open(file_path, "w", encoding="utf-8") as f:
        f.write("") # Empty file
    with pytest.raises(SpecValidationError, match="YAML file is empty or invalid"):
        load_typology_spec_from_file(str(file_path))

def test_load_yaml_file_with_only_null(tmp_path: Path):
    """Test loading a YAML file that parses to None (e.g. just 'null')."""
    file_path = tmp_path / "null.yml"
    with open(file_path, "w", encoding="utf-8") as f:
        f.write("null")
    with pytest.raises(SpecValidationError, match="YAML file is empty or invalid"):
        load_typology_spec_from_file(str(file_path))