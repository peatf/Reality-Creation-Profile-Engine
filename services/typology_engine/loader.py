import yaml
from pydantic import ValidationError
from typing import Dict, Any

from services.typology_engine.models import TypologyConfig

class SpecValidationError(ValueError):
    """Custom exception for specification validation errors not covered by Pydantic."""
    pass

def load_typology_spec_data(data: Dict[str, Any]) -> TypologyConfig:
    """
    Validates the raw dictionary data against the TypologyConfig model
    and performs additional custom validations.
    """
    try:
        config = TypologyConfig.model_validate(data)
    except ValidationError as e:
        # Re-raise Pydantic's validation error for schema issues
        raise e

    # Custom validation for duplicate IDs
    spectrum_ids = set()
    all_spectrum_question_ids = set() # To check for global uniqueness if needed, currently not enforced globally

    for spectrum in config.spectrums:
        if spectrum.id in spectrum_ids:
            raise SpecValidationError(f"Duplicate spectrum ID found: {spectrum.id}")
        spectrum_ids.add(spectrum.id)

        question_ids_in_spectrum = set()
        for question in spectrum.questions:
            if question.id in question_ids_in_spectrum:
                raise SpecValidationError(f"Duplicate question ID '{question.id}' in spectrum '{spectrum.id}'")
            question_ids_in_spectrum.add(question.id)
            # all_spectrum_question_ids.add(question.id) # Uncomment to track for global check

            # Pydantic's Literal type on SpectrumAnswerDetail.value handles invalid weights.
            # Pydantic's model structure for SpectrumAnswers handles missing answer keys.

    mastery_dim_ids = set()
    all_mastery_question_ids = set() # To check for global uniqueness if needed

    for dim in config.mastery_dimensions:
        if dim.id in mastery_dim_ids:
            raise SpecValidationError(f"Duplicate mastery_dimension ID found: {dim.id}")
        mastery_dim_ids.add(dim.id)

        question_ids_in_mastery_dim = set()
        for question in dim.questions:
            if question.id in question_ids_in_mastery_dim:
                raise SpecValidationError(f"Duplicate question ID '{question.id}' in mastery_dimension '{dim.id}'")
            question_ids_in_mastery_dim.add(question.id)
            # all_mastery_question_ids.add(question.id) # Uncomment to track for global check

            answer_ids_in_mastery_question = set()
            for answer in question.answers:
                if answer.id in answer_ids_in_mastery_question:
                    raise SpecValidationError(f"Duplicate answer ID '{answer.id}' in mastery question '{question.id}' (dimension '{dim.id}')")
                answer_ids_in_mastery_question.add(answer.id)
    
    # Example: Global check for spectrum question IDs (if required by business logic)
    # This is commented out as the original request didn't specify global uniqueness for question IDs.
    # If question IDs across all spectrums must be unique:
    # temp_q_ids = []
    # for spectrum in config.spectrums:
    #     for q in spectrum.questions:
    #         temp_q_ids.append(q.id)
    # if len(temp_q_ids) != len(set(temp_q_ids)):
    #     raise SpecValidationError("Duplicate question IDs found across different spectrums.")

    return config

def load_typology_spec_from_file(file_path: str) -> TypologyConfig:
    """
    Loads a typology specification from a YAML file, validates it,
    and returns a TypologyConfig object.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
    except FileNotFoundError:
        raise SpecValidationError(f"File not found: {file_path}")
    except yaml.YAMLError as e:
        raise SpecValidationError(f"Error parsing YAML file {file_path}: {e}")
    
    if data is None:
        raise SpecValidationError(f"YAML file is empty or invalid: {file_path}")
        
    return load_typology_spec_data(data)