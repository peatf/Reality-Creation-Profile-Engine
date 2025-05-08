import pytest
import json
import os
import pandas as pd
import numpy as np
import yaml
import tempfile

from services.typology_engine.regression import generate_validation_report, load_spec
from services.typology_engine.engine import TypologyEngine

# Define paths relative to the project root
# Assuming tests are run from the project root directory
SPEC_PATH = "assets/typology_questions.yml"
TEST_DATASET_PATH = "tests/typology/test_dataset.json"

# Helper function to load spec (if not already imported, but it is)
# from services.typology_engine.loader import load_spec as load_spec_engine

def generate_synthetic_responses_for_alpha(spec: dict, num_respondents: int = 150) -> pd.DataFrame:
    """
    Generates a synthetic DataFrame for Cronbach's Alpha testing.
    Spectrum answers are -1, 0, 1.
    Mastery answers are numerically encoded (0, 1, 2...).
    """
    all_question_data = {}

    # Spectrums
    for spectrum in spec.get('spectrums', []):
        question_ids = [q['id'] for q in spectrum.get('questions', [])]
        # For each respondent, generate a latent tendency for this spectrum
        latent_tendencies = np.random.uniform(-1.5, 1.5, num_respondents)
        for q_id in question_ids:
            answers = []
            for tendency in latent_tendencies:
                if tendency < -0.5:
                    ans = -1
                elif tendency > 0.5:
                    ans = 1
                else:
                    ans = 0
                # Add some noise: 10% chance of picking a random different answer
                if np.random.rand() < 0.10:
                    ans = np.random.choice([-1, 0, 1])
                answers.append(ans)
            all_question_data[q_id] = answers

    # Mastery Dimensions
    dimension_tendencies_cache = {} # Cache tendencies per dimension
    for dimension in spec.get('mastery_dimensions', []):
        dimension_id = dimension['id'] # Get dimension_id here

        # Generate or retrieve tendency for this dimension
        if dimension_id not in dimension_tendencies_cache:
             # Determine num_options based on the *first* question in the dimension
             # Assuming all questions in a dimension have the same number of options
             first_q_spec = dimension.get('questions', [{}])[0]
             first_q_answers = first_q_spec.get('answers', [])
             num_options_for_dim = len(first_q_answers)
             if num_options_for_dim == 0:
                 # Skip dimension if first question has no answers defined
                 print(f"Warning: Skipping dimension '{dimension_id}' in synthetic data generation - no answers found for first question.")
                 continue

             # Generate tendency only once per dimension per respondent batch
             dim_tendencies = np.random.normal(loc=num_options_for_dim / 2, scale=num_options_for_dim / 4, size=num_respondents)
             # Clamp tendencies to be within valid index range roughly
             dim_tendencies = np.clip(dim_tendencies, 0, num_options_for_dim - 1)
             dimension_tendencies_cache[dimension_id] = dim_tendencies
        else:
             dim_tendencies = dimension_tendencies_cache[dimension_id]

        for q_spec in dimension.get('questions', []):
            q_id = q_spec['id']
            # Mastery answers are lists of dicts, each with a 'value'
            possible_answer_values = [ans['value'] for ans in q_spec.get('answers', [])]
            if not possible_answer_values:
                all_question_data[q_id] = [np.nan] * num_respondents # Or handle as error
                continue

            # Numerically encode these string values for alpha calculation
            value_to_int_map = {val: i for i, val in enumerate(possible_answer_values)}
            num_options = len(possible_answer_values)
            num_options = len(possible_answer_values) # num_options specific to this question now
            if num_options == 0:
                 all_question_data[q_id] = [np.nan] * num_respondents # Handle questions with no answers
                 continue

            # Use the pre-calculated tendencies for this dimension
            dim_tendencies = dimension_tendencies_cache.get(dimension_id)
            if dim_tendencies is None: # Should not happen if dimension was processed above, but safety check
                 all_question_data[q_id] = [np.nan] * num_respondents
                 continue

            answers = []
            for i in range(num_respondents):
                respondent_tendency = dim_tendencies[i]
                # Base choice influenced by tendency (e.g., rounding the tendency)
                base_choice_idx = int(round(respondent_tendency))
                base_choice_idx = max(0, min(num_options - 1, base_choice_idx)) # Ensure valid index

                # Add some noise: 20% chance of picking a random different option
                if np.random.rand() < 0.10:
                    final_idx = np.random.randint(0, num_options)
                else:
                    final_idx = base_choice_idx # Use the calculated base_choice_idx
                answers.append(final_idx) # Store the numerically encoded answer
            all_question_data[q_id] = answers
            
    return pd.DataFrame(all_question_data)


def test_cronbach_alpha_validation(tmp_path):
    """
    Tests the generate_validation_report function for Cronbach's Alpha.
    Uses a synthetically generated dataset designed to pass alpha thresholds.
    """
    assert os.path.exists(SPEC_PATH), f"Spec file not found at {SPEC_PATH}"
    spec = load_spec(SPEC_PATH)
    
    num_synthetic_respondents = 200 # A larger dataset for alpha
    synthetic_df = generate_synthetic_responses_for_alpha(spec, num_synthetic_respondents)
    
    # Save synthetic data to a temporary JSON file (as list of dicts for answers)
    # regression.py expects a file path and can read JSON list of {"answers": ...}
    # or a direct list of answer dicts. Pandas to_dict('records') is suitable.
    
    # Create a temporary file for the synthetic responses
    # tempfile.NamedTemporaryFile creates a file that is deleted when closed.
    # We need a path that generate_validation_report can open.
    # tmp_path is a pytest fixture providing a temporary directory unique to the test call.
    
    synthetic_responses_path = tmp_path / "synthetic_responses.json"
    
    # The regression script expects a list of {"answers": record} if it's from test_dataset.json structure
    # or just a list of records if it's a simple list of answer dicts.
    # Let's format it as a list of answer dicts, which generate_validation_report handles.
    synthetic_df.to_json(synthetic_responses_path, orient='records', indent=2)

    assert os.path.exists(synthetic_responses_path), "Synthetic responses file was not created"

    report = generate_validation_report(SPEC_PATH, str(synthetic_responses_path))

    assert report["overall_pass"] is True, \
        f"Overall Cronbach's Alpha validation failed. Report: {json.dumps(report, indent=2)}"

    for category_id, details in report["categories"].items():
        assert details["pass"] is True, \
            f"Cronbach's Alpha validation failed for category '{details['name']}' ({category_id}). Alpha: {details['cronbach_alpha']}. Report: {json.dumps(details, indent=2)}"
        # Also check if respondent count for alpha is reasonable
        assert details.get("respondent_count_for_alpha", 0) > num_synthetic_respondents * 0.5, \
            f"Too few respondents for alpha calculation in category {category_id}, possibly due to NaNs."


def test_end_to_end_typology_engine():
    """
    Tests the TypologyEngine end-to-end by comparing calculated profiles
    with expected profiles from test_dataset.json.
    """
    assert os.path.exists(SPEC_PATH), f"Spec file not found at {SPEC_PATH}"
    assert os.path.exists(TEST_DATASET_PATH), f"Test dataset not found at {TEST_DATASET_PATH}"

    with open(TEST_DATASET_PATH, 'r') as f:
        test_data = json.load(f)

    # Initialize TypologyEngine - it loads the spec internally
    # Assuming TypologyEngine constructor takes spec_path
    # Need to confirm TypologyEngine's __init__ signature
    # Based on project structure, it's likely: services.typology_engine.engine.TypologyEngine
    try:
        engine = TypologyEngine(config_path=SPEC_PATH) # Changed spec_path to config_path
    except Exception as e:
        pytest.fail(f"Failed to initialize TypologyEngine: {e}")

    passed_count = 0
    failed_count = 0
    failures = []

    for i, entry in enumerate(test_data):
        answers = entry.get("answers")
        expected_profile = entry.get("expected_profile")

        if not answers or not expected_profile:
            failures.append(f"Test entry {i} is missing 'answers' or 'expected_profile'.")
            failed_count += 1
            continue
        
        try:
            # calculate_scores returns a dict. The top profile name is in:
            # result['top_matching_typologies'][0]['typology_name']
            result = engine.calculate_scores(answers)
            if result.get("top_matching_typologies") and len(result["top_matching_typologies"]) > 0:
                 # Extract the name from the first profile in the list
                 calculated_profile_name = result["top_matching_typologies"][0].get("typology_name")
            else:
                 calculated_profile_name = None # Or handle as an error/unexpected result
        except Exception as e:
            failures.append(f"Test entry {i} (Expected: {expected_profile}): Error during calculate_scores: {e}\nAnswers: {answers}")
            failed_count += 1
            continue

        if calculated_profile_name == expected_profile:
            passed_count += 1
        else:
            failed_count += 1
            failures.append(
                f"Test entry {i}: Expected profile '{expected_profile}', but got '{calculated_profile_name}'.\nAnswers: {answers}"
            )

    print(f"\nEnd-to-End Typology Engine Test Summary:")
    print(f"Passed: {passed_count}")
    print(f"Failed: {failed_count}")

    if failures:
        print("\nFailures:")
        for f_msg in failures:
            print(f"- {f_msg}")
        # Use pytest.fail to make the test fail if there are any discrepancies
        pytest.fail(f"{failed_count} end-to-end test(s) failed. See details above.")

    assert failed_count == 0, f"{failed_count} end-to-end test(s) failed."

# To run these tests, navigate to the project root in the terminal and run:
# python -m pytest tests/typology/test_regression.py
# Ensure that PYTHONPATH is set up correctly if services.typology_engine cannot be found,
# or run pytest from the directory containing the 'services' and 'tests' folders.
# e.g., if project root is Reality Creation Profile Engine, run from there.