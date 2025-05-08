import yaml
import pandas as pd
import numpy as np
import json
import os

def load_spec(spec_path: str) -> dict:
    """Loads the YAML specification file."""
    with open(spec_path, 'r') as f:
        return yaml.safe_load(f)

def calculate_cronbach_alpha(data: pd.DataFrame) -> float:
    """
    Calculates Cronbach's alpha for a set of items.
    Assumes data is a DataFrame where rows are subjects and columns are items.
    """
    if data.shape[1] < 2: # Need at least 2 items
        return np.nan
    
    # Item variances
    item_variances = data.var(axis=0, ddof=1).sum()
    # Total score variance
    total_scores = data.sum(axis=1)
    total_variance = total_scores.var(ddof=1)
    
    N = data.shape[1]
    
    if total_variance == 0: # Avoid division by zero if all total scores are the same
        return 1.0 if item_variances == 0 else 0.0

    alpha = (N / (N - 1)) * (1 - (item_variances / total_variance))
    return alpha

def generate_validation_report(spec_path: str, responses_path: str) -> dict:
    """
    Generates a validation report including Cronbach's alpha for each category.

    Args:
        spec_path: Path to the YAML specification file.
        responses_path: Path to the CSV/JSON file of simulated user responses.

    Returns:
        A dictionary containing the validation report.
    """
    spec = load_spec(spec_path)
    cronbach_alpha_threshold = spec.get('meta', {}).get('cronbach_alpha_threshold', 0.8)
    report = {
        "cronbach_alpha_threshold": cronbach_alpha_threshold,
        "categories": {},
        "overall_pass": True
    }

    # Load responses
    if responses_path.endswith('.csv'):
        responses_df = pd.read_csv(responses_path)
    elif responses_path.endswith('.json'):
        try:
            with open(responses_path, 'r') as f:
                responses_data = json.load(f)
            # Assuming JSON is a list of records like [{"answers": {...}}, ...]
            if isinstance(responses_data, list) and len(responses_data) > 0 and "answers" in responses_data[0]:
                responses_df = pd.DataFrame([r['answers'] for r in responses_data])
            else: # Or a direct list of answer dicts, or pandas-orient 'records'
                responses_df = pd.DataFrame(responses_data)
        except json.JSONDecodeError:
            raise ValueError(f"Invalid JSON file: {responses_path}")
        except Exception as e:
            raise ValueError(f"Could not parse JSON response file {responses_path}: {e}")

    else:
        raise ValueError("Responses file must be a CSV or JSON file.")

    # Ensure all question IDs from spec are columns in responses_df, fill missing with NaN
    all_question_ids = []
    for spectrum in spec.get('spectrums', []):
        for q in spectrum.get('questions', []):
            all_question_ids.append(q['id'])
    for dim in spec.get('mastery_dimensions', []):
        for q in dim.get('questions', []):
            all_question_ids.append(q['id'])
    
    for q_id in all_question_ids:
        if q_id not in responses_df.columns:
            responses_df[q_id] = np.nan # Or some other placeholder like 0 if appropriate

    # Calculate Cronbach's alpha for spectrums
    for spectrum in spec.get('spectrums', []):
        spectrum_id = spectrum['id']
        question_ids = [q['id'] for q in spectrum.get('questions', [])]
        
        # Ensure all question_ids for this spectrum exist in responses_df
        valid_question_ids = [qid for qid in question_ids if qid in responses_df.columns]
        if not valid_question_ids:
            alpha = np.nan
            item_stats = {}
            is_pass = False
        else:
            spectrum_data = responses_df[valid_question_ids].astype(float).dropna() # Drop rows with any NaN for this spectrum
            
            if spectrum_data.shape[0] < 2 or spectrum_data.shape[1] < 2: # Need at least 2 subjects and 2 items
                alpha = np.nan
                is_pass = False
            else:
                alpha = calculate_cronbach_alpha(spectrum_data)
                is_pass = alpha >= cronbach_alpha_threshold if not np.isnan(alpha) else False

            item_stats = {
                qid: {
                    "mean": responses_df[qid].mean() if qid in responses_df else np.nan,
                    "variance": responses_df[qid].var(ddof=1) if qid in responses_df else np.nan,
                    "stddev": responses_df[qid].std(ddof=1) if qid in responses_df else np.nan,
                    "min": responses_df[qid].min() if qid in responses_df else np.nan,
                    "max": responses_df[qid].max() if qid in responses_df else np.nan,
                } for qid in valid_question_ids
            }
            
        report["categories"][spectrum_id] = {
            "name": spectrum['name'],
            "type": "spectrum",
            "cronbach_alpha": alpha,
            "pass": is_pass,
            "item_count": len(valid_question_ids),
            "respondent_count_for_alpha": spectrum_data.shape[0] if 'spectrum_data' in locals() and not spectrum_data.empty else 0,
            "item_statistics": item_stats
        }
        if not is_pass:
            report["overall_pass"] = False

    # Calculate Cronbach's alpha for mastery dimensions
    # This assumes that mastery dimension answers in the responses_path are already numerically encoded.
    # If they are strings, they need to be converted to a numerical scale first.
    # For now, we proceed assuming they are numerical or can be cast to float.
    for dimension in spec.get('mastery_dimensions', []):
        dimension_id = dimension['id']
        question_ids = [q['id'] for q in dimension.get('questions', [])]

        valid_question_ids = [qid for qid in question_ids if qid in responses_df.columns]
        if not valid_question_ids:
            alpha = np.nan
            item_stats = {}
            is_pass = False
        else:
            # Attempt to convert to numeric, coercing errors to NaN
            dimension_data = responses_df[valid_question_ids].apply(pd.to_numeric, errors='coerce').dropna()

            if dimension_data.shape[0] < 2 or dimension_data.shape[1] < 2:
                alpha = np.nan
                is_pass = False
            else:
                alpha = calculate_cronbach_alpha(dimension_data)
                is_pass = alpha >= cronbach_alpha_threshold if not np.isnan(alpha) else False
            
            item_stats = {
                qid: {
                    "mean": responses_df[qid].astype(float).mean() if qid in responses_df and pd.api.types.is_numeric_dtype(responses_df[qid].astype(float)) else np.nan,
                    "variance": responses_df[qid].astype(float).var(ddof=1) if qid in responses_df and pd.api.types.is_numeric_dtype(responses_df[qid].astype(float)) else np.nan,
                    "stddev": responses_df[qid].astype(float).std(ddof=1) if qid in responses_df and pd.api.types.is_numeric_dtype(responses_df[qid].astype(float)) else np.nan,
                    "min": responses_df[qid].astype(float).min() if qid in responses_df and pd.api.types.is_numeric_dtype(responses_df[qid].astype(float)) else np.nan,
                    "max": responses_df[qid].astype(float).max() if qid in responses_df and pd.api.types.is_numeric_dtype(responses_df[qid].astype(float)) else np.nan,
                } for qid in valid_question_ids
            }

        report["categories"][dimension_id] = {
            "name": dimension['name'],
            "type": "mastery_dimension",
            "cronbach_alpha": alpha,
            "pass": is_pass,
            "item_count": len(valid_question_ids),
            "respondent_count_for_alpha": dimension_data.shape[0] if 'dimension_data' in locals() and not dimension_data.empty else 0,
            "item_statistics": item_stats
        }
        if not is_pass:
            report["overall_pass"] = False
    
    # Convert numpy types to python native types for JSON serialization compatibility
    def convert_numpy_types(obj):
        if isinstance(obj, dict):
            return {k: convert_numpy_types(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert_numpy_types(i) for i in obj]
        elif isinstance(obj, np.bool_):
            return bool(obj)
        elif isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif pd.isna(obj): # Handle pandas NaT/NaN if they appear
            return None
        return obj

    return convert_numpy_types(report)

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description="Generate Typology Validation Report")
    parser.add_argument(
        "--spec",
        type=str,
        default=os.getenv('TYPOLOGY_SPEC_PATH', 'assets/typology_questions.yml'),
        help="Path to the YAML specification file."
    )
    parser.add_argument(
        "--responses",
        type=str,
        default=os.getenv('TYPOLOGY_RESPONSES_PATH', 'tests/typology/test_dataset.json'),
        help="Path to the CSV/JSON file of simulated user responses."
    )
    args = parser.parse_args()

    SPEC_FILE = args.spec
    RESPONSES_FILE = args.responses

    # Ensure assets directory exists (if using default spec path)
    spec_dir = os.path.dirname(SPEC_FILE)
    if spec_dir and not os.path.exists(spec_dir) and spec_dir == 'assets':
        os.makedirs(spec_dir)
    
    # Ensure tests/typology directory exists (if using default responses path)
    responses_dir = os.path.dirname(RESPONSES_FILE)
    if responses_dir and not os.path.exists(responses_dir) and responses_dir == 'tests/typology':
        os.makedirs('assets')
    
    # Ensure tests/typology directory exists
    if not os.path.exists('tests/typology'):
        os.makedirs('tests/typology', exist_ok=True)

    # Create a dummy spec file if it doesn't exist
    if not os.path.exists(SPEC_FILE) and SPEC_FILE == 'assets/typology_questions.yml':
        print(f"Warning: Spec file {SPEC_FILE} not found. Creating a dummy one for basic script execution.")
        dummy_spec_content = {
            "meta": {"cronbach_alpha_threshold": 0.8},
            "spectrums": [{
                "id": "dummy_spectrum", "name": "Dummy Spectrum",
                "questions": [{"id": "q1"}, {"id": "q2"}, {"id": "q3"}]
            }],
            "mastery_dimensions": [{
                "id": "dummy_mastery", "name": "Dummy Mastery",
                "questions": [{"id": "mq1"}, {"id": "mq2"}]
            }]
        }
        with open(SPEC_FILE, 'w') as f:
            yaml.dump(dummy_spec_content, f)

    # Create a dummy responses file if it doesn't exist
    if not os.path.exists(RESPONSES_FILE) and RESPONSES_FILE == 'tests/typology/test_dataset.json':
        print(f"Warning: Responses file {RESPONSES_FILE} not found. Creating a dummy one for basic script execution.")
        dummy_responses_content = [
            {"answers": {"q1": 1, "q2": 0, "q3": -1, "mq1": 1, "mq2": 0}},
            {"answers": {"q1": 0, "q2": 1, "q3": 0, "mq1": 0, "mq2": 1}},
            {"answers": {"q1": -1, "q2": 0, "q3": 1, "mq1": 1, "mq2": 1}},
            {"answers": {"q1": 1, "q2": 1, "q3": 1, "mq1": 0, "mq2": 0}},
            {"answers": {"q1": 0, "q2": -1, "q3": -1, "mq1": 1, "mq2": 0}}
        ]
        with open(RESPONSES_FILE, 'w') as f:
            json.dump(dummy_responses_content, f)
    
    # Check if files exist before running
    if os.path.exists(SPEC_FILE) and os.path.exists(RESPONSES_FILE):
        print(f"Running regression report with spec: {SPEC_FILE} and responses: {RESPONSES_FILE}")
        try:
            validation_report = generate_validation_report(SPEC_FILE, RESPONSES_FILE)
            print("\nValidation Report:")
            print(json.dumps(validation_report, indent=2, default=lambda x: float(x) if isinstance(x, (np.floating, np.integer)) else x if not pd.isna(x) else None ))
            if not validation_report["overall_pass"]:
                print("\nRegression Test Failed: One or more categories did not meet the Cronbach's alpha threshold.")
                # In a CI/CD context, you would exit with a non-zero status code here
                import sys
                sys.exit(1)
            else:
                print("\nRegression Test Passed: All categories met the Cronbach's alpha threshold.")


        except FileNotFoundError as e:
            print(f"Error: {e}. Please ensure the spec and responses files exist at the specified paths.")
        except ValueError as e:
            print(f"Error generating report: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
    else:
        print("Skipping example run: SPEC_FILE or RESPONSES_FILE not found.")
        if not os.path.exists(SPEC_FILE):
            print(f"Missing: {SPEC_FILE}")
        if not os.path.exists(RESPONSES_FILE):
            print(f"Missing: {RESPONSES_FILE}")