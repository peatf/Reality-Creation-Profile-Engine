"""
Typology Scoring Engine
"""
from typing import Dict, Any, List, Tuple

# Assuming TypologySpec will be defined in models.py and load_spec in loader.py
# from .loader import load_spec
# from .models import TypologySpec # Placeholder

# --- Custom Exceptions ---

class InvalidSubmissionError(ValueError):
    """Custom exception for invalid submission data."""
    pass

class IncompleteAssessmentError(ValueError):
    """Custom exception for incomplete assessment answers."""
    pass

class TypologyConfigurationError(Exception):
    """Custom exception for issues with typology specification."""
    pass


def calculate_typology_score(user_answers: Dict[str, str], spec_path: str = "assets/typology_questions.yml") -> Dict[str, Any]:
    """
    Calculates the typology score based on user answers and a typology specification.

    Args:
        user_answers: A dictionary of question IDs to answer keys.
        spec_path: Path to the typology specification YAML file.

    Returns:
        A dictionary containing the typology name, confidence, and trace details.

    Raises:
        InvalidSubmissionError: If answer keys are invalid.
        IncompleteAssessmentError: If required questions are not answered.
        TypologyConfigurationError: If the typology spec is misconfigured.
    """
    # Placeholder for loading the specification
    # spec: TypologySpec = load_spec(spec_path)
    # if not spec:
    #     raise TypologyConfigurationError("Failed to load typology specification.")

    # Placeholder for actual spec data until loader and models are integrated
    # This is a simplified mock for now.
    # In a real scenario, this would come from load_spec() and models.TypologySpec
    mock_spec = {
        "name": "Reality Creation Profile",
        "spectrums": {
            "structure_vs_flow": {
                "name": "Structure vs. Flow",
                "weight": 1.0,
                "questions": ["cognitive_alignment_1", "flow_state_1"],
                "answers_influence": {
                    "cognitive_alignment_1": {"right": 1, "left": -1, "center": 0},
                    "flow_state_1": {"creative_expression": 1, "structured_approach": -1}
                }
            },
            "focus_vs_breadth": {
                "name": "Focus vs. Breadth",
                "weight": 0.8,
                "questions": ["learning_style_1"],
                "answers_influence": {
                     "learning_style_1": {"deep_dive": 1, "broad_scan": -1}
                }
            }
        },
        "mastery_dimensions": {
            "self_awareness": {
                "name": "Self-Awareness",
                "questions": ["reflection_frequency_1"],
                "scoring_rules": {
                    "mastery_spectrum_influence": {
                        "daily": {"structure_vs_flow": 1.1, "focus_vs_breadth": 1.05},
                        "weekly": {"structure_vs_flow": 1.0, "focus_vs_breadth": 1.0},
                        "monthly": {"structure_vs_flow": 0.9, "focus_vs_breadth": 0.95},
                    }
                }
            }
        },
        "placement_mapping": {
            "strongly-structured_deep-dive": "Architect",
            "moderately-structured_deep-dive": "Planner",
            "balanced_deep-dive": "Analyst",
            "moderately-flow_deep-dive": "Strategist",
            "strongly-flow_deep-dive": "Visionary",
            "strongly-structured_broad-scan": "Systematizer",
            # ... more mappings
        },
        "scoring_rules": { # General scoring rules, if any, not used in this simplified version yet
            "confidence_normalization_factor": 100.0 # Example
        }
    }
    spec_name = mock_spec["name"]
    spectrums_spec = mock_spec["spectrums"]
    mastery_dimensions_spec = mock_spec["mastery_dimensions"]
    placement_mapping_spec = mock_spec["placement_mapping"]
    # scoring_rules_spec = mock_spec.get("scoring_rules", {})


    # 1. Validate completeness
    required_questions = set()
    for spectrum_id, spectrum_data in spectrums_spec.items():
        required_questions.update(spectrum_data.get("questions", []))
    for dim_id, dim_data in mastery_dimensions_spec.items():
        required_questions.update(dim_data.get("questions", []))

    answered_questions = set(user_answers.keys())
    missing_questions = required_questions - answered_questions
    if missing_questions:
        raise IncompleteAssessmentError(f"Missing answers for required questions: {sorted(list(missing_questions))}")

    # Validate answer keys (basic check, assumes all answers in spec are valid)
    for q_id, ans_key in user_answers.items():
        found_in_spectrum = any(
            q_id in s_data.get("questions", []) and ans_key in s_data.get("answers_influence", {}).get(q_id, {})
            for s_data in spectrums_spec.values()
        )
        found_in_mastery = any(
            q_id in m_data.get("questions", []) and ans_key in m_data.get("scoring_rules", {}).get("mastery_spectrum_influence", {})
            for m_data in mastery_dimensions_spec.values()
        )
        # This validation is simplified. A real spec would list valid answer keys per question.
        # For now, we assume if a question is part of a spectrum, its answer key must be in that spectrum's answers_influence.
        # If it's a mastery question, its answer key must be a key in mastery_spectrum_influence.
        # This needs refinement once models.py defines the spec structure more concretely.
        is_spectrum_q = any(q_id in s_data.get("questions", []) for s_data in spectrums_spec.values())
        is_mastery_q = any(q_id in m_data.get("questions", []) for m_data in mastery_dimensions_spec.values())

        if is_spectrum_q:
            valid_keys_for_q = []
            for s_data in spectrums_spec.values():
                if q_id in s_data.get("questions", []):
                    valid_keys_for_q.extend(s_data.get("answers_influence", {}).get(q_id, {}).keys())
            if not valid_keys_for_q or ans_key not in valid_keys_for_q:
                 raise InvalidSubmissionError(f"Invalid answer key '{ans_key}' for question '{q_id}'. Valid keys: {valid_keys_for_q}")
        elif is_mastery_q:
            # For mastery questions, the answer key itself is used to lookup influence rules
            valid_mastery_keys = []
            for m_data in mastery_dimensions_spec.values():
                if q_id in m_data.get("questions", []): # Check if this question belongs to this mastery dim
                    valid_mastery_keys.extend(m_data.get("scoring_rules", {}).get("mastery_spectrum_influence", {}).keys())
            if not valid_mastery_keys or ans_key not in valid_mastery_keys:
                raise InvalidSubmissionError(f"Invalid answer key '{ans_key}' for mastery question '{q_id}'. Valid keys: {valid_mastery_keys}")
        # If a question is not in any spec, it's an issue, but completeness check should catch it if it's required.
        # If it's an extra, unrequired question, we might ignore it or raise an error. For now, assume all user_answers keys are for known questions.


    # 2. Calculate initial spectrum scores
    spectrum_scores: Dict[str, float] = {}
    raw_spectrum_scores: Dict[str, float] = {} # Before weighting

    for spectrum_id, spectrum_data in spectrums_spec.items():
        current_spectrum_sum = 0.0
        for question_id in spectrum_data.get("questions", []):
            if question_id in user_answers:
                answer_key = user_answers[question_id]
                answer_values = spectrum_data.get("answers_influence", {}).get(question_id, {})
                if answer_key in answer_values:
                    current_spectrum_sum += answer_values[answer_key]
                else:
                    # This should have been caught by validation if question_id was in answers_influence
                    raise InvalidSubmissionError(
                        f"Answer key '{answer_key}' for question '{question_id}' not found in spectrum '{spectrum_id}' influence mapping."
                    )
        raw_spectrum_scores[spectrum_id] = current_spectrum_sum
        spectrum_scores[spectrum_id] = current_spectrum_sum * spectrum_data.get("weight", 1.0)

    # 3. Apply mastery dimension effects
    mastery_effects: Dict[str, Dict[str, float]] = {} # E.g. {"self_awareness": {"structure_vs_flow": 1.1}}

    for dim_id, dim_data in mastery_dimensions_spec.items():
        dim_effects_on_spectrums: Dict[str, float] = {}
        for question_id in dim_data.get("questions", []):
            if question_id in user_answers:
                answer_key = user_answers[question_id] # e.g., "daily"
                influence_rules = dim_data.get("scoring_rules", {}).get("mastery_spectrum_influence", {})

                if answer_key in influence_rules:
                    # influence_rules[answer_key] is like {"structure_vs_flow": 1.1, ...}
                    for spectrum_id_to_influence, multiplier in influence_rules[answer_key].items():
                        if spectrum_id_to_influence in spectrum_scores:
                            # Store the multiplier itself for tracing
                            dim_effects_on_spectrums[spectrum_id_to_influence] = multiplier
                        else:
                            # This would be a spec configuration error
                            pass # Or log a warning
                else:
                    # This should have been caught by validation
                    raise InvalidSubmissionError(
                        f"Answer key '{answer_key}' for mastery question '{question_id}' not found in dimension '{dim_id}' scoring rules."
                    )
        if dim_effects_on_spectrums:
            mastery_effects[dim_id] = dim_effects_on_spectrums

    # Apply the collected multipliers
    adjusted_spectrum_scores = spectrum_scores.copy()
    for dim_id, effects in mastery_effects.items():
        for spectrum_id, multiplier in effects.items():
            if spectrum_id in adjusted_spectrum_scores:
                adjusted_spectrum_scores[spectrum_id] *= multiplier


    # 4. Determine final rank and placement key
    # Order spectrums by their final scores (descending).
    # The problem description implies "ordered spectrum scores" are used for placement.
    # This part is highly dependent on how placement_mapping keys are constructed.
    # Assuming placement keys are like "spectrum1State-spectrum2State-..."
    # For simplicity, let's assume a simple categorization (e.g., positive is one state, negative another)
    # This needs to be more robust based on actual TypologySpec definition.

    final_ranked_spectrums: List[Tuple[str, float]] = sorted(
        adjusted_spectrum_scores.items(),
        key=lambda item: item[1],
        reverse=True
    )

    # Example: Create a placement key based on the dominant characteristic of each spectrum.
    # This is a placeholder and needs to be defined by the TypologySpec's rules.
    # For instance, if scores are bipolar (-N to +N), we might categorize:
    # score > threshold_high => "strong-[spectrum_positive_name]"
    # score < threshold_low => "strong-[spectrum_negative_name]"
    # else => "balanced-[spectrum_name]"
    # The key for placement_mapping would be a concatenation of these.

    # Simplified placement key generation:
    # This assumes a very simple mapping where the order of spectrums and their relative strength
    # directly maps to a key. The example "strongly-structured" implies categories per spectrum.
    # Let's assume for "structure_vs_flow": >0 is "flow", <0 is "structure".
    # And for "focus_vs_breadth": >0 is "deep-dive", <0 is "broad-scan".
    # This is highly speculative and needs to come from the spec.

    placement_key_parts = []
    # This logic is very basic and needs to be driven by the spec's definition of how to derive placement keys.
    # For now, let's use the names of the top N spectrums or some qualitative assessment.
    # The example "strongly-structured" suggests qualitative buckets.
    # Let's assume a simple thresholding for the example spectrums:
    if "structure_vs_flow" in adjusted_spectrum_scores:
        score = adjusted_spectrum_scores["structure_vs_flow"]
        if score > 0.5: placement_key_parts.append("strongly-flow")
        elif score > 0: placement_key_parts.append("moderately-flow")
        elif score < -0.5: placement_key_parts.append("strongly-structured")
        elif score < 0: placement_key_parts.append("moderately-structured")
        else: placement_key_parts.append("balanced-structure_flow")

    if "focus_vs_breadth" in adjusted_spectrum_scores:
        score = adjusted_spectrum_scores["focus_vs_breadth"]
        if score > 0.3: placement_key_parts.append("deep-dive") # Adjusted threshold for example
        elif score < -0.3: placement_key_parts.append("broad-scan")
        else: placement_key_parts.append("balanced-focus_breadth")

    placement_key = "_".join(placement_key_parts) if placement_key_parts else "default_placement"


    typology_result_name = placement_mapping_spec.get(placement_key, "Undefined Typology")
    if typology_result_name == "Undefined Typology":
        # Try to find the closest match or handle default
        # This is a fallback, ideally keys match perfectly.
        # For now, if no exact match, we'll indicate it.
        # A more sophisticated system might use fuzzy matching or a default based on dominant spectrum.
        pass


    # 5. Calculate confidence
    # Confidence: "normalized score strength". This is also underspecified.
    # Possibilities:
    # - Magnitude of the primary spectrum score relative to its max possible score.
    # - Average magnitude of all spectrum scores relative to their max.
    # - Variance or standard deviation of scores.
    # For now, let's use a simple approach: average absolute score, normalized.
    # Max possible raw score sum for a spectrum (assuming all answers align perfectly for max value)
    # This requires knowing the min/max values from `answers_influence`.
    # For simplicity, let's assume scores are somewhat normalized around a range, e.g., -X to +X.
    # A placeholder for confidence:
    total_abs_score = sum(abs(s) for s in adjusted_spectrum_scores.values())
    num_spectrums = len(adjusted_spectrum_scores)
    # This normalization factor should ideally come from the spec or be dynamically calculated
    # based on max possible scores.
    normalization_factor = mock_spec.get("scoring_rules", {}).get("confidence_normalization_factor", 10.0) # Arbitrary
    confidence = min(1.0, total_abs_score / (num_spectrums * normalization_factor)) if num_spectrums > 0 else 0.0
    confidence = round(confidence, 4)


    # 6. Result Output
    result = {
        "typology_name": typology_result_name,
        "confidence": confidence,
        "trace": {
            "raw_spectrum_scores": raw_spectrum_scores, # Scores before weighting
            "weighted_spectrum_scores": spectrum_scores, # Scores after weighting, before mastery
            "mastery_effects": mastery_effects, # Multipliers applied
            "final_spectrum_scores": adjusted_spectrum_scores, # Scores after mastery
            "final_rank": [{"spectrum_id": item[0], "score": item[1]} for item in final_ranked_spectrums],
            "placement_key_used": placement_key,
        }
    }

    return result

if __name__ == '__main__':
    # Example Usage (requires a more complete mock_spec or actual loader)
    print("Typology Scoring Engine - Basic Test")

    # Test Case 1: Full valid submission (example)
    sample_answers_valid = {
        "cognitive_alignment_1": "right",  # structure_vs_flow: +1
        "flow_state_1": "creative_expression", # structure_vs_flow: +1
        "learning_style_1": "deep_dive",       # focus_vs_breadth: +1
        "reflection_frequency_1": "daily"      # mastery: self_awareness
    }
    try:
        score_result = calculate_typology_score(sample_answers_valid)
        print("\n--- Valid Submission Result ---")
        import json
        print(json.dumps(score_result, indent=2))
    except (InvalidSubmissionError, IncompleteAssessmentError, TypologyConfigurationError) as e:
        print(f"Error during scoring: {e}")

    # Test Case 2: Missing question
    sample_answers_incomplete = {
        "cognitive_alignment_1": "right",
        # "flow_state_1": "creative_expression", # Missing
        "learning_style_1": "deep_dive",
        "reflection_frequency_1": "daily"
    }
    try:
        print("\n--- Incomplete Submission Test ---")
        score_result = calculate_typology_score(sample_answers_incomplete)
        print(json.dumps(score_result, indent=2))
    except IncompleteAssessmentError as e:
        print(f"Caught expected error: {e}")
    except Exception as e:
        print(f"Caught unexpected error: {e}")


    # Test Case 3: Invalid answer key
    sample_answers_invalid_key = {
        "cognitive_alignment_1": "very_right", # Invalid key
        "flow_state_1": "creative_expression",
        "learning_style_1": "deep_dive",
        "reflection_frequency_1": "daily"
    }
    try:
        print("\n--- Invalid Answer Key Test ---")
        score_result = calculate_typology_score(sample_answers_invalid_key)
        print(json.dumps(score_result, indent=2))
    except InvalidSubmissionError as e:
        print(f"Caught expected error: {e}")
    except Exception as e:
        print(f"Caught unexpected error: {e}")

    # Test Case 4: Mastery influencing scores
    sample_answers_mastery_low = {
        "cognitive_alignment_1": "left",        # structure_vs_flow: -1
        "flow_state_1": "structured_approach",  # structure_vs_flow: -1
        "learning_style_1": "broad_scan",       # focus_vs_breadth: -1
        "reflection_frequency_1": "monthly"     # mastery: self_awareness (lower influence)
    }
    try:
        print("\n--- Mastery Influence (Low) ---")
        score_result_low_mastery = calculate_typology_score(sample_answers_mastery_low)
        print(json.dumps(score_result_low_mastery, indent=2))

        # Compare with high mastery
        sample_answers_mastery_high = sample_answers_mastery_low.copy()
        sample_answers_mastery_high["reflection_frequency_1"] = "daily" # mastery: self_awareness (higher influence)
        print("\n--- Mastery Influence (High) ---")
        score_result_high_mastery = calculate_typology_score(sample_answers_mastery_high)
        print(json.dumps(score_result_high_mastery, indent=2))

    except (InvalidSubmissionError, IncompleteAssessmentError, TypologyConfigurationError) as e:
        print(f"Error during mastery test: {e}")

    # Example of how placement key might work
    # Assuming structure_vs_flow score = -1.5 (strongly-structured)
    # Assuming focus_vs_breadth score = 0.8 (deep-dive)
    # Expected placement_key = "strongly-structured_deep-dive"
    # Expected typology_name = "Architect" (from mock_spec)

    # To test tie-breaking, we'd need a spec where two spectrums could easily have identical scores
    # and the placement mapping has distinct outcomes based on a tie-breaking rule (e.g. predefined order).
    # The current `final_ranked_spectrums` sorts by score, then implicitly by key if scores are equal.
    # A specific tie-breaking rule in placement_mapping logic would be needed if simple sorting isn't enough.
    print("\nNote: Confidence and placement key generation are simplified in this standalone example.")
    print("Full integration with loader.py and models.py is required for production use.")

"""
TODO:
- Integrate with actual `loader.load_spec()` and `models.TypologySpec`.
  The current `mock_spec` is a placeholder.
- Refine validation of answer keys based on the actual `TypologySpec` structure.
- Implement robust placement key generation logic as defined by `TypologySpec`.
  The current method is a simplified placeholder.
- Implement robust confidence calculation as defined by `TypologySpec`.
  The current method is a simplified placeholder.
- Add more comprehensive logging for debugging and auditing.
- Consider edge cases for scoring (e.g., all zero scores, ties in spectrum ranking if relevant for placement).
"""