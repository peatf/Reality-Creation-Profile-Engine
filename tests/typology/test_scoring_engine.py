import pytest
from pytest import approx
from services.typology_engine.scoring_engine import (
    calculate_typology_score,
    InvalidSubmissionError,
    IncompleteAssessmentError,
    TypologyConfigurationError
)

# Helper to get the mock_spec's relevant parts for assertions if needed,
# though direct calculation is better for test independence.
# For now, tests will rely on the hardcoded mock_spec in scoring_engine.py

VALID_ANSWERS_BASE = {
    "cognitive_alignment_1": "right",
    "flow_state_1": "creative_expression",
    "learning_style_1": "deep_dive",
    "reflection_frequency_1": "daily"
}

def test_full_valid_submission():
    """
    Tests a full valid submission and checks the output structure and key values.
    Relies on the mock_spec within scoring_engine.py for expected values.
    """
    user_answers = VALID_ANSWERS_BASE.copy()
    result = calculate_typology_score(user_answers)

    assert "typology_name" in result
    assert "confidence" in result
    assert "trace" in result

    trace = result["trace"]
    assert "raw_spectrum_scores" in trace
    assert "weighted_spectrum_scores" in trace
    assert "mastery_effects" in trace
    assert "final_spectrum_scores" in trace
    assert "final_rank" in trace
    assert "placement_key_used" in trace

    # Expected values based on manual calculation from mock_spec:
    # structure_vs_flow: raw = (1+1)=2, weighted=2*1.0=2.0. Mastery "daily" -> 1.1 multiplier. Final = 2.2
    # focus_vs_breadth: raw = (1)=1, weighted=1*0.8=0.8. Mastery "daily" -> 1.05 multiplier. Final = 0.84
    assert trace["raw_spectrum_scores"]["structure_vs_flow"] == 2.0
    assert trace["raw_spectrum_scores"]["focus_vs_breadth"] == 1.0

    assert trace["weighted_spectrum_scores"]["structure_vs_flow"] == 2.0
    assert trace["weighted_spectrum_scores"]["focus_vs_breadth"] == 0.8

    assert "self_awareness" in trace["mastery_effects"]
    assert trace["mastery_effects"]["self_awareness"]["structure_vs_flow"] == 1.1
    assert trace["mastery_effects"]["self_awareness"]["focus_vs_breadth"] == 1.05

    assert trace["final_spectrum_scores"]["structure_vs_flow"] == approx(2.2)
    assert trace["final_spectrum_scores"]["focus_vs_breadth"] == approx(0.84)

    # Placement key: structure_vs_flow (2.2 > 0.5) -> "strongly-flow"
    # focus_vs_breadth (0.84 > 0.3) -> "deep-dive"
    # key = "strongly-flow_deep-dive"
    assert trace["placement_key_used"] == "strongly-flow_deep-dive"
    # Typology name from mock_spec: "Visionary"
    assert result["typology_name"] == "Visionary"

    # Confidence: total_abs_score = 2.2 + 0.84 = 3.04. num_spectrums = 2.
    # norm_factor = 100.0 (from mock_spec's scoring_rules)
    # confidence = min(1.0, 3.04 / (2 * 100.0)) = min(1.0, 3.04 / 200.0) = 0.0152
    assert result["confidence"] == approx(0.0152)

    # Check final rank order (structure_vs_flow > focus_vs_breadth)
    assert trace["final_rank"][0]["spectrum_id"] == "structure_vs_flow"
    assert trace["final_rank"][0]["score"] == approx(2.2)
    assert trace["final_rank"][1]["spectrum_id"] == "focus_vs_breadth"
    assert trace["final_rank"][1]["score"] == approx(0.84)

def test_missing_required_spectrum_question():
    """Tests IncompleteAssessmentError if a spectrum question is missing."""
    user_answers = VALID_ANSWERS_BASE.copy()
    del user_answers["flow_state_1"] # Part of 'structure_vs_flow'
    with pytest.raises(IncompleteAssessmentError) as excinfo:
        calculate_typology_score(user_answers)
    assert "Missing answers for required questions" in str(excinfo.value)
    assert "'flow_state_1'" in str(excinfo.value)

def test_missing_required_mastery_question():
    """Tests IncompleteAssessmentError if a mastery question is missing."""
    user_answers = VALID_ANSWERS_BASE.copy()
    del user_answers["reflection_frequency_1"] # Part of 'self_awareness'
    with pytest.raises(IncompleteAssessmentError) as excinfo:
        calculate_typology_score(user_answers)
    assert "Missing answers for required questions" in str(excinfo.value)
    assert "'reflection_frequency_1'" in str(excinfo.value)

def test_invalid_answer_key_for_spectrum_question():
    """Tests InvalidSubmissionError for an invalid answer key in a spectrum question."""
    user_answers = VALID_ANSWERS_BASE.copy()
    user_answers["cognitive_alignment_1"] = "invalid_answer_for_spectrum"
    with pytest.raises(InvalidSubmissionError) as excinfo:
        calculate_typology_score(user_answers)
    assert "Invalid answer key 'invalid_answer_for_spectrum' for question 'cognitive_alignment_1'" in str(excinfo.value)

def test_invalid_answer_key_for_mastery_question():
    """Tests InvalidSubmissionError for an invalid answer key in a mastery question."""
    user_answers = VALID_ANSWERS_BASE.copy()
    user_answers["reflection_frequency_1"] = "invalid_answer_for_mastery" # Not 'daily', 'weekly', or 'monthly'
    with pytest.raises(InvalidSubmissionError) as excinfo:
        calculate_typology_score(user_answers)
    assert "Invalid answer key 'invalid_answer_for_mastery' for mastery question 'reflection_frequency_1'" in str(excinfo.value)

def test_spectrum_tie_break_consistency():
    """
    Tests behavior when spectrum scores might be tied.
    The current mock_spec makes exact ties difficult without modification.
    This test sets up a scenario where scores are numerically close or could be identical
    if weights/influences were different. It primarily checks if final_rank is consistent.
    For a true tie-break affecting placement_name, the mock_spec's placement_mapping
    and key generation logic would need to be more sophisticated.
    """
    # To force a tie, we'd need to adjust mock_spec or answers carefully.
    # Let's use answers that make scores equal before mastery, then see mastery effect.
    # structure_vs_flow: cognitive_alignment_1 (left: -1), flow_state_1 (structured_approach: -1) -> raw = -2. weight = 1.0 -> -2.0
    # focus_vs_breadth: learning_style_1 (broad_scan: -1). To make it -2.0, raw would be -2.0 / 0.8 = -2.5.
    # This is not possible with current single question for focus_vs_breadth with value -1.

    # Let's try to make final scores equal.
    # structure_vs_flow: raw = X, weighted = X*1.0. Mastery "daily" -> 1.1. Final = 1.1X
    # focus_vs_breadth: raw = Y, weighted = Y*0.8. Mastery "daily" -> 1.05. Final = 0.8 * 1.05 * Y = 0.84Y
    # We want 1.1X = 0.84Y. If X=0.84, Y=1.1.
    # Can we get raw score 0.84 for structure_vs_flow? Not easily with integer influences.
    # Can we get raw score 1.1 for focus_vs_breadth? Not easily.

    # Let's use a simpler case: make weighted scores equal before mastery.
    # structure_vs_flow: weight 1.0. focus_vs_breadth: weight 0.8.
    # If structure_vs_flow raw = 0.8, weighted = 0.8.
    # If focus_vs_breadth raw = 1, weighted = 0.8.
    # This is achievable:
    # cognitive_alignment_1: "center" (0), flow_state_1: "N/A" (need to add to mock or use one q)
    # Let's assume structure_vs_flow has a question "q_struct" with {"val": 0.8}
    # And focus_vs_breadth has "learning_style_1": {"deep_dive": 1}
    # This test highlights dependency on mock_spec.
    # For now, we'll use an example that results in different scores but tests ranking.

    user_answers = {
        "cognitive_alignment_1": "left",  # svf: -1
        "flow_state_1": "structured_approach", # svf: -1. Raw svf = -2. Weighted svf = -2.0. Final svf = -2.0 * 1.1 = -2.2
        "learning_style_1": "broad_scan",       # fvb: -1. Raw fvb = -1. Weighted fvb = -0.8. Final fvb = -0.8 * 1.05 = -0.84
        "reflection_frequency_1": "daily"
    }
    # Here, svf (-2.2) < fvb (-0.84). So fvb should rank higher (less negative is better if reverse=True means higher value).
    # The sort is `reverse=True`, so higher scores come first.
    # -0.84 is "higher" than -2.2.
    result = calculate_typology_score(user_answers)
    final_rank = result["trace"]["final_rank"]

    assert final_rank[0]["spectrum_id"] == "focus_vs_breadth"
    assert final_rank[0]["score"] == approx(-0.84)
    assert final_rank[1]["spectrum_id"] == "structure_vs_flow"
    assert final_rank[1]["score"] == approx(-2.2)

    # If scores were identical, e.g., both 1.0, Python's list.sort() is stable.
    # If `adjusted_spectrum_scores` was `{"a_spec": 1.0, "b_spec": 1.0}`,
    # `sorted(..., key=lambda item: item[1], reverse=True)`
    # The order of items with equal sort keys is preserved from the original dict's iteration order (Python 3.7+).
    # For a more explicit tie-break rule test, the placement logic itself would need to show it.
    # This test primarily confirms the sorting works as expected.

def test_unhandled_question_in_answers():
    """
    Tests how unhandled (not in spec) questions in user_answers are treated.
    Currently, the validation loop iterates over user_answers. If a question is not
    in any spec, it might pass validation if not required, or fail if it tries to find
    its definition. The current validation logic in scoring_engine.py:
    `if is_spectrum_q: ... elif is_mastery_q: ...`
    If a question is neither, it won't raise InvalidSubmissionError there.
    It also won't contribute to scores. This test is to document current behavior.
    """
    user_answers = VALID_ANSWERS_BASE.copy()
    user_answers["extra_unhandled_question"] = "some_answer"

    # Expect it to run without error, and the extra question should not affect scores.
    result_orig = calculate_typology_score(VALID_ANSWERS_BASE.copy())
    result_with_extra = calculate_typology_score(user_answers)

    # Check that core results are identical
    assert result_orig["typology_name"] == result_with_extra["typology_name"]
    assert result_orig["confidence"] == result_with_extra["confidence"]
    assert result_orig["trace"]["final_spectrum_scores"] == result_with_extra["trace"]["final_spectrum_scores"]
    # The extra question should not appear in any score calculation trace.
    assert "extra_unhandled_question" not in result_with_extra["trace"]["raw_spectrum_scores"]


# Note: Testing TypologyConfigurationError would require mocking `load_spec`
# or passing an invalid path if `load_spec` handled file errors by raising it.
# Since `load_spec` is commented out and `mock_spec` is used, this path isn't
# directly testable for `calculate_typology_score` in its current form.