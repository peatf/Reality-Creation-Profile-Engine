# tests/assessment/test_scorer.py
import pytest
from services.typology_engine.scorer import (
    calculate_mastery_scores,
    determine_dominant_values,
    apply_mastery_influences,
    MASTERY_SPECTRUM_INFLUENCE,
    calculate_typology_scores,
    calculate_mastery_alignment_score,
    determine_typology_pair,
    SPECTRUM_PRIORITY_ORDER,
    PLACEMENT_MAPPING,
    calculate_energy_focus_scores,
    generate_complete_results
)
from services.typology_engine.definitions import TYPOLOGY_SPECTRUMS

# --- Helper Function ---
def generate_full_typology_responses(value: str) -> dict:
    """Generates a full set of typology responses with the given value."""
    responses = {}
    for spectrum in TYPOLOGY_SPECTRUMS:
        for question in spectrum['questions']:
            responses[question['id']] = value
    return responses

# --- Test Cases ---

# Test cases for calculate_mastery_scores
def test_calculate_mastery_scores_empty():
    """Test with no mastery responses."""
    assert calculate_mastery_scores({}) == {
        'corePriorities': {}, 'growthAreas': {},
        'alignmentNeeds': {}, 'energyPatterns': {}
    }

def test_calculate_mastery_scores_single_category():
    """Test with responses in a single category."""
    responses = {
        'core-q1': 'creative-expression',
        'core-q2': 'financial-abundance',
        'core-q3': 'creative-expression'
    }
    expected = {
        'corePriorities': {'creative-expression': 2, 'financial-abundance': 1},
        'growthAreas': {},
        'alignmentNeeds': {},
        'energyPatterns': {}
    }
    assert calculate_mastery_scores(responses) == expected

def test_calculate_mastery_scores_multiple_categories():
    """Test with responses across multiple categories."""
    responses = {
        'core-q1': 'creative-expression',
        'growth-q1': 'consistency-challenge',
        'alignment-q1': 'accept-cycles',
        'energy-q1': 'intuitive-instincts',
        'core-q2': 'creative-expression',
        'growth-q2': 'clarity-challenge',
        'growth-q3': 'consistency-challenge',
    }
    expected = {
        'corePriorities': {'creative-expression': 2},
        'growthAreas': {'consistency-challenge': 2, 'clarity-challenge': 1},
        'alignmentNeeds': {'accept-cycles': 1},
        'energyPatterns': {'intuitive-instincts': 1}
    }
    assert calculate_mastery_scores(responses) == expected

def test_calculate_mastery_scores_unknown_prefix():
    """Test with an unknown question prefix, should be ignored."""
    responses = {
        'core-q1': 'creative-expression',
        'unknown-q1': 'some-value'
    }
    expected = {
        'corePriorities': {'creative-expression': 1},
        'growthAreas': {},
        'alignmentNeeds': {},
        'energyPatterns': {}
    }
    assert calculate_mastery_scores(responses) == expected

# Test cases for determine_dominant_values
def test_determine_dominant_values_empty():
    """Test with empty mastery scores."""
    scores = {'corePriorities': {}, 'growthAreas': {}, 'alignmentNeeds': {}, 'energyPatterns': {}}
    expected = {'corePriorities': [], 'growthAreas': [], 'alignmentNeeds': [], 'energyPatterns': []}
    assert determine_dominant_values(scores) == expected

def test_determine_dominant_values_single_dominant():
    """Test with a single dominant value per category."""
    scores = {
        'corePriorities': {'a': 2, 'b': 1},
        'growthAreas': {'c': 3},
        'alignmentNeeds': {},
        'energyPatterns': {'d': 1, 'e': 5}
    }
    expected = {
        'corePriorities': ['a'],
        'growthAreas': ['c'],
        'alignmentNeeds': [],
        'energyPatterns': ['e']
    }
    assert determine_dominant_values(scores) == expected

def test_determine_dominant_values_multiple_dominant_tie():
    """Test with multiple dominant values (tie) in a category."""
    scores = {
        'corePriorities': {'a': 2, 'b': 2, 'c': 1},
        'growthAreas': {'d': 3, 'e': 3},
        'alignmentNeeds': {'f': 1},
        'energyPatterns': {}
    }
    # Order doesn't matter within the list for ties
    result = determine_dominant_values(scores)
    assert sorted(result['corePriorities']) == sorted(['a', 'b'])
    assert sorted(result['growthAreas']) == sorted(['d', 'e'])
    assert result['alignmentNeeds'] == ['f']
    assert result['energyPatterns'] == []

def test_determine_dominant_values_missing_category():
    """Test when a category is missing from the input scores dict."""
    scores = {
        'corePriorities': {'a': 1},
        # 'growthAreas' is missing
        'alignmentNeeds': {'b': 2},
        'energyPatterns': {}
    }
    expected = {
        'corePriorities': ['a'],
        'growthAreas': [], # Should default to empty list
        'alignmentNeeds': ['b'],
        'energyPatterns': []
    }
    assert determine_dominant_values(scores) == expected

# Test cases for apply_mastery_influences
def test_apply_mastery_influences_no_dominant():
    """Test when there are no dominant values."""
    numeric_scores = {'cognitive-alignment': 1.0, 'kinetic-drive': -1.0}
    dominant_values = {'corePriorities': [], 'growthAreas': [], 'alignmentNeeds': [], 'energyPatterns': []}
    adjusted_scores, influences = apply_mastery_influences(numeric_scores, dominant_values)
    assert adjusted_scores == numeric_scores # Scores should be unchanged
    assert influences == {'cognitive-alignment': [], 'kinetic-drive': []} # No influences applied

def test_apply_mastery_influences_single_influence():
    """Test applying a single mastery influence."""
    numeric_scores = {'resonance-field': 0.0, 'perceptual-focus': 0.0}
    # 'creative-expression' influences resonance-field +0.5, perceptual-focus +0.5
    dominant_values = {'corePriorities': ['creative-expression'], 'growthAreas': [], 'alignmentNeeds': [], 'energyPatterns': []}
    expected_scores = {'resonance-field': 0.5, 'perceptual-focus': 0.5}
    adjusted_scores, influences = apply_mastery_influences(numeric_scores, dominant_values)
    assert adjusted_scores == expected_scores
    assert influences['resonance-field'] == [{'category': 'corePriorities', 'value': 'creative-expression', 'influence': 0.5}]
    assert influences['perceptual-focus'] == [{'category': 'corePriorities', 'value': 'creative-expression', 'influence': 0.5}]

def test_apply_mastery_influences_multiple_influences():
    """Test applying multiple influences from different categories."""
    numeric_scores = {'kinetic-drive': 0.0, 'manifestation-rhythm': 0.0, 'perceptual-focus': 0.0}
    # 'craft-mastery': kinetic-drive -0.5, manifestation-rhythm -0.3
    # 'consistency-challenge': manifestation-rhythm -0.3, kinetic-drive -0.3
    dominant_values = {
        'corePriorities': ['craft-mastery'],
        'growthAreas': ['consistency-challenge'],
        'alignmentNeeds': [],
        'energyPatterns': []
    }
    expected_scores = {'kinetic-drive': -0.8, 'manifestation-rhythm': -0.6, 'perceptual-focus': 0.0}
    adjusted_scores, influences = apply_mastery_influences(numeric_scores, dominant_values)
    assert adjusted_scores == expected_scores
    # Check influences (order within list doesn't strictly matter)
    assert len(influences['kinetic-drive']) == 2
    assert len(influences['manifestation-rhythm']) == 2
    assert len(influences['perceptual-focus']) == 0

def test_apply_mastery_influences_capping_positive():
    """Test score capping at +2.0."""
    numeric_scores = {'resonance-field': 1.8}
    # 'creative-expression': resonance-field +0.5 -> potential 2.3
    dominant_values = {'corePriorities': ['creative-expression'], 'growthAreas': [], 'alignmentNeeds': [], 'energyPatterns': []}
    expected_scores = {'resonance-field': 2.0} # Should be capped
    adjusted_scores, _ = apply_mastery_influences(numeric_scores, dominant_values)
    assert adjusted_scores == expected_scores

def test_apply_mastery_influences_capping_negative():
    """Test score capping at -2.0."""
    numeric_scores = {'kinetic-drive': -1.7}
    # 'craft-mastery': kinetic-drive -0.5 -> potential -2.2
    dominant_values = {'corePriorities': ['craft-mastery'], 'growthAreas': [], 'alignmentNeeds': [], 'energyPatterns': []}
    expected_scores = {'kinetic-drive': -2.0} # Should be capped
    adjusted_scores, _ = apply_mastery_influences(numeric_scores, dominant_values)
    assert adjusted_scores == expected_scores

def test_apply_mastery_influences_value_not_in_map():
    """Test when a dominant value has no defined influences."""
    numeric_scores = {'cognitive-alignment': 0.0}
    dominant_values = {'corePriorities': ['unknown-value'], 'growthAreas': [], 'alignmentNeeds': [], 'energyPatterns': []}
    adjusted_scores, influences = apply_mastery_influences(numeric_scores, dominant_values)
    assert adjusted_scores == numeric_scores # Score unchanged
    assert influences == {'cognitive-alignment': []} # No influences

# Test cases for calculate_typology_scores
def test_calculate_typology_scores_empty_input():
    """Test with no typology responses."""
    result = calculate_typology_scores({})
    assert result == {
        "scores": {}, "placements": {}, "numericScores": {},
        "originalNumericScores": {}, "masteryInfluences": {}
    }

def test_calculate_typology_scores_all_left_no_mastery():
    """Test all 'left' responses without mastery influence."""
    responses = generate_full_typology_responses('left')
    result = calculate_typology_scores(responses)

    assert len(result['scores']) == len(TYPOLOGY_SPECTRUMS)
    for spectrum_id in result['scores']:
        assert result['scores'][spectrum_id] == {'left': 2, 'balanced': 0, 'right': 0}
        assert result['numericScores'][spectrum_id] == -2.0
        assert result['originalNumericScores'][spectrum_id] == -2.0
        assert result['placements'][spectrum_id] == 'strongLeft'
    assert result['masteryInfluences'] == {} # No mastery applied

def test_calculate_typology_scores_all_right_no_mastery():
    """Test all 'right' responses without mastery influence."""
    responses = generate_full_typology_responses('right')
    result = calculate_typology_scores(responses)

    for spectrum_id in result['scores']:
        assert result['scores'][spectrum_id] == {'left': 0, 'balanced': 0, 'right': 2}
        assert result['numericScores'][spectrum_id] == 2.0
        assert result['originalNumericScores'][spectrum_id] == 2.0
        assert result['placements'][spectrum_id] == 'strongRight'
    assert result['masteryInfluences'] == {}

def test_calculate_typology_scores_all_balanced_no_mastery():
    """Test all 'balanced' responses without mastery influence."""
    responses = generate_full_typology_responses('balanced')
    result = calculate_typology_scores(responses)

    for spectrum_id in result['scores']:
        assert result['scores'][spectrum_id] == {'left': 0, 'balanced': 2, 'right': 0}
        assert result['numericScores'][spectrum_id] == 0.0
        assert result['originalNumericScores'][spectrum_id] == 0.0
        assert result['placements'][spectrum_id] == 'balanced'
    assert result['masteryInfluences'] == {}

def test_calculate_typology_scores_mixed_no_mastery():
    """Test mixed responses leading to different placements."""
    responses = {
        'cognitive-q1': 'left', 'cognitive-q2': 'left',         # Score -2.0 -> strongLeft
        'perceptual-q1': 'right', 'perceptual-q2': 'right',       # Score +2.0 -> strongRight
        'kinetic-q1': 'left', 'kinetic-q2': 'balanced',       # Score -1.0 -> leftLeaning
        'choice-q1': 'right', 'choice-q2': 'balanced',        # Score +1.0 -> rightLeaning
        'resonance-q1': 'balanced', 'resonance-q2': 'balanced', # Score 0.0 -> balanced
        'rhythm-q1': 'left', 'rhythm-q2': 'right'             # Score 0.0 -> balanced
    }
    result = calculate_typology_scores(responses)

    assert result['placements']['cognitive-alignment'] == 'strongLeft'
    assert result['numericScores']['cognitive-alignment'] == -2.0
    assert result['placements']['perceptual-focus'] == 'strongRight'
    assert result['numericScores']['perceptual-focus'] == 2.0
    assert result['placements']['kinetic-drive'] == 'leftLeaning'
    assert result['numericScores']['kinetic-drive'] == -1.0
    assert result['placements']['choice-navigation'] == 'rightLeaning'
    assert result['numericScores']['choice-navigation'] == 1.0
    assert result['placements']['resonance-field'] == 'balanced'
    assert result['numericScores']['resonance-field'] == 0.0
    assert result['placements']['manifestation-rhythm'] == 'balanced'
    assert result['numericScores']['manifestation-rhythm'] == 0.0
    assert result['masteryInfluences'] == {}

def test_calculate_typology_scores_with_mastery_influence():
    """Test that mastery influences adjust scores and potentially placements."""
    typology_responses = {
        'cognitive-q1': 'balanced', 'cognitive-q2': 'balanced', # Original score 0.0
        'resonance-q1': 'balanced', 'resonance-q2': 'balanced',  # Original score 0.0
        'perceptual-q1': 'balanced', 'perceptual-q2': 'balanced' # Original score 0.0
    }
    # Add other responses to make it complete, assume balanced for simplicity
    for spectrum in TYPOLOGY_SPECTRUMS:
         if spectrum['id'] not in ['cognitive-alignment', 'resonance-field', 'perceptual-focus']:
             for question in spectrum['questions']:
                 typology_responses[question['id']] = 'balanced'

    mastery_responses = {
        # 'creative-expression': resonance-field +0.5, perceptual-focus +0.5
        # 'emotional-fulfillment': resonance-field +0.5, cognitive-alignment +0.3
        'core-q1': 'creative-expression',
        'core-q2': 'emotional-fulfillment'
        # Only need one dominant value per category for this test
    }

    result = calculate_typology_scores(typology_responses, mastery_responses)

    # Check cognitive-alignment: original 0.0 + influence 0.3 = 0.3 -> balanced
    assert result['originalNumericScores']['cognitive-alignment'] == 0.0
    assert result['numericScores']['cognitive-alignment'] == 0.3
    assert result['placements']['cognitive-alignment'] == 'balanced'
    assert len(result['masteryInfluences']['cognitive-alignment']) == 1
    assert result['masteryInfluences']['cognitive-alignment'][0]['value'] == 'emotional-fulfillment'

    # Check resonance-field: original 0.0 + influence 0.5 + influence 0.5 = 1.0 -> rightLeaning
    assert result['originalNumericScores']['resonance-field'] == 0.0
    assert result['numericScores']['resonance-field'] == 1.0
    assert result['placements']['resonance-field'] == 'rightLeaning'
    assert len(result['masteryInfluences']['resonance-field']) == 2

    # Check perceptual-focus: original 0.0 + influence 0.5 = 0.5 -> balanced
    assert result['originalNumericScores']['perceptual-focus'] == 0.0
    assert result['numericScores']['perceptual-focus'] == 0.5
    assert result['placements']['perceptual-focus'] == 'balanced'
    assert len(result['masteryInfluences']['perceptual-focus']) == 1

# Test cases for calculate_mastery_alignment_score
def test_calculate_mastery_alignment_score_no_alignment():
    """Test when dominant values don't align with the spectrum/placement."""
    placement = 'leftLeaning' # structured side
    spectrum_id = 'cognitive-alignment'
    dominant_values = {'growthAreas': ['some-other-value']}
    score = calculate_mastery_alignment_score(spectrum_id, placement, dominant_values)
    assert score == 0.0

def test_calculate_mastery_alignment_score_single_alignment():
    """Test alignment with one relevant dominant value."""
    placement = 'leftLeaning' # structured side
    spectrum_id = 'cognitive-alignment'
    # 'clarity-challenge' aligns with cognitive-alignment-structured
    dominant_values = {'growthAreas': ['clarity-challenge']}
    score = calculate_mastery_alignment_score(spectrum_id, placement, dominant_values)
    assert score == 1.0

def test_calculate_mastery_alignment_score_strong_placement_bonus():
    """Test alignment with bonus for strong placement."""
    placement = 'strongLeft' # structured side, strong
    spectrum_id = 'cognitive-alignment'
    # 'clarity-challenge' aligns with cognitive-alignment-structured
    dominant_values = {'growthAreas': ['clarity-challenge']}
    score = calculate_mastery_alignment_score(spectrum_id, placement, dominant_values)
    assert score == 1.5 # 1.0 base + 0.5 bonus

def test_calculate_mastery_alignment_score_multiple_alignments():
    """Test alignment with multiple relevant dominant values."""
    placement = 'strongRight' # fluid side, strong
    spectrum_id = 'manifestation-rhythm'
    # 'burnout-pattern' and 'accept-cycles' align with manifestation-rhythm-fluid
    dominant_values = {'growthAreas': ['burnout-pattern'], 'alignmentNeeds': ['accept-cycles']}
    score = calculate_mastery_alignment_score(spectrum_id, placement, dominant_values)
    # Base scores are 1.0 each. Strong bonus is 0.5 each. Total = (1.0+0.5) + (1.0+0.5) = 3.0
    assert score == 3.0

def test_calculate_mastery_alignment_score_no_dominant_values():
    """Test when dominant_values is empty."""
    placement = 'balanced'
    spectrum_id = 'kinetic-drive'
    dominant_values = {} # Empty dict
    score = calculate_mastery_alignment_score(spectrum_id, placement, dominant_values)
    assert score == 0.0

def test_calculate_mastery_alignment_score_dominant_values_none():
    """Test when dominant_values is None."""
    placement = 'balanced'
    spectrum_id = 'kinetic-drive'
    dominant_values = None # None object
    score = calculate_mastery_alignment_score(spectrum_id, placement, dominant_values)
    assert score == 0.0

# Test cases for determine_typology_pair
def test_determine_typology_pair_all_balanced():
    """Test when all spectrum placements are balanced."""
    placements = {sid: 'balanced' for sid in SPECTRUM_PRIORITY_ORDER}
    result = determine_typology_pair(placements)
    assert result == {
        "key": "balanced-balanced",
        "primary": {"spectrumId": SPECTRUM_PRIORITY_ORDER[0], "placement": 'balanced'},
        "secondary": {"spectrumId": SPECTRUM_PRIORITY_ORDER[1], "placement": 'balanced'}
    }

def test_determine_typology_pair_one_clear_placement_left():
    """Test with one clear 'leftLeaning' placement."""
    placements = {sid: 'balanced' for sid in SPECTRUM_PRIORITY_ORDER}
    placements['kinetic-drive'] = 'leftLeaning' # Structured
    result = determine_typology_pair(placements)
    # Should be structured-balanced, primary is kinetic-drive
    assert result['key'] == 'structured-balanced'
    assert result['primary']['spectrumId'] == 'kinetic-drive'
    assert result['primary']['placement'] == 'leftLeaning'
    assert result['secondary']['placement'] == 'balanced'
    # Secondary should be the highest priority balanced one (cognitive-alignment)
    assert result['secondary']['spectrumId'] == 'cognitive-alignment'


def test_determine_typology_pair_one_clear_placement_right():
    """Test with one clear 'strongRight' placement."""
    placements = {sid: 'balanced' for sid in SPECTRUM_PRIORITY_ORDER}
    placements['resonance-field'] = 'strongRight' # Fluid
    result = determine_typology_pair(placements)
    # Should be balanced-fluid, primary is resonance-field
    assert result['key'] == 'balanced-strongly-fluid'
    assert result['primary']['spectrumId'] == 'resonance-field'
    assert result['primary']['placement'] == 'strongRight'
    assert result['secondary']['placement'] == 'balanced'
    # Secondary should be the highest priority balanced one (cognitive-alignment)
    assert result['secondary']['spectrumId'] == 'cognitive-alignment'

def test_determine_typology_pair_two_clear_structured_fluid():
    """Test with two clear placements, one structured, one fluid."""
    placements = {sid: 'balanced' for sid in SPECTRUM_PRIORITY_ORDER}
    placements['cognitive-alignment'] = 'strongLeft' # Structured, higher priority
    placements['resonance-field'] = 'rightLeaning' # Fluid, lower priority
    result = determine_typology_pair(placements)
    # Key should be structured-fluid
    assert result['key'] == 'strongly-structured-fluid'
    assert result['primary']['spectrumId'] == 'cognitive-alignment'
    assert result['secondary']['spectrumId'] == 'resonance-field'

def test_determine_typology_pair_two_clear_fluid_structured_swap():
    """Test with two clear placements, fluid higher priority, should swap."""
    placements = {sid: 'balanced' for sid in SPECTRUM_PRIORITY_ORDER}
    placements['cognitive-alignment'] = 'rightLeaning' # Fluid, higher priority
    placements['manifestation-rhythm'] = 'leftLeaning' # Structured, lower priority
    result = determine_typology_pair(placements)
    # Key should be structured-fluid (swapped)
    assert result['key'] == 'structured-fluid'
    assert result['primary']['spectrumId'] == 'manifestation-rhythm' # Swapped
    assert result['secondary']['spectrumId'] == 'cognitive-alignment' # Swapped

def test_determine_typology_pair_two_clear_both_structured():
    """Test with two clear structured placements."""
    placements = {sid: 'balanced' for sid in SPECTRUM_PRIORITY_ORDER}
    placements['cognitive-alignment'] = 'strongLeft' # Higher priority
    placements['kinetic-drive'] = 'leftLeaning'    # Lower priority
    result = determine_typology_pair(placements)
    # Key should be structured-structured
    assert result['key'] == 'strongly-structured-structured'
    assert result['primary']['spectrumId'] == 'cognitive-alignment'
    assert result['secondary']['spectrumId'] == 'kinetic-drive'

def test_determine_typology_pair_two_clear_both_fluid():
    """Test with two clear fluid placements."""
    placements = {sid: 'balanced' for sid in SPECTRUM_PRIORITY_ORDER}
    placements['perceptual-focus'] = 'rightLeaning' # Higher priority
    placements['resonance-field'] = 'strongRight' # Lower priority
    result = determine_typology_pair(placements)
    # Key should be fluid-fluid
    assert result['key'] == 'fluid-strongly-fluid'
    assert result['primary']['spectrumId'] == 'perceptual-focus'
    assert result['secondary']['spectrumId'] == 'resonance-field'

def test_determine_typology_pair_with_mastery_influence():
    """Test that mastery values are passed through (though alignment logic is simple)."""
    placements = {sid: 'balanced' for sid in SPECTRUM_PRIORITY_ORDER}
    placements['cognitive-alignment'] = 'strongLeft'
    placements['kinetic-drive'] = 'leftLeaning'
    # Dominant values that align with cognitive-alignment-structured
    dominant_values = {'growthAreas': ['clarity-challenge']}
    # Expect same result as without mastery, as scoring difference is large
    result = determine_typology_pair(placements, dominant_values)
    assert result['key'] == 'strongly-structured-structured'
    assert result['primary']['spectrumId'] == 'cognitive-alignment'
    assert result['secondary']['spectrumId'] == 'kinetic-drive'

# Test cases for calculate_energy_focus_scores
def test_calculate_energy_focus_scores_no_typology():
    """Test when no typology responses are provided."""
    result = calculate_energy_focus_scores({})
    assert result == {"expansionScore": 50, "contractionScore": 50}

def test_calculate_energy_focus_scores_all_left():
    """Test with all left responses (max contraction)."""
    # All left gives numeric score of -2.0 for each of 6 spectrums = -12.0 total
    # Normalized: -12.0 / 12.0 = -1.0
    # Expansion % = (-1.0 + 1) / 2 * 100 = 0%
    responses = generate_full_typology_responses('left')
    result = calculate_energy_focus_scores(responses)
    assert result == {"expansionScore": 0, "contractionScore": 100}

def test_calculate_energy_focus_scores_all_right():
    """Test with all right responses (max expansion)."""
    # All right gives numeric score of +2.0 for each of 6 spectrums = +12.0 total
    # Normalized: +12.0 / 12.0 = +1.0
    # Expansion % = (+1.0 + 1) / 2 * 100 = 100%
    responses = generate_full_typology_responses('right')
    result = calculate_energy_focus_scores(responses)
    assert result == {"expansionScore": 100, "contractionScore": 0}

def test_calculate_energy_focus_scores_all_balanced():
    """Test with all balanced responses (perfect balance)."""
    # All balanced gives numeric score of 0.0 for each = 0.0 total
    # Normalized: 0.0 / 12.0 = 0.0
    # Expansion % = (0.0 + 1) / 2 * 100 = 50%
    responses = generate_full_typology_responses('balanced')
    result = calculate_energy_focus_scores(responses)
    assert result == {"expansionScore": 50, "contractionScore": 50}

def test_calculate_energy_focus_scores_mixed():
    """Test with mixed responses."""
    # Using the mixed responses from a previous test:
    # Scores: Cog=-2, Per=+2, Kin=-1, Cho=+1, Res=0, Rhy=0
    # Total = -2 + 2 - 1 + 1 + 0 + 0 = 0.0
    # Should result in 50/50
    responses = {
        'cognitive-q1': 'left', 'cognitive-q2': 'left',
        'perceptual-q1': 'right', 'perceptual-q2': 'right',
        'kinetic-q1': 'left', 'kinetic-q2': 'balanced',
        'choice-q1': 'right', 'choice-q2': 'balanced',
        'resonance-q1': 'balanced', 'resonance-q2': 'balanced',
        'rhythm-q1': 'left', 'rhythm-q2': 'right'
    }
    result = calculate_energy_focus_scores(responses)
    assert result == {"expansionScore": 50, "contractionScore": 50}

def test_calculate_energy_focus_scores_with_mastery_shift():
    """Test where mastery significantly shifts the balance."""
    # Start with all balanced (total score 0.0)
    typology_responses = generate_full_typology_responses('balanced')
    # Add mastery that pushes strongly right (expansion)
    mastery_responses = {
        # 'passion-inspiration': resonance +0.5, kinetic +0.5
        # 'joy-excitement': kinetic +0.5, resonance +0.5
        # 'intuitive-instincts': cognitive +0.5, choice +0.5
        # 'accept-cycles': rhythm +0.5, kinetic +0.3
        # 'accept-intuition': cognitive +0.5, choice +0.5
        # 'accept-flexibility': perceptual +0.5, rhythm +0.5
        'core-q1': 'passion-inspiration', 'core-q2': 'joy-excitement',
        'energy-q1': 'intuitive-instincts',
        'alignment-q1': 'accept-cycles', 'alignment-q2': 'accept-intuition', 'alignment-q3': 'accept-flexibility' # Assuming alignment-q3 exists for test
    }
    # Expected influences sum:
    # cog: 0.5 + 0.5 = 1.0
    # per: 0.5
    # kin: 0.5 + 0.5 + 0.3 = 1.3
    # cho: 0.5 + 0.5 = 1.0
    # res: 0.5 + 0.5 = 1.0
    # rhy: 0.5 + 0.5 = 1.0
    # Total score = 0 (original) + 1.0 + 0.5 + 1.3 + 1.0 + 1.0 + 1.0 = 5.8
    # Normalized = 5.8 / 12.0 = 0.4833...
    # Expansion % = (0.4833 + 1) / 2 * 100 = 74.166... -> rounded 74
    result = calculate_energy_focus_scores(typology_responses, mastery_responses)
    assert result == {"expansionScore": 74, "contractionScore": 26}


# Test cases for generate_complete_results
def test_generate_complete_results_typology_only():
    """Test generating results with only typology responses."""
    typology_responses = generate_full_typology_responses('balanced') # All balanced
    result = generate_complete_results(typology_responses)

    assert "typologyResults" in result
    assert "spectrumPlacements" in result
    assert "numericScores" in result
    assert "typologyPair" in result
    assert "masteryScores" in result
    assert "dominantValues" in result
    assert "masteryInfluences" in result
    assert "energyFocus" in result

    # Check some basic consistency
    assert result['spectrumPlacements']['cognitive-alignment'] == 'balanced'
    assert result['typologyPair']['key'] == 'balanced-balanced'
    assert result['masteryScores'] == {'corePriorities': {}, 'growthAreas': {}, 'alignmentNeeds': {}, 'energyPatterns': {}}
    assert result['dominantValues'] == {'corePriorities': [], 'growthAreas': [], 'alignmentNeeds': [], 'energyPatterns': []}
    assert result['energyFocus'] == {"expansionScore": 50, "contractionScore": 50}
    assert result['masteryInfluences'] == {} # No mastery influences applied

def test_generate_complete_results_with_mastery():
    """Test generating results with both typology and mastery responses."""
    typology_responses = { # Mixed typology
        'cognitive-q1': 'left', 'cognitive-q2': 'left',
        'perceptual-q1': 'right', 'perceptual-q2': 'right',
        'kinetic-q1': 'left', 'kinetic-q2': 'balanced',
        'choice-q1': 'right', 'choice-q2': 'balanced',
        'resonance-q1': 'balanced', 'resonance-q2': 'balanced',
        'rhythm-q1': 'left', 'rhythm-q2': 'right'
    }
    mastery_responses = { # Some mastery
        'core-q1': 'creative-expression',
        'growth-q1': 'consistency-challenge',
    }
    result = generate_complete_results(typology_responses, mastery_responses)

    assert "typologyResults" in result
    assert "spectrumPlacements" in result
    assert "numericScores" in result
    assert "typologyPair" in result
    assert "masteryScores" in result
    assert "dominantValues" in result
    assert "masteryInfluences" in result
    assert "energyFocus" in result

    # Check some basic consistency
    # Placements might change due to mastery, check a known one
    assert 'cognitive-alignment' in result['spectrumPlacements']
    # Mastery scores should reflect input
    assert result['masteryScores']['corePriorities'] == {'creative-expression': 1}
    assert result['masteryScores']['growthAreas'] == {'consistency-challenge': 1}
    # Dominant values should reflect scores
    assert result['dominantValues']['corePriorities'] == ['creative-expression']
    assert result['dominantValues']['growthAreas'] == ['consistency-challenge']
    # Mastery influences should exist for affected spectrums
    assert len(result['masteryInfluences'].get('resonance-field', [])) > 0 # creative-expression affects resonance
    assert len(result['masteryInfluences'].get('manifestation-rhythm', [])) > 0 # consistency-challenge affects rhythm

def test_generate_complete_results_empty_inputs():
    """Test generating results with empty typology and mastery responses."""
    result = generate_complete_results({}, {})

    assert "typologyResults" in result
    assert "spectrumPlacements" in result
    assert "numericScores" in result
    assert "typologyPair" in result
    assert "masteryScores" in result
    assert "dominantValues" in result
    assert "masteryInfluences" in result
    assert "energyFocus" in result

    # Check for empty/default values
    assert result['typologyResults']['scores'] == {}
    assert result['spectrumPlacements'] == {}
    assert result['numericScores'] == {}
    # Typology pair defaults even with no placements
    assert result['typologyPair']['key'] == 'balanced-balanced'
    assert result['masteryScores'] == {'corePriorities': {}, 'growthAreas': {}, 'alignmentNeeds': {}, 'energyPatterns': {}}
    assert result['dominantValues'] == {'corePriorities': [], 'growthAreas': [], 'alignmentNeeds': [], 'energyPatterns': []}
    assert result['energyFocus'] == {"expansionScore": 50, "contractionScore": 50}
    assert result['masteryInfluences'] == {}