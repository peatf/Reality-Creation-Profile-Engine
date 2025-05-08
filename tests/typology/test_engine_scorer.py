import pytest
from services.typology_engine.engine import TypologyEngine

# Use the actual config for more realistic scoring tests
VALID_CONFIG_PATH = "assets/typology_questions.yml"

@pytest.fixture(scope="module")
def engine():
    """Provides a TypologyEngine instance loaded with the valid config."""
    try:
        return TypologyEngine(config_path=VALID_CONFIG_PATH)
    except Exception as e:
        pytest.fail(f"Failed to initialize TypologyEngine: {e}")

@pytest.fixture
def sample_answers_balanced(engine):
    """Provides a sample set of answers leaning towards balanced (0)."""
    answers = {}
    for s in engine.config.spectrums:
        for q in s.questions:
            answers[q.id] = 0 # All balanced
    for d in engine.config.mastery_dimensions:
        for q in d.questions:
            if q.answers:
                answers[q.id] = q.answers[0].value # Choose first mastery answer
    return answers

@pytest.fixture
def sample_answers_left_leaning(engine):
    """Provides a sample set of answers leaning left (-1)."""
    answers = {}
    for s in engine.config.spectrums:
        for q in s.questions:
            answers[q.id] = -1 # All left
    for d in engine.config.mastery_dimensions:
        for q in d.questions:
             # Choose specific mastery answers known to influence scores negatively or towards structure
            if q.id == 'core_q1':
                 answers[q.id] = 'financial_abundance' # Influences perceptual (-0.5), kinetic (-0.3)
            elif q.id == 'growth_q1':
                 answers[q.id] = 'consistency_challenge' # Influences rhythm (-0.3), kinetic (-0.3)
            elif q.answers:
                 answers[q.id] = q.answers[0].value
            else:
                 answers[q.id] = 'dummy_value' # Placeholder if no answers defined
    return answers

@pytest.fixture
def sample_answers_right_leaning(engine):
    """Provides a sample set of answers leaning right (+1)."""
    answers = {}
    for s in engine.config.spectrums:
        for q in s.questions:
            answers[q.id] = 1 # All right
    for d in engine.config.mastery_dimensions:
        for q in d.questions:
             # Choose specific mastery answers known to influence scores positively or towards fluidity
            if q.id == 'core_q1':
                 answers[q.id] = 'creative_expression' # Influences resonance (+0.5), perceptual (+0.5)
            elif q.id == 'growth_q1':
                 answers[q.id] = 'receiving_challenge' # Influences perceptual (+0.5), rhythm (+0.3)
            elif q.answers:
                 answers[q.id] = q.answers[-1].value # Choose last mastery answer
            else:
                 answers[q.id] = 'dummy_value' # Placeholder if no answers defined
    return answers

# --- Scoring Logic Tests ---

def test_compute_initial_scores_balanced(engine, sample_answers_balanced):
    """Test initial score calculation with all balanced answers."""
    spectrum_answers = {
        qid: val for qid, val in sample_answers_balanced.items()
        if qid in engine.spectrum_questions
    }
    initial_scores = engine._compute_initial_spectrum_scores(spectrum_answers)
    for spec_id in initial_scores:
        assert initial_scores[spec_id] == 0.0

def test_compute_initial_scores_left(engine, sample_answers_left_leaning):
    """Test initial score calculation with all left answers."""
    spectrum_answers = {
        qid: val for qid, val in sample_answers_left_leaning.items()
        if qid in engine.spectrum_questions
    }
    initial_scores = engine._compute_initial_spectrum_scores(spectrum_answers)
    for spectrum in engine.config.spectrums:
        assert initial_scores[spectrum.id] == -len(spectrum.questions) # Should be -3 for most

def test_compute_initial_scores_right(engine, sample_answers_right_leaning):
    """Test initial score calculation with all right answers."""
    spectrum_answers = {
        qid: val for qid, val in sample_answers_right_leaning.items()
        if qid in engine.spectrum_questions
    }
    initial_scores = engine._compute_initial_spectrum_scores(spectrum_answers)
    for spectrum in engine.config.spectrums:
        assert initial_scores[spectrum.id] == len(spectrum.questions) # Should be +3 for most

def test_apply_mastery_influence(engine, sample_answers_left_leaning):
    """Test that mastery influence correctly adjusts scores."""
    spectrum_answers = {
        qid: val for qid, val in sample_answers_left_leaning.items()
        if qid in engine.spectrum_questions
    }
    mastery_answers = {
        qid: val for qid, val in sample_answers_left_leaning.items()
        if qid in engine.mastery_questions
    }
    initial_scores = engine._compute_initial_spectrum_scores(spectrum_answers)
    initial_kinetic = initial_scores['kinetic_drive'] # Example: -3.0
    initial_perceptual = initial_scores['perceptual_focus'] # Example: -3.0
    initial_rhythm = initial_scores['manifestation_rhythm'] # Example: -3.0

    adjusted_scores = engine._apply_mastery_influence(initial_scores, mastery_answers)

    # Check influenced scores based on sample_answers_left_leaning fixture choices
    # financial_abundance: perceptual (-0.5), kinetic (-0.3)
    # consistency_challenge: rhythm (-0.3), kinetic (-0.3)
    # Total kinetic influence: -0.3 + -0.3 = -0.6
    # Total perceptual influence: -0.5
    # Total rhythm influence: -0.3
    assert adjusted_scores['kinetic_drive'] == pytest.approx(initial_kinetic - 0.6)
    assert adjusted_scores['perceptual_focus'] == pytest.approx(initial_perceptual - 0.5)
    assert adjusted_scores['manifestation_rhythm'] == pytest.approx(initial_rhythm - 0.3)
    # Check a non-influenced score remains the same
    assert adjusted_scores['cognitive_alignment'] == initial_scores['cognitive_alignment']

def test_rank_spectrums(engine):
    """Test spectrum ranking based on priority order."""
    # Scores designed to test priority vs. magnitude
    scores = {
        'cognitive_alignment': 1.0, # Priority 1
        'kinetic_drive': -2.0,      # Priority 2
        'choice_navigation': 3.0,   # Priority 3
        'perceptual_focus': -0.5,   # Priority 4
        'resonance_field': 1.5,     # Priority 5
        'manifestation_rhythm': -1.5 # Priority 6
    }
    ranked = engine._rank_spectrums(scores)
    ranked_ids = [item[0] for item in ranked]
    expected_order = engine.config.scoring_rules.spectrum_priority_order
    assert ranked_ids == expected_order

def test_determine_placements(engine):
    """Test score-to-placement mapping."""
    # Assuming 3 questions per spectrum (range -3 to +3)
    # Thresholds: strong=2.0, leaning=1.0
    scores = {
        'cognitive_alignment': -2.5, # strong_left -> strongly_structured
        'kinetic_drive': -1.5,      # left_leaning -> structured
        'choice_navigation': 0.0,   # balanced -> balanced
        'perceptual_focus': 1.8,    # right_leaning -> fluid
        'resonance_field': 3.0,     # strong_right -> strongly_fluid
        'manifestation_rhythm': -0.9 # balanced -> balanced
    }
    placements = engine._determine_placements(scores)
    placement_map = engine.config.scoring_rules.placement_mapping
    assert placements['cognitive_alignment'] == placement_map['strong_left']
    assert placements['kinetic_drive'] == placement_map['left_leaning']
    assert placements['choice_navigation'] == placement_map['balanced']
    assert placements['perceptual_focus'] == placement_map['right_leaning']
    assert placements['resonance_field'] == placement_map['strong_right']
    assert placements['manifestation_rhythm'] == placement_map['balanced']

def test_calculate_scores_output_structure(engine, sample_answers_balanced):
    """Test the overall structure of the calculate_scores output."""
    results = engine.calculate_scores(sample_answers_balanced)
    assert "initial_spectrum_scores" in results
    assert "adjusted_spectrum_scores" in results
    assert "ranked_spectrums" in results
    assert "spectrum_placements" in results
    assert "final_profile_id" in results
    assert "top_matching_typologies" in results
    assert "raw_answers" in results
    assert isinstance(results["initial_spectrum_scores"], dict)
    assert isinstance(results["adjusted_spectrum_scores"], dict)
    assert isinstance(results["ranked_spectrums"], list)
    assert isinstance(results["spectrum_placements"], dict)
    assert isinstance(results["final_profile_id"], str)
    assert isinstance(results["top_matching_typologies"], list)
    assert isinstance(results["raw_answers"], dict)
    # Check if top_matching_typologies contains profile dicts
    if results["top_matching_typologies"]:
        assert isinstance(results["top_matching_typologies"][0], dict)
        assert "id" in results["top_matching_typologies"][0]
        assert "typology_name" in results["top_matching_typologies"][0]

def test_calculate_scores_empty_answers(engine):
    """Test that calculating scores with empty answers raises ValueError."""
    with pytest.raises(ValueError, match="Answers dictionary cannot be empty"):
        engine.calculate_scores({})

def test_calculate_scores_invalid_spectrum_answer_value(engine, sample_answers_balanced):
    """Test that invalid spectrum answer values raise ValueError during calculation."""
    invalid_answers = sample_answers_balanced.copy()
    # Find a spectrum question ID and set an invalid value
    spec_q_id = list(engine.spectrum_questions.keys())[0]
    invalid_answers[spec_q_id] = 5 # Invalid value

    with pytest.raises(ValueError, match=f"Invalid answer value '5' for spectrum question '{spec_q_id}'"):
        engine.calculate_scores(invalid_answers)