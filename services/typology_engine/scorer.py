# src/assessment/scorer.py
# Handles scoring and evaluation for the Reality Creation Assessment.

import logging
import math
from typing import Dict, Any, List, Tuple, Optional

# Import definitions (assuming definitions.py is in the same directory)
from .definitions import TYPOLOGY_SPECTRUMS, MASTERY_SECTIONS

logger = logging.getLogger(__name__)

# --- Constants (ported from redesigned-scoring.js) ---

SPECTRUM_PRIORITY_ORDER = [
    'cognitive-alignment',   # How users mentally process reality
    'kinetic-drive',         # How users take action and generate momentum
    'choice-navigation',     # Decision-making style
    'perceptual-focus',      # Clarity and openness in manifestation
    'resonance-field',       # Emotional interaction in manifestation
    'manifestation-rhythm'   # Sustainability and adaptability over time
]

PLACEMENT_MAPPING = {
    'strongLeft': 'strongly-structured',  # Highly grounded, structured, logical, methodical
    'leftLeaning': 'structured',          # Leaning toward structure, logic, and method
    'left': 'structured',                 # Legacy support (maps to leaning)
    'balanced': 'balanced',               # Integrative and adaptive
    'right': 'fluid',                     # Legacy support (maps to leaning)
    'rightLeaning': 'fluid',              # Leaning toward intuition, flow, and spontaneity
    'strongRight': 'strongly-fluid'       # Highly expansive, intuitive, momentum-driven
}

# Mapping from mastery response values to spectrum influence values
# Positive values push towards 'fluid' (+1), negative towards 'structured' (-1)
MASTERY_SPECTRUM_INFLUENCE = {
    # Core Priorities influence on spectrums
    'corePriorities': {
        'creative-expression': {'resonance-field': 0.5, 'perceptual-focus': 0.5},
        'financial-abundance': {'perceptual-focus': -0.5, 'kinetic-drive': -0.3},
        'emotional-fulfillment': {'resonance-field': 0.5, 'cognitive-alignment': 0.3},
        'personal-autonomy': {'choice-navigation': 0.5, 'kinetic-drive': 0.3},
        'deep-relationships': {'resonance-field': 0.3, 'perceptual-focus': 0.3},
        'spiritual-connection': {'cognitive-alignment': 0.5, 'choice-navigation': 0.3},
        'craft-mastery': {'kinetic-drive': -0.5, 'manifestation-rhythm': -0.3},
        'wealth-security': {'manifestation-rhythm': -0.5, 'perceptual-focus': -0.3},
        'emotional-peace': {'resonance-field': -0.5, 'manifestation-rhythm': -0.3},
        'personal-freedom': {'manifestation-rhythm': 0.5, 'choice-navigation': 0.5},
        'deep-connection': {'resonance-field': 0.5, 'cognitive-alignment': 0.3},
        'higher-meaning': {'cognitive-alignment': 0.5, 'perceptual-focus': 0.3},
        'confidence-trust': {'choice-navigation': 0.5, 'cognitive-alignment': 0.3},
        'peace-ease': {'kinetic-drive': 0.3, 'manifestation-rhythm': 0.5},
        'choice-autonomy': {'choice-navigation': 0.5, 'cognitive-alignment': 0.3},
        'stability-security': {'manifestation-rhythm': -0.5, 'perceptual-focus': -0.5},
        'passion-inspiration': {'resonance-field': 0.5, 'kinetic-drive': 0.5},
        'joy-excitement': {'kinetic-drive': 0.5, 'resonance-field': 0.5}
    },
    # Growth Areas influence on spectrums
    'growthAreas': {
        'consistency-challenge': {'manifestation-rhythm': -0.3, 'kinetic-drive': -0.3},
        'clarity-challenge': {'perceptual-focus': -0.3, 'cognitive-alignment': -0.3},
        'action-challenge': {'kinetic-drive': -0.3, 'choice-navigation': -0.3},
        'intuition-challenge': {'cognitive-alignment': 0.5, 'choice-navigation': 0.3},
        'emotion-challenge': {'resonance-field': 0.5, 'cognitive-alignment': 0.3},
        'receiving-challenge': {'perceptual-focus': 0.5, 'manifestation-rhythm': 0.3},
        'decision-doubt': {'choice-navigation': -0.5, 'cognitive-alignment': -0.3},
        'action-gap': {'kinetic-drive': -0.5, 'choice-navigation': -0.3},
        'focus-challenge': {'perceptual-focus': -0.5, 'manifestation-rhythm': -0.3},
        'emotional-block': {'resonance-field': -0.5, 'cognitive-alignment': -0.3},
        'burnout-pattern': {'manifestation-rhythm': 0.5, 'kinetic-drive': 0.3},
        'commitment-hesitation': {'choice-navigation': 0.5, 'perceptual-focus': 0.3},
        'self-trust-resistance': {'cognitive-alignment': 0.5, 'choice-navigation': 0.3},
        'risk-resistance': {'choice-navigation': 0.5, 'kinetic-drive': 0.3},
        'emotional-expression-resistance': {'resonance-field': 0.5, 'cognitive-alignment': 0.3},
        'vision-clarity-resistance': {'perceptual-focus': -0.5, 'cognitive-alignment': -0.3},
        'momentum-resistance': {'kinetic-drive': 0.5, 'manifestation-rhythm': 0.3},
        'control-resistance': {'perceptual-focus': 0.5, 'choice-navigation': 0.5}
    },
    # Alignment Needs influence on spectrums
    'alignmentNeeds': {
        'accept-cycles': {'manifestation-rhythm': 0.5, 'kinetic-drive': 0.3},
        'accept-structure': {'kinetic-drive': -0.5, 'manifestation-rhythm': -0.3},
        'accept-emotions': {'resonance-field': 0.5, 'cognitive-alignment': 0.3},
        'accept-gradual-clarity': {'perceptual-focus': 0.5, 'manifestation-rhythm': 0.3},
        'accept-intuition': {'cognitive-alignment': 0.5, 'choice-navigation': 0.5},
        'accept-flexibility': {'perceptual-focus': 0.5, 'manifestation-rhythm': 0.5},
        'control-outcomes': {'perceptual-focus': -0.5, 'manifestation-rhythm': -0.5},
        'control-emotions': {'resonance-field': -0.5, 'cognitive-alignment': -0.3},
        'control-consistency': {'manifestation-rhythm': -0.5, 'kinetic-drive': -0.5},
        'control-clarity': {'perceptual-focus': -0.5, 'cognitive-alignment': -0.3},
        'control-decisions': {'choice-navigation': -0.5, 'cognitive-alignment': -0.3},
        'control-intuition': {'cognitive-alignment': -0.5, 'choice-navigation': -0.5}
    },
    # Energy Patterns influence on spectrums
    'energyPatterns': {
        'clear-instructions': {'perceptual-focus': -0.5, 'choice-navigation': -0.3},
        'intuitive-instincts': {'cognitive-alignment': 0.5, 'choice-navigation': 0.5},
        'emotional-inspiration': {'resonance-field': 0.5, 'cognitive-alignment': 0.3},
        'balanced-rhythm': {'manifestation-rhythm': 0, 'kinetic-drive': 0},
        'gradual-clarity': {'perceptual-focus': 0.5, 'cognitive-alignment': 0.3},
        'process-trust': {'choice-navigation': 0.5, 'manifestation-rhythm': 0.3},
        'rigid-routines': {'manifestation-rhythm': -0.5, 'kinetic-drive': -0.5},
        'ignored-intuition': {'cognitive-alignment': 0.5, 'choice-navigation': 0.3}, # Note: JS had 0.5, 0.3 - assuming typo fixed
        'structured-productivity': {'kinetic-drive': -0.5, 'perceptual-focus': -0.3},
        'spontaneous-productivity': {'kinetic-drive': 0.5, 'manifestation-rhythm': 0.3},
        'structured-environment': {'manifestation-rhythm': -0.5, 'perceptual-focus': -0.3},
        'dynamic-environment': {'manifestation-rhythm': 0.5, 'perceptual-focus': 0.3},
        # Adding missing patterns from JS energy-q2/q3/q4 for completeness, assuming neutral influence if not mapped
        'suppressed-emotions': {'resonance-field': -0.3}, # Guessing slight structured pull
        'forced-clarity': {'perceptual-focus': -0.3}, # Guessing slight structured pull
        'ignored-cycles': {'manifestation-rhythm': 0.5}, # Guessing fluid pull
        'overcontrolling': {'perceptual-focus': -0.5, 'choice-navigation': -0.5}, # Guessing structured pull
        'flexible-productivity': {'kinetic-drive': 0.5, 'manifestation-rhythm': 0.3}, # Guessing fluid pull
        'emotional-productivity': {'resonance-field': 0.5}, # Guessing fluid pull
        'adaptive-productivity': {'manifestation-rhythm': 0.5, 'kinetic-drive': 0.3}, # Guessing fluid pull
        'balanced-productivity': {'kinetic-drive': 0, 'manifestation-rhythm': 0}, # Neutral
        'emotionally-supportive-environment': {'resonance-field': 0.3}, # Guessing slight fluid pull
        'inspiring-environment': {'cognitive-alignment': 0.3, 'perceptual-focus': 0.3}, # Guessing slight fluid pull
        'balanced-environment': {'manifestation-rhythm': 0, 'perceptual-focus': 0}, # Neutral
        'pressure-free-environment': {'manifestation-rhythm': 0.5, 'kinetic-drive': 0.3} # Guessing fluid pull
    }
}

# --- Scoring Functions ---

def calculate_mastery_scores(mastery_responses: Dict[str, str]) -> Dict[str, Dict[str, int]]:
    """Calculates mastery scores from Part 2 responses."""
    if not mastery_responses:
        return {
            'corePriorities': {}, 'growthAreas': {},
            'alignmentNeeds': {}, 'energyPatterns': {}
        }

    result = {
        'corePriorities': {}, 'growthAreas': {},
        'alignmentNeeds': {}, 'energyPatterns': {}
    }

    for question_id, chosen_value in mastery_responses.items():
        if question_id.startswith("core-"):
            category = 'corePriorities'
        elif question_id.startswith("growth-"):
            category = 'growthAreas'
        elif question_id.startswith("alignment-"):
            category = 'alignmentNeeds'
        elif question_id.startswith("energy-"):
            category = 'energyPatterns'
        else:
            continue # Skip if question ID doesn't match known prefixes

        if chosen_value not in result[category]:
            result[category][chosen_value] = 0
        result[category][chosen_value] += 1

    return result

def determine_dominant_values(mastery_scores: Dict[str, Dict[str, int]]) -> Dict[str, List[str]]:
    """Determines dominant values from mastery scores."""
    dominant_values = {
        'corePriorities': [], 'growthAreas': [],
        'alignmentNeeds': [], 'energyPatterns': []
    }

    def get_dominant_values_for_category(score_category: Dict[str, int]) -> List[str]:
        if not score_category:
            return []
        max_score = max(score_category.values())
        # Return all values that have the maximum score
        return [value for value, score in score_category.items() if score == max_score]

    for category in dominant_values.keys():
        if category in mastery_scores:
            dominant_values[category] = get_dominant_values_for_category(mastery_scores[category])

    return dominant_values

def apply_mastery_influences(
    numeric_scores: Dict[str, float],
    dominant_values: Dict[str, List[str]]
) -> Tuple[Dict[str, float], Dict[str, List[Dict[str, Any]]]]:
    """Applies mastery influences to spectrum scores."""
    adjusted_scores = numeric_scores.copy()
    influences: Dict[str, List[Dict[str, Any]]] = {spectrum_id: [] for spectrum_id in numeric_scores}

    for category, influence_map_for_category in MASTERY_SPECTRUM_INFLUENCE.items():
        if category in dominant_values and dominant_values[category]:
            for value in dominant_values[category]:
                influence_map = influence_map_for_category.get(value, {})
                for spectrum_id, influence_value in influence_map.items():
                    if spectrum_id in adjusted_scores:
                        adjusted_scores[spectrum_id] += influence_value
                        influences[spectrum_id].append({
                            'category': category,
                            'value': value,
                            'influence': influence_value
                        })

    # Cap scores to maintain -2 to +2 range
    for spectrum_id in adjusted_scores:
        adjusted_scores[spectrum_id] = max(-2.0, min(2.0, adjusted_scores[spectrum_id]))

    return adjusted_scores, influences


def calculate_typology_scores(
    typology_responses: Dict[str, str],
    mastery_responses: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    """
    Calculates typology scores from Part 1 responses, optionally integrating mastery influences.
    """
    placements = {}
    scores = {} # Raw counts: {left: x, balanced: y, right: z}
    numeric_scores = {} # Score: -2 (strongLeft) to +2 (strongRight)
    original_numeric_scores = {} # Before mastery influence

    if not typology_responses:
         logger.warning("No typology responses provided.")
         return {
            "scores": {}, "placements": {}, "numericScores": {},
            "originalNumericScores": {}, "masteryInfluences": {}
         }

    for spectrum in TYPOLOGY_SPECTRUMS:
        spectrum_id = spectrum['id']
        question_ids = [q['id'] for q in spectrum['questions']]
        responses = [typology_responses.get(qid) for qid in question_ids]

        left_count = responses.count('left')
        balanced_count = responses.count('balanced')
        right_count = responses.count('right')

        scores[spectrum_id] = {
            'left': left_count,
            'balanced': balanced_count,
            'right': right_count
        }

        # Calculate numerical score: left=-1, balanced=0, right=+1
        numeric_score = (right_count * 1) + (left_count * -1)
        original_numeric_scores[spectrum_id] = float(numeric_score)
        numeric_scores[spectrum_id] = float(numeric_score) # Initialize with original

    # Apply mastery influences if mastery responses are provided
    mastery_influences = {}
    if mastery_responses:
        mastery_scores = calculate_mastery_scores(mastery_responses)
        dominant_values = determine_dominant_values(mastery_scores)
        adjusted_scores, influences = apply_mastery_influences(original_numeric_scores, dominant_values)
        numeric_scores = adjusted_scores # Update with adjusted scores
        mastery_influences = influences

    # Determine placements based on the final (potentially adjusted) numeric scores
    for spectrum_id, score in numeric_scores.items():
        if score <= -1.5: # Adjusted threshold for strong
            placement = 'strongLeft'
        elif score < -0.5:
            placement = 'leftLeaning'
        elif score <= 0.5:
            placement = 'balanced'
        elif score < 1.5:
            placement = 'rightLeaning'
        else: # score >= 1.5
            placement = 'strongRight'
        placements[spectrum_id] = placement

    logger.debug(f"Calculated numeric scores (final): {numeric_scores}")
    return {
        "scores": scores,
        "placements": placements,
        "numericScores": numeric_scores,
        "originalNumericScores": original_numeric_scores,
        "masteryInfluences": mastery_influences
    }

def calculate_mastery_alignment_score(
    spectrum_id: str,
    placement: str,
    dominant_values: Dict[str, List[str]]
) -> float:
    """Helper function to calculate how well a spectrum aligns with mastery priorities."""
    score = 0.0
    # Simplified mapping for demonstration - real mapping would be complex
    alignment_map = {
        'cognitive-alignment-structured': ['clarity-challenge', 'control-clarity'],
        'cognitive-alignment-fluid': ['intuition-challenge', 'accept-intuition'],
        'perceptual-focus-structured': ['clarity-challenge', 'control-clarity'],
        'perceptual-focus-fluid': ['receiving-challenge', 'accept-flexibility'],
        'kinetic-drive-structured': ['consistency-challenge', 'accept-structure'],
        'kinetic-drive-fluid': ['action-challenge', 'accept-cycles'],
        'choice-navigation-structured': ['decision-doubt', 'control-decisions'],
        'choice-navigation-fluid': ['intuition-challenge', 'accept-intuition'],
        'resonance-field-structured': ['emotion-challenge', 'control-emotions'],
        'resonance-field-fluid': ['emotion-challenge', 'accept-emotions'],
        'manifestation-rhythm-structured': ['consistency-challenge', 'control-consistency'],
        'manifestation-rhythm-fluid': ['burnout-pattern', 'accept-cycles'],
    }

    # Determine if placement is structured or fluid side
    side = 'balanced'
    if 'Left' in placement or 'structured' in PLACEMENT_MAPPING.get(placement, ''):
        side = 'structured'
    elif 'Right' in placement or 'fluid' in PLACEMENT_MAPPING.get(placement, ''):
        side = 'fluid'

    key = f"{spectrum_id}-{side}"
    relevant_priorities = alignment_map.get(key, [])

    if dominant_values: # Add check for None or empty dict
        for category, values in dominant_values.items():
            if values:
                for priority in values:
                    if priority in relevant_priorities:
                        score += 1.0 # Basic score for alignment
                        if 'strong' in placement:
                            score += 0.5 # Bonus for strong placement alignment
    return score


def determine_typology_pair(
    spectrum_placements: Dict[str, str],
    dominant_values: Optional[Dict[str, List[str]]] = None
) -> Dict[str, Any]:
    """Determines typology pair based on spectrum placements and optional mastery values."""
    clear_placements = {sid: p for sid, p in spectrum_placements.items() if p != 'balanced'}
    clear_spectrums = list(clear_placements.keys())

    if len(clear_spectrums) >= 2:
        spectrum_scores = {}
        for spectrum_id in clear_spectrums:
            foundational_score = len(SPECTRUM_PRIORITY_ORDER) - SPECTRUM_PRIORITY_ORDER.index(spectrum_id)
            placement_score = 2 if 'strong' in spectrum_placements[spectrum_id] else 1
            mastery_alignment = calculate_mastery_alignment_score(spectrum_id, spectrum_placements[spectrum_id], dominant_values or {}) if dominant_values else 0
            spectrum_scores[spectrum_id] = (foundational_score * 2) + placement_score + mastery_alignment

        sorted_spectrums = sorted(clear_spectrums, key=lambda sid: spectrum_scores[sid], reverse=True)
        primary_id = sorted_spectrums[0]
        secondary_id = sorted_spectrums[1]

        # Logic to prefer contrasting secondary if scores are close (simplified)
        primary_is_structured = 'structured' in PLACEMENT_MAPPING.get(clear_placements[primary_id], '')
        secondary_is_structured = 'structured' in PLACEMENT_MAPPING.get(clear_placements[secondary_id], '')

        if primary_is_structured == secondary_is_structured and len(sorted_spectrums) > 2:
             for i in range(2, len(sorted_spectrums)):
                 tertiary_id = sorted_spectrums[i]
                 tertiary_is_structured = 'structured' in PLACEMENT_MAPPING.get(clear_placements[tertiary_id], '')
                 if primary_is_structured != tertiary_is_structured:
                     # Simple check: if the contrasting one exists, prefer it if score isn't drastically lower
                     if spectrum_scores[secondary_id] - spectrum_scores[tertiary_id] < 2: # Arbitrary threshold
                          secondary_id = tertiary_id
                          break

        primary_placement_key = PLACEMENT_MAPPING.get(clear_placements[primary_id], 'balanced')
        secondary_placement_key = PLACEMENT_MAPPING.get(clear_placements[secondary_id], 'balanced')

        # Ensure primary is structured if both exist, otherwise primary is fluid if both exist
        if primary_is_structured and not secondary_is_structured:
             pair_key = f"{primary_placement_key}-{secondary_placement_key}"
        elif not primary_is_structured and secondary_is_structured:
             pair_key = f"{secondary_placement_key}-{primary_placement_key}" # Swap to put structured first
             primary_id, secondary_id = secondary_id, primary_id # Swap IDs too
        elif primary_is_structured and secondary_is_structured:
             pair_key = f"{primary_placement_key}-{secondary_placement_key}" # Keep order by score
        else: # Both fluid
             pair_key = f"{primary_placement_key}-{secondary_placement_key}" # Keep order by score


        return {
            "key": pair_key,
            "primary": {"spectrumId": primary_id, "placement": clear_placements[primary_id]},
            "secondary": {"spectrumId": secondary_id, "placement": clear_placements[secondary_id]}
        }

    elif len(clear_spectrums) == 1:
        primary_id = clear_spectrums[0]
        primary_placement_key = PLACEMENT_MAPPING.get(clear_placements[primary_id], 'balanced')
        # Find most important balanced spectrum
        secondary_id = next((sid for sid in SPECTRUM_PRIORITY_ORDER if sid != primary_id and spectrum_placements.get(sid) == 'balanced'), SPECTRUM_PRIORITY_ORDER[1] if SPECTRUM_PRIORITY_ORDER[0] == primary_id else SPECTRUM_PRIORITY_ORDER[0])

        pair_key = f"{primary_placement_key}-balanced"
        if 'fluid' in primary_placement_key: # Prefer structured-balanced over fluid-balanced naming if possible
             pair_key = f"balanced-{primary_placement_key}"


        return {
            "key": pair_key,
            "primary": {"spectrumId": primary_id, "placement": clear_placements[primary_id]},
            "secondary": {"spectrumId": secondary_id, "placement": 'balanced'}
        }

    else: # All balanced
        primary_id = SPECTRUM_PRIORITY_ORDER[0]
        secondary_id = SPECTRUM_PRIORITY_ORDER[1]
        return {
            "key": "balanced-balanced",
            "primary": {"spectrumId": primary_id, "placement": 'balanced'},
            "secondary": {"spectrumId": secondary_id, "placement": 'balanced'}
        }


def calculate_energy_focus_scores(
    typology_responses: Dict[str, str],
    mastery_responses: Optional[Dict[str, str]] = None
) -> Dict[str, int]:
    """Calculates the expansion-contraction energy scores."""
    expansion_score = 50
    contraction_score = 50

    if not typology_responses:
        logger.warning("Typology responses not available for energy focus calculation")
        return {"expansionScore": expansion_score, "contractionScore": contraction_score}

    # Simplified calculation based on numeric scores
    typology_results = calculate_typology_scores(typology_responses, mastery_responses)
    numeric_scores = typology_results['numericScores']

    # Sum scores: positive leans expansion, negative leans contraction
    total_leaning = sum(numeric_scores.values())

    # Max possible sum magnitude (6 spectrums * score of 2) = 12
    max_magnitude = 12.0
    if max_magnitude == 0: # Avoid division by zero if no scores
         return {"expansionScore": 50, "contractionScore": 50}


    # Normalize the leaning to a -1 to +1 scale
    normalized_leaning = total_leaning / max_magnitude

    # Map the -1 to +1 scale to 0% to 100% expansion
    # -1 leaning = 0% expansion, +1 leaning = 100% expansion, 0 leaning = 50% expansion
    expansion_percentage = (normalized_leaning + 1) / 2 * 100

    expansionScore = int(round(expansion_percentage))
    contractionScore = 100 - expansionScore

    # Ensure scores are within bounds
    expansionScore = max(0, min(100, expansionScore))
    contractionScore = max(0, min(100, contractionScore))


    logger.debug(f"Calculated Energy Focus: Expansion={expansionScore}%, Contraction={contractionScore}%")
    return {"expansionScore": expansionScore, "contractionScore": contractionScore}


def generate_complete_results(
    typology_responses: Dict[str, str],
    mastery_responses: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    """Generates complete result data including typology, mastery, and energy focus."""
    typology_results = calculate_typology_scores(typology_responses, mastery_responses)
    mastery_scores = calculate_mastery_scores(mastery_responses or {})
    dominant_values = determine_dominant_values(mastery_scores)
    typology_pair = determine_typology_pair(typology_results['placements'], dominant_values)
    energy_focus = calculate_energy_focus_scores(typology_responses, mastery_responses) # Pass both response sets

    return {
        "typologyResults": typology_results,
        "spectrumPlacements": typology_results['placements'],
        "numericScores": typology_results['numericScores'],
        "typologyPair": typology_pair,
        "masteryScores": mastery_scores,
        "dominantValues": dominant_values,
        "masteryInfluences": typology_results['masteryInfluences'], # Include influences
        "energyFocus": energy_focus
    }

# (Removed example usage block)