# Typology Engine Service

This service is responsible for calculating the Reality Creation Typology profile based on user assessment responses.

It takes raw responses (both typology spectrum questions and mastery section questions) and applies scoring logic, including adjustments based on mastery answers, to determine:

*   Spectrum placements (e.g., Rational, Intuitive, Balanced)
*   Numeric scores for each spectrum
*   Dominant mastery values
*   The primary/secondary typology pair
*   Expansion/Contraction energy focus scores

## Usage

The primary entry point is the `generate_typology_result` function within `engine.py`.

```python
import uuid
import json
from services.typology_engine.engine import generate_typology_result # Adjust import path if necessary

# Example user responses (replace with actual data)
user_answers = {
    "typology_responses": {
        'cognitive-q1': 'left', 'cognitive-q2': 'balanced',
        'perceptual-q1': 'right', 'perceptual-q2': 'right',
        'kinetic-q1': 'left', 'kinetic-q2': 'left',
        'choice-q1': 'balanced', 'choice-q2': 'left',
        'resonance-q1': 'right', 'resonance-q2': 'balanced',
        'rhythm-q1': 'balanced', 'rhythm-q2': 'right'
    },
    "mastery_responses": {
        'core-q1': 'creative-expression', 'core-q2': 'craft-mastery', 'core-q3': 'passion-inspiration',
        'growth-q1': 'action-challenge', 'growth-q2': 'action-gap', 'growth-q3': 'momentum-resistance',
        'alignment-q1': 'accept-cycles', 'alignment-q2': 'control-consistency',
        'energy-q1': 'intuitive-instincts', 'energy-q2': 'rigid-routines', 'energy-q3': 'spontaneous-productivity', 'energy-q4': 'dynamic-environment'
    }
}

user_id = uuid.uuid4()
algo_version = "1.1" # Specify the algorithm version used

try:
    result_data = generate_typology_result(
        answer_json=user_answers,
        algo_version=algo_version,
        user_id=user_id
    )
    print("Typology Result Generated:")
    print(json.dumps(result_data, indent=2))

    # Access specific parts of the result:
    # print(f"Typology Pair: {result_data['scores']['typologyPair']['key']}")
    # print(f"Numeric Scores: {result_data['scores']['numericScores']}")

except Exception as e:
    print(f"An error occurred: {e}")

```

This function returns a dictionary containing the `user_id`, `algo_version`, the original `raw_json` answers, the calculated `scores` (which includes all sub-calculations like placements, numeric scores, pair, etc.), and a `created_at` timestamp.