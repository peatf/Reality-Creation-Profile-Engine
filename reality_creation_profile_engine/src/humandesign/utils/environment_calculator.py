"""
You are given outputs from HumanDesignAPI:
  • design_sun_gate: int (1–64)
  • design_sun_line: int (1–6)
  • personality_sun_gate: int (1–64)
  • personality_sun_line: int (1–6)
  • design_earth_gate: int (1–64)
  • design_earth_line: int (1–6)
  • personality_earth_gate: int (1–64)
  • personality_earth_line: int (1–6)

Write a Python module with these functions, full type hints, doc‑strings, and pytest unit tests. No external dependencies outside the standard library:

1. def get_rl(line: int) -> Literal['L','R']
   - Return 'L' if line is odd, 'R' if even.

2. def get_variable_orientation(design_sun_line: int, personality_sun_line: int, design_earth_line: int, personality_earth_line: int) -> str
   - Use get_rl on each line in order: design sun, personality sun, design earth, personality earth.
   - Return a 4-character string, e.g. 'LRRL'.

3. def get_environment_variable(design_sun_gate: int, design_earth_line: int) -> str
   - env_index = (design_sun_gate - 1) % 6
   - If get_rl(design_earth_line) == 'L', use left_environments = ["Caves","Valleys","Shores","Caves","Valleys","Shores"]
     else use right_environments = ["Markets","Kitchens","Mountains","Markets","Kitchens","Mountains"]
   - Return environments[env_index].

4. def get_environment_direction(design_earth_line: int) -> Literal['L','R']
   - Return get_rl(design_earth_line).

5. def get_observer_role(design_earth_line: int) -> str
   - If get_environment_direction(...) == 'L', return 'Observed'; else return 'Observer'.

6. def calculate_all(design_sun_gate: int, design_sun_line: int, personality_sun_line: int, design_earth_line: int, personality_earth_line: int) -> dict
   - Return {'orientation': ..., 'environment': ..., 'direction': ..., 'role': ...} by calling the above functions.

Write pytest tests in tests/unit/humandesign/test_environment_calculator.py that cover:
  • Both RL outputs for get_rl
  • All six possible env_index values for get_environment_variable on L and R
  • Observer vs Observed roles
  • Full calculate_all output structure

Ensure 100% coverage for this module.
"""
from typing import Literal

def get_rl(line: int) -> Literal['L', 'R']:
    """
    Determines if a line is Left ('L') or Right ('R').
    'L' if line is odd, 'R' if even.
    """
    if not 1 <= line <= 6:
        raise ValueError("Line must be between 1 and 6.")
    return 'L' if line % 2 != 0 else 'R'

def get_variable_orientation(
    design_sun_line: int,
    personality_sun_line: int,
    design_earth_line: int,
    personality_earth_line: int
) -> str:
    """
    Calculates the 4-character variable orientation string.
    Uses get_rl on each line in order: design sun, personality sun, design earth, personality earth.
    Example: 'LRRL'.
    """
    return (
        get_rl(design_sun_line) +
        get_rl(personality_sun_line) +
        get_rl(design_earth_line) +
        get_rl(personality_earth_line)
    )

def get_environment_variable(design_sun_gate: int, design_earth_line: int) -> str:
    """
    Determines the environment variable based on design sun gate and design earth line.
    env_index = (design_sun_gate - 1) % 6
    If get_rl(design_earth_line) == 'L', use left_environments.
    Else use right_environments.
    """
    if not 1 <= design_sun_gate <= 64:
        raise ValueError("Design Sun Gate must be between 1 and 64.")

    env_index = (design_sun_gate - 1) % 6
    rl_design_earth = get_rl(design_earth_line)

    left_environments = ["Caves", "Valleys", "Shores", "Caves", "Valleys", "Shores"]
    right_environments = ["Markets", "Kitchens", "Mountains", "Markets", "Kitchens", "Mountains"]

    if rl_design_earth == 'L':
        return left_environments[env_index]
    else: # rl_design_earth == 'R'
        return right_environments[env_index]

def get_environment_direction(design_earth_line: int) -> Literal['L', 'R']:
    """
    Determines the environment direction based on the design earth line.
    Returns 'L' or 'R'.
    """
    return get_rl(design_earth_line)

def get_observer_role(design_earth_line: int) -> str:
    """
    Determines the observer role based on the environment direction.
    If environment direction is 'L', returns 'Observed'.
    Else (direction is 'R'), returns 'Observer'.
    """
    direction = get_environment_direction(design_earth_line)
    return 'Observed' if direction == 'L' else 'Observer'

def calculate_all(
    design_sun_gate: int,
    design_sun_line: int,
    personality_sun_gate: int, # Not used directly in calculations but often part of input data
    personality_sun_line: int,
    design_earth_gate: int, # Not used directly in calculations but often part of input data
    design_earth_line: int,
    personality_earth_gate: int, # Not used directly in calculations but often part of input data
    personality_earth_line: int
) -> dict:
    """
    Calculates all environment-related variables.
    Returns a dictionary containing:
        'orientation': The 4-character variable orientation.
        'environment': The specific environment type (e.g., "Caves", "Markets").
        'direction': The environment direction ('L' or 'R').
        'role': The observer role ('Observed' or 'Observer').
    """
    # personality_sun_gate, design_earth_gate, personality_earth_gate are not used
    # by the functions in this module directly, but are included for completeness
    # of the typical input set.
    orientation = get_variable_orientation(
        design_sun_line,
        personality_sun_line,
        design_earth_line,
        personality_earth_line
    )
    environment = get_environment_variable(design_sun_gate, design_earth_line)
    direction = get_environment_direction(design_earth_line)
    role = get_observer_role(design_earth_line)

    return {
        'orientation': orientation,
        'environment': environment,
        'direction': direction,
        'role': role,
    }