import pytest
from typing import Literal
from reality_creation_profile_engine.src.humandesign.utils.environment_calculator import (
    get_rl,
    get_variable_orientation,
    get_environment_variable,
    get_environment_direction,
    get_observer_role,
    calculate_all,
)

# Tests for get_rl
@pytest.mark.parametrize(
    "line, expected_rl",
    [
        (1, "L"),
        (2, "R"),
        (3, "L"),
        (4, "R"),
        (5, "L"),
        (6, "R"),
    ],
)
def test_get_rl(line: int, expected_rl: Literal["L", "R"]):
    """Test get_rl for all valid line inputs."""
    assert get_rl(line) == expected_rl

def test_get_rl_invalid_input():
    """Test get_rl with invalid line inputs."""
    with pytest.raises(ValueError, match="Line must be between 1 and 6."):
        get_rl(0)
    with pytest.raises(ValueError, match="Line must be between 1 and 6."):
        get_rl(7)

# Tests for get_variable_orientation
@pytest.mark.parametrize(
    "design_sun_line, personality_sun_line, design_earth_line, personality_earth_line, expected_orientation",
    [
        (1, 2, 3, 4, "LRLR"),
        (6, 5, 4, 3, "RLRL"),
        (1, 1, 1, 1, "LLLL"),
        (2, 2, 2, 2, "RRRR"),
    ],
)
def test_get_variable_orientation(
    design_sun_line: int,
    personality_sun_line: int,
    design_earth_line: int,
    personality_earth_line: int,
    expected_orientation: str,
):
    """Test get_variable_orientation with various line combinations."""
    assert (
        get_variable_orientation(
            design_sun_line,
            personality_sun_line,
            design_earth_line,
            personality_earth_line,
        )
        == expected_orientation
    )

# Tests for get_environment_variable
# Test cases for Left environments (design_earth_line is odd)
@pytest.mark.parametrize(
    "design_sun_gate, design_earth_line, expected_environment",
    [
        (1, 1, "Caves"),    # (1-1)%6 = 0
        (2, 1, "Valleys"),  # (2-1)%6 = 1
        (3, 1, "Shores"),   # (3-1)%6 = 2
        (4, 1, "Caves"),    # (4-1)%6 = 3
        (5, 1, "Valleys"),  # (5-1)%6 = 4
        (6, 1, "Shores"),   # (6-1)%6 = 5
        (7, 3, "Caves"),    # (7-1)%6 = 0
        (64, 5, "Valleys"), # (64-1)%6 = 63%6 = 3 -> index 3 is Caves, but (64-1)%6 = 3, so it should be Caves.
                            # (64-1)%6 = 63%6 = 3. left_environments[3] = "Caves"
                            # Correcting: (64-1)%6 = 3. Expected: Caves
    ],
)
def test_get_environment_variable_left(
    design_sun_gate: int, design_earth_line: int, expected_environment: str
):
    """Test get_environment_variable for Left environments (odd design_earth_line)."""
    # Correcting the 64th gate case for left environments
    if design_sun_gate == 64 and design_earth_line == 5: # (64-1)%6 = 3
        expected_environment = "Caves"
    assert get_environment_variable(design_sun_gate, design_earth_line) == expected_environment

# Test cases for Right environments (design_earth_line is even)
@pytest.mark.parametrize(
    "design_sun_gate, design_earth_line, expected_environment",
    [
        (1, 2, "Markets"),   # (1-1)%6 = 0
        (2, 2, "Kitchens"),  # (2-1)%6 = 1
        (3, 2, "Mountains"), # (3-1)%6 = 2
        (4, 2, "Markets"),   # (4-1)%6 = 3
        (5, 2, "Kitchens"),  # (5-1)%6 = 4
        (6, 2, "Mountains"), # (6-1)%6 = 5
        (7, 4, "Markets"),   # (7-1)%6 = 0
        (64, 6, "Markets"),  # (64-1)%6 = 3. right_environments[3] = "Markets"
    ],
)
def test_get_environment_variable_right(
    design_sun_gate: int, design_earth_line: int, expected_environment: str
):
    """Test get_environment_variable for Right environments (even design_earth_line)."""
    assert get_environment_variable(design_sun_gate, design_earth_line) == expected_environment

def test_get_environment_variable_invalid_gate():
    """Test get_environment_variable with invalid design_sun_gate."""
    with pytest.raises(ValueError, match="Design Sun Gate must be between 1 and 64."):
        get_environment_variable(0, 1)
    with pytest.raises(ValueError, match="Design Sun Gate must be between 1 and 64."):
        get_environment_variable(65, 1)

# Tests for get_environment_direction
@pytest.mark.parametrize(
    "design_earth_line, expected_direction",
    [
        (1, "L"),
        (2, "R"),
        (3, "L"),
        (4, "R"),
        (5, "L"),
        (6, "R"),
    ],
)
def test_get_environment_direction(
    design_earth_line: int, expected_direction: Literal["L", "R"]
):
    """Test get_environment_direction for all valid design_earth_line inputs."""
    assert get_environment_direction(design_earth_line) == expected_direction

# Tests for get_observer_role
@pytest.mark.parametrize(
    "design_earth_line, expected_role",
    [
        (1, "Observed"),  # L
        (2, "Observer"),  # R
        (3, "Observed"),  # L
        (4, "Observer"),  # R
        (5, "Observed"),  # L
        (6, "Observer"),  # R
    ],
)
def test_get_observer_role(design_earth_line: int, expected_role: str):
    """Test get_observer_role for both Observed and Observer roles."""
    assert get_observer_role(design_earth_line) == expected_role

# Tests for calculate_all
@pytest.mark.parametrize(
    "design_sun_gate, design_sun_line, p_sun_gate, p_sun_line, d_earth_gate, d_earth_line, p_earth_gate, p_earth_line, expected_output",
    [
        (
            1, 1, 10, 2, 20, 3, 30, 4, # design_sun_gate, design_sun_line, personality_sun_gate, personality_sun_line, design_earth_gate, design_earth_line, personality_earth_gate, personality_earth_line
            {
                "orientation": "LRLR",
                "environment": "Caves",  # Gate 1, Line 3 (L) -> (1-1)%6=0 -> Caves
                "direction": "L",
                "role": "Observed",
            },
        ),
        (
            7, 6, 11, 5, 21, 4, 31, 3,
            {
                "orientation": "RLRL", # 6(R), 5(L), 4(R), 3(L)
                "environment": "Markets", # Gate 7, Line 4 (R) -> (7-1)%6=0 -> Markets
                "direction": "R",
                "role": "Observer",
            },
        ),
        ( # Test case for env_index = 1, Left
            2, 1, 1, 1, 1, 1, 1, 1,
            {
                "orientation": "LLLL",
                "environment": "Valleys", # Gate 2, Line 1 (L) -> (2-1)%6=1 -> Valleys
                "direction": "L",
                "role": "Observed",
            }
        ),
        ( # Test case for env_index = 2, Right
            3, 2, 2, 2, 2, 2, 2, 2,
            {
                "orientation": "RRRR",
                "environment": "Mountains", # Gate 3, Line 2 (R) -> (3-1)%6=2 -> Mountains
                "direction": "R",
                "role": "Observer",
            }
        ),
         ( # Test case for env_index = 5, Left
            6, 5, 1, 1, 1, 5, 1, 1,
            {
                "orientation": "LLLL",
                "environment": "Shores", # Gate 6, Line 5 (L) -> (6-1)%6=5 -> Shores
                "direction": "L",
                "role": "Observed",
            }
        ),
        ( # Test case for env_index = 4, Right
            63, 4, 1, 1, 1, 6, 1, 1, # design_sun_gate = 63 -> (63-1)%6 = 62%6 = 2
            {                                 # design_earth_line = 6 (R) -> right_environments[2] = Mountains
                "orientation": "RLRL", # dsl=4(R), psl=1(L), del=6(R), pel=1(L)
                "environment": "Mountains",
                "direction": "R",
                "role": "Observer",
            }
        ),
    ],
)
def test_calculate_all(
    design_sun_gate: int,
    design_sun_line: int,
    p_sun_gate: int,
    p_sun_line: int,
    d_earth_gate: int,
    d_earth_line: int,
    p_earth_gate: int,
    p_earth_line: int,
    expected_output: dict,
):
    """Test calculate_all for correct output structure and values."""
    result = calculate_all(
        design_sun_gate,
        design_sun_line,
        p_sun_gate,
        p_sun_line,
        d_earth_gate,
        d_earth_line,
        p_earth_gate,
        p_earth_line,
    )
    assert result == expected_output

def test_calculate_all_invalid_line_input():
    """Test calculate_all with an invalid line input to ensure ValueError propagates."""
    with pytest.raises(ValueError, match="Line must be between 1 and 6."):
        calculate_all(1, 7, 1, 1, 1, 1, 1, 1) # design_sun_line = 7 (invalid)

def test_calculate_all_invalid_gate_input():
    """Test calculate_all with an invalid gate input to ensure ValueError propagates."""
    with pytest.raises(ValueError, match="Design Sun Gate must be between 1 and 64."):
        calculate_all(0, 1, 1, 1, 1, 1, 1, 1) # design_sun_gate = 0 (invalid)
    with pytest.raises(ValueError, match="Design Sun Gate must be between 1 and 64."):
        calculate_all(65, 1, 1, 1, 1, 1, 1, 1) # design_sun_gate = 65 (invalid)