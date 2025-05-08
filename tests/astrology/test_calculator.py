# tests/astrology/test_calculator.py
import pytest
import math
from datetime import datetime, time, date, timedelta, timezone as dt_timezone # Renamed to avoid conflict
import pytz
from src.astrology.calculator import get_sign, get_sign_longitude, calculate_aspect, ASPECTS, calculate_chart, SKYFIELD_OBJECTS, SIGNS
from src.models.input_models import BirthData
from src.location.timezone import HistoricalTimezoneInfo

# --- Tests for get_sign ---

@pytest.mark.parametrize("lon, expected_sign", [
    (0.0, "Aries"),
    (15.5, "Aries"),
    (29.99, "Aries"),
    (30.0, "Taurus"),
    (55.0, "Taurus"),
    (60.0, "Gemini"),
    (90.0, "Cancer"),
    (120.0, "Leo"),
    (150.0, "Virgo"),
    (180.0, "Libra"),
    (210.0, "Scorpio"),
    (240.0, "Sagittarius"),
    (270.0, "Capricorn"),
    (300.0, "Aquarius"),
    (330.0, "Pisces"),
    (359.99, "Pisces"),
    (360.0, "Aries"), # Wrap around
    (375.0, "Aries"), # Wrap around > 360
    (-1.0, "Pisces"), # Negative wrap around
    (-30.0, "Pisces"),
    (-31.0, "Aquarius"),
])
def test_get_sign(lon, expected_sign):
    assert get_sign(lon) == expected_sign

# --- Tests for get_sign_longitude ---

@pytest.mark.parametrize("lon, expected_sign_lon", [
    (0.0, 0.0),
    (15.5, 15.5),
    (29.99, 29.99),
    (30.0, 0.0),
    (55.0, 25.0),
    (60.0, 0.0),
    (359.99, 29.99),
    (360.0, 0.0),
    (375.0, 15.0),
    (-1.0, 29.0), # -1 mod 30 = 29
    (-30.0, 0.0), # -30 mod 30 = 0
    (-31.0, 29.0), # -31 mod 30 = 29
])
def test_get_sign_longitude(lon, expected_sign_lon):
    assert math.isclose(get_sign_longitude(lon), expected_sign_lon, abs_tol=1e-9)

# --- Tests for calculate_aspect ---

# Helper to check aspect details
def check_aspect(lon1, lon2, expected_type, expected_orb):
    aspect = calculate_aspect(lon1, lon2)
    assert aspect is not None
    assert aspect["type"] == expected_type
    assert math.isclose(aspect["orb"], expected_orb, abs_tol=0.01)
    # Applying/Separating is not implemented yet
    assert aspect["applying"] is None

def test_calculate_aspect_conjunction():
    orb = ASPECTS["Conjunction"][1]
    check_aspect(10.0, 10.0, "Conjunction", 0.0)
    check_aspect(10.0, 10.0 + orb / 2, "Conjunction", orb / 2)
    check_aspect(10.0, 10.0 - orb / 2, "Conjunction", orb / 2)
    check_aspect(358.0, 2.0, "Conjunction", 4.0) # Wrap around
    check_aspect(2.0, 358.0, "Conjunction", 4.0) # Wrap around other way

def test_calculate_aspect_opposition():
    orb = ASPECTS["Opposition"][1]
    check_aspect(10.0, 190.0, "Opposition", 0.0)
    check_aspect(10.0, 190.0 + orb / 2, "Opposition", orb / 2)
    check_aspect(10.0, 190.0 - orb / 2, "Opposition", orb / 2)
    check_aspect(350.0, 175.0, "Opposition", 5.0) # Wrap around involved

def test_calculate_aspect_square():
    orb = ASPECTS["Square"][1]
    check_aspect(10.0, 100.0, "Square", 0.0)
    check_aspect(10.0, 100.0 + orb / 2, "Square", orb / 2)
    check_aspect(10.0, 100.0 - orb / 2, "Square", orb / 2)
    check_aspect(10.0, 280.0, "Square", 0.0) # Waning square (360 - 270 = 90)

def test_calculate_aspect_trine():
    orb = ASPECTS["Trine"][1]
    check_aspect(10.0, 130.0, "Trine", 0.0)
    check_aspect(10.0, 130.0 + orb / 2, "Trine", orb / 2)
    check_aspect(10.0, 130.0 - orb / 2, "Trine", orb / 2)
    check_aspect(10.0, 250.0, "Trine", 0.0) # Waning trine (360 - 120 = 240)

def test_calculate_aspect_sextile():
    orb = ASPECTS["Sextile"][1]
    check_aspect(10.0, 70.0, "Sextile", 0.0)
    check_aspect(10.0, 70.0 + orb / 2, "Sextile", orb / 2)
    check_aspect(10.0, 70.0 - orb / 2, "Sextile", orb / 2)
    check_aspect(10.0, 310.0, "Sextile", 0.0) # Waning sextile (360 - 60 = 300)

def test_calculate_aspect_no_aspect():
    assert calculate_aspect(10.0, 25.0) is None # Too far for conjunction
    # Test just outside opposition orb (10 vs 198.1 -> delta 171.9 -> diff from 180 is 8.1 > 8.0)
    assert calculate_aspect(10.0, 10.0 + 180.0 + ASPECTS["Opposition"][1] + 0.1) is None
    # Test just outside square orb (10 vs 107.1 -> delta 97.1 -> diff from 90 is 7.1 > 7.0)
    assert calculate_aspect(10.0, 10.0 + 90.0 + ASPECTS["Square"][1] + 0.1) is None
# --- Tests for calculate_chart ---

# Use a consistent test birth data
TEST_BIRTH_DATA = BirthData(
    birth_date=date(1990, 5, 15),
    birth_time=time(14, 30, 0),
    latitude=34.0522,
    longitude=-118.2437,
    city_of_birth="Los Angeles",
    country_of_birth="USA",
    timezone='America/Los_Angeles'
)

# Expected Sun sign for May 15th is Taurus
EXPECTED_SUN_SIGN = "Taurus"

@pytest.fixture
def historical_tz_info(birth_data=TEST_BIRTH_DATA) -> HistoricalTimezoneInfo:
    """Provides a HistoricalTimezoneInfo object for the TEST_BIRTH_DATA."""
    birth_dt_naive = datetime.combine(birth_data.birth_date, birth_data.birth_time)
    tz_obj = pytz.timezone(birth_data.timezone)
    localized_dt = tz_obj.localize(birth_dt_naive)
    
    utc_offset = localized_dt.utcoffset()
    dst_offset = localized_dt.dst()
    
    return HistoricalTimezoneInfo(
        iana_timezone=birth_data.timezone,
        localized_datetime=localized_dt,
        utc_offset_seconds=int(utc_offset.total_seconds()) if utc_offset else 0,
        dst_offset_seconds=int(dst_offset.total_seconds()) if dst_offset else 0
    )

def test_calculate_chart_structure_and_basic_values(historical_tz_info):
    """
    Test calculate_chart for successful execution, correct output structure,
    and basic plausible values using known birth data.
    Note: This test relies on skyfield performing calculations and having
    the ephemeris file available in the test environment (e.g., Docker).
    """
    chart_result = calculate_chart(
        birth_date=TEST_BIRTH_DATA.birth_date,
        birth_time=TEST_BIRTH_DATA.birth_time,
        latitude=TEST_BIRTH_DATA.latitude,
        longitude=TEST_BIRTH_DATA.longitude,
        timezone_info=historical_tz_info
    )

    # 1. Check successful execution
    assert chart_result is not None
    assert isinstance(chart_result, dict)

    # 2. Check top-level structure
    assert "objects" in chart_result
    assert "houses" in chart_result
    assert "angles" in chart_result
    assert "aspects" in chart_result
    assert "north_node" in chart_result # North Node key should exist
    assert isinstance(chart_result["objects"], dict)
    assert isinstance(chart_result["houses"], dict)
    assert isinstance(chart_result["angles"], dict)
    assert isinstance(chart_result["aspects"], list)

    # 3. Check angles structure
    assert "Asc" in chart_result["angles"]
    assert "MC" in chart_result["angles"]
    assert isinstance(chart_result["angles"]["Asc"], dict)
    assert "sign" in chart_result["angles"]["Asc"]
    assert "lon" in chart_result["angles"]["Asc"]

    # 4. Check houses structure (Whole Sign)
    assert len(chart_result["houses"]) == 12
    for i in range(1, 13):
        house_key = f"House{i}"
        assert house_key in chart_result["houses"]
        assert isinstance(chart_result["houses"][house_key], dict)
        assert "sign" in chart_result["houses"][house_key]
        assert "lon" in chart_result["houses"][house_key]
        # Check house cusps are roughly 30 deg apart
        if i > 1:
            prev_house_key = f"House{i-1}"
            lon_diff = (chart_result["houses"][house_key]["lon"] - chart_result["houses"][prev_house_key]["lon"] + 360) % 360
            assert math.isclose(lon_diff, 30.0, abs_tol=0.1) # Allow small tolerance

    # 5. Check objects structure and basic values
    assert "Sun" in chart_result["objects"]
    assert "Moon" in chart_result["objects"]
    assert isinstance(chart_result["objects"]["Sun"], dict)
    assert "sign" in chart_result["objects"]["Sun"]
    assert "lon" in chart_result["objects"]["Sun"]
    assert "speed" in chart_result["objects"]["Sun"]
    assert "house" in chart_result["objects"]["Sun"]
    assert isinstance(chart_result["objects"]["Sun"]["house"], int)

    # Check expected Sun sign
    assert chart_result["objects"]["Sun"]["sign"] == EXPECTED_SUN_SIGN

    # Check that most objects were calculated. "North Node" in SKYFIELD_OBJECTS is a placeholder.
    # The actual North Node data is separate.
    # SKYFIELD_OBJECTS has one 'None' entry for "North Node" placeholder.
    calculated_count = sum(1 for obj_name, obj_data in chart_result["objects"].items()
                           if obj_data is not None and SKYFIELD_OBJECTS.get(obj_name) is not None)
    assert calculated_count >= len([k for k, v in SKYFIELD_OBJECTS.items() if v is not None])


    # 6. Check aspects list (just check if it's populated, specific aspects depend heavily on orbs)
    # For a typical chart, we expect *some* aspects.
    assert len(chart_result["aspects"]) > 0
    if chart_result["aspects"]:
        first_aspect = chart_result["aspects"][0]
        assert "obj1" in first_aspect
        assert "obj2" in first_aspect
        assert "type" in first_aspect
        assert "orb" in first_aspect

# Removed test_calculate_chart_invalid_timezone as Pydantic handles this validation upstream.

def test_calculate_chart_missing_ephemeris(mocker, historical_tz_info):
    """Test calculate_chart when ephemeris loading fails."""
    # Mock the skyfield Loader's __call__ method to raise an exception when called
    mocker.patch('skyfield.api.Loader.__call__', side_effect=Exception("Mocked Ephemeris load failed"))
    chart_result = calculate_chart(
        birth_date=TEST_BIRTH_DATA.birth_date,
        birth_time=TEST_BIRTH_DATA.birth_time,
        latitude=TEST_BIRTH_DATA.latitude,
        longitude=TEST_BIRTH_DATA.longitude,
        timezone_info=historical_tz_info
    )
    # The try...except block in calculate_chart should catch this and return None
    assert chart_result is None

def test_calculate_chart_north_node_data(historical_tz_info):
    """
    Test that the North Node data is correctly calculated and included in the chart.
    """
    chart_result = calculate_chart(
        birth_date=TEST_BIRTH_DATA.birth_date,
        birth_time=TEST_BIRTH_DATA.birth_time,
        latitude=TEST_BIRTH_DATA.latitude,
        longitude=TEST_BIRTH_DATA.longitude,
        timezone_info=historical_tz_info
    )

    assert chart_result is not None
    assert "north_node" in chart_result
    
    nn_data = chart_result["north_node"]
    # For TEST_BIRTH_DATA, we expect a North Node to be found.
    assert nn_data is not None, "North Node data should be present for the test birth date."
    assert isinstance(nn_data, dict)

    # Assert Longitude
    assert "longitude" in nn_data
    assert isinstance(nn_data["longitude"], float)
    assert nn_data["longitude"] >= 0.0, "Longitude {} should be >= 0.0".format(nn_data['longitude'])
    assert nn_data["longitude"] < 360.0, "Longitude {} should be < 360.0".format(nn_data['longitude'])

    # Assert Sign
    assert "sign" in nn_data
    assert nn_data["sign"] in SIGNS, "Sign {} is not a valid zodiac sign".format(nn_data['sign'])

    # Assert House
    assert "house" in nn_data
    assert isinstance(nn_data["house"], int)
    assert nn_data["house"] >= 1, "House {} should be >= 1".format(nn_data['house'])
    assert nn_data["house"] <= 12, "House {} should be <= 12".format(nn_data['house'])

    # Assert Time
    assert "time" in nn_data
    assert isinstance(nn_data["time"], str)
    
    try:
        # Skyfield's utc_iso() output includes 'Z' for UTC.
        # datetime.fromisoformat handles 'Z' correctly in Python 3.11+
        # For broader compatibility, replace 'Z' if needed, though modern Pythons are fine.
        if nn_data["time"].endswith('Z'):
            nn_time_str = nn_data["time"][:-1] + "+00:00"
        else:
            nn_time_str = nn_data["time"]
        nn_time_dt = datetime.fromisoformat(nn_time_str)
        assert nn_time_dt.tzinfo is not None and nn_time_dt.tzinfo.utcoffset(nn_time_dt) == timedelta(0), \
            "North Node time should be timezone-aware and UTC."
    except ValueError:
        pytest.fail("North Node time '{}' is not a valid ISO 8601 format.".format(nn_data['time']))

    # Assert Time is within 30 days before or at the birth time
    # The calculator uses historical_tz_info.localized_datetime
    birth_datetime_localized = historical_tz_info.localized_datetime
    birth_datetime_utc = birth_datetime_localized.astimezone(pytz.utc) # Convert to UTC for comparison

    # North Node time (nn_time_dt) is already UTC
    # The North Node found is the most recent one *at or before* birth_datetime_utc,
    # searched within a window extending 30 days prior.
    thirty_days_before_birth = birth_datetime_utc - timedelta(days=30)
    
    assert thirty_days_before_birth <= nn_time_dt, \
        "North Node time {} should be after or at {} (30 days before birth {})".format(nn_time_dt, thirty_days_before_birth, birth_datetime_utc)
    assert nn_time_dt <= birth_datetime_utc, \
        "North Node time {} should be before or at birth time {}".format(nn_time_dt, birth_datetime_utc)

# Note: More specific tests would require mocking skyfield's `at().observe().ecliptic_latlon()`
# to return predetermined values for precise checking of angles, houses, and aspects.
# These tests provide a good baseline for ensuring the function runs and structures data correctly.
# Test with a date known to be near a lunar node crossing
TEST_BIRTH_DATA_NODE_CROSSING = BirthData(
    birth_date=date(2022, 1, 10),
    birth_time=time(10, 0, 0), # 10:00 AM
    latitude=40.7128,    # New York
    longitude=-74.0060,  # New York
    city_of_birth="New York",
    country_of_birth="USA",
    timezone='America/New_York'
)

def test_calculate_chart_north_node_data_near_crossing():
    """
    Test North Node data for a birthdate known to be near a lunar node crossing.
    """
    # Create HistoricalTimezoneInfo for this specific test case
    birth_dt_naive_nc = datetime.combine(TEST_BIRTH_DATA_NODE_CROSSING.birth_date, TEST_BIRTH_DATA_NODE_CROSSING.birth_time)
    tz_obj_nc = pytz.timezone(TEST_BIRTH_DATA_NODE_CROSSING.timezone)
    localized_dt_nc = tz_obj_nc.localize(birth_dt_naive_nc)
    utc_offset_nc = localized_dt_nc.utcoffset()
    dst_offset_nc = localized_dt_nc.dst()
    historical_tz_info_nc = HistoricalTimezoneInfo(
        iana_timezone=TEST_BIRTH_DATA_NODE_CROSSING.timezone,
        localized_datetime=localized_dt_nc,
        utc_offset_seconds=int(utc_offset_nc.total_seconds()) if utc_offset_nc else 0,
        dst_offset_seconds=int(dst_offset_nc.total_seconds()) if dst_offset_nc else 0
    )

    chart_result = calculate_chart(
        birth_date=TEST_BIRTH_DATA_NODE_CROSSING.birth_date,
        birth_time=TEST_BIRTH_DATA_NODE_CROSSING.birth_time,
        latitude=TEST_BIRTH_DATA_NODE_CROSSING.latitude,
        longitude=TEST_BIRTH_DATA_NODE_CROSSING.longitude,
        timezone_info=historical_tz_info_nc
    )

    assert chart_result is not None
    assert "north_node" in chart_result
    
    nn_data = chart_result["north_node"]
    assert nn_data is not None, "North Node data should be present for the Jan 10, 2022 test birth date."
    assert isinstance(nn_data, dict)

    # Assert Longitude
    assert "longitude" in nn_data
    assert isinstance(nn_data["longitude"], float)
    msg_lon_gte = "Longitude {} should be >= 0.0".format(nn_data['longitude'])
    assert nn_data["longitude"] >= 0.0, msg_lon_gte
    msg_lon_lt = "Longitude {} should be < 360.0".format(nn_data['longitude'])
    assert nn_data["longitude"] < 360.0, msg_lon_lt

    # Assert Sign
    assert "sign" in nn_data
    msg_sign_valid = "Sign {} is not a valid zodiac sign".format(nn_data['sign'])
    assert nn_data["sign"] in SIGNS, msg_sign_valid

    # Assert House
    assert "house" in nn_data
    assert isinstance(nn_data["house"], int)
    msg_house_gte = "House {} should be >= 1".format(nn_data['house'])
    assert nn_data["house"] >= 1, msg_house_gte
    msg_house_lte = "House {} should be <= 12".format(nn_data['house'])
    assert nn_data["house"] <= 12, msg_house_lte

    # Assert Time
    assert "time" in nn_data
    assert isinstance(nn_data["time"], str)
    
    try:
        if nn_data["time"].endswith('Z'):
            nn_time_str = nn_data["time"][:-1] + "+00:00"
        else:
            nn_time_str = nn_data["time"]
        nn_time_dt = datetime.fromisoformat(nn_time_str)
        msg_nn_time_utc = "North Node time should be timezone-aware and UTC."
        assert nn_time_dt.tzinfo is not None and nn_time_dt.tzinfo.utcoffset(nn_time_dt) == timedelta(0), msg_nn_time_utc
    except ValueError:
        msg_iso_fail = "North Node time '{}' is not a valid ISO 8601 format.".format(nn_data['time'])
        pytest.fail(msg_iso_fail)

    birth_datetime_localized_nc = historical_tz_info_nc.localized_datetime
    birth_datetime_utc_nc = birth_datetime_localized_nc.astimezone(pytz.utc)
    thirty_days_before_birth_nc = birth_datetime_utc_nc - timedelta(days=30)
    
    msg_nn_time_after = "North Node time {} should be after or at {} (30 days before birth {})".format(nn_time_dt, thirty_days_before_birth_nc, birth_datetime_utc_nc)
    assert thirty_days_before_birth_nc <= nn_time_dt, msg_nn_time_after
    
    msg_nn_time_before = "North Node time {} should be before or at birth time {}".format(nn_time_dt, birth_datetime_utc_nc)
    assert nn_time_dt <= birth_datetime_utc_nc, msg_nn_time_before