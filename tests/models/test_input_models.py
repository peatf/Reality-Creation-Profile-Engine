# tests/models/test_input_models.py

import pytest
from unittest.mock import patch
from datetime import date, time
import logging # For caplog

from pydantic import ValidationError # For testing Pydantic's own validation

# Assuming these are correctly defined and importable as per the project structure
from src.models.input_models import BirthData
from src.location.geocoding import GeocodingResult, GeocodingError

# Common test data
VALID_BIRTH_DATE = date(1990, 1, 1)
VALID_BIRTH_TIME = time(12, 0, 0)
VALID_CITY = "London"
VALID_COUNTRY = "UK"
EMPTY_COUNTRY = "" # For testing city-only geocoding path (country field is required but can be empty)
VALID_TIMEZONE = "Europe/London"
INVALID_TIMEZONE = "Invalid/Timezone"

GEOCODED_LAT = 51.5074
GEOCODED_LON = -0.1278 # London's approx coordinates
PROVIDED_LAT = 40.7128
PROVIDED_LON = -74.0060 # New York's approx coordinates

MOCK_GEO_RESULT = GeocodingResult(latitude=GEOCODED_LAT, longitude=GEOCODED_LON, formatted_address=f"{VALID_CITY}, {VALID_COUNTRY}")
MOCK_GEO_RESULT_CITY_ONLY = GeocodingResult(latitude=GEOCODED_LAT, longitude=GEOCODED_LON, formatted_address=f"{VALID_CITY}")

# Base valid data, can be overridden in tests
BASE_VALID_DATA = {
    "birth_date": VALID_BIRTH_DATE,
    "birth_time": VALID_BIRTH_TIME,
    "city_of_birth": VALID_CITY,
    "country_of_birth": VALID_COUNTRY,
    "timezone": VALID_TIMEZONE,
}

@patch('src.models.input_models.get_coordinates')
def test_successful_geocoding_city_and_country(mock_get_coordinates):
    mock_get_coordinates.return_value = MOCK_GEO_RESULT
    data_in = {**BASE_VALID_DATA}

    birth_data = BirthData(**data_in)

    mock_get_coordinates.assert_called_once_with(city=VALID_CITY, country=VALID_COUNTRY)
    assert birth_data.latitude == GEOCODED_LAT
    assert birth_data.longitude == GEOCODED_LON
    assert birth_data.timezone == VALID_TIMEZONE # Check timezone is preserved

@patch('src.models.input_models.get_coordinates')
def test_successful_geocoding_city_only(mock_get_coordinates):
    mock_get_coordinates.return_value = MOCK_GEO_RESULT_CITY_ONLY
    # To hit the 'elif city:' branch, country must be falsy.
    # Since country_of_birth is required (Field(...)), we pass an empty string.
    data_in = {
        **BASE_VALID_DATA,
        "country_of_birth": EMPTY_COUNTRY, # Falsy country
    }

    birth_data = BirthData(**data_in)

    # The model calls get_coordinates(city=city) in this branch
    mock_get_coordinates.assert_called_once_with(city=VALID_CITY)
    assert birth_data.latitude == GEOCODED_LAT
    assert birth_data.longitude == GEOCODED_LON

@patch('src.models.input_models.get_coordinates')
def test_geocoding_failure_geocoding_error_city_country(mock_get_coordinates, caplog):
    mock_get_coordinates.side_effect = GeocodingError("Mocked geocoding failure")
    data_in = {**BASE_VALID_DATA}

    with caplog.at_level(logging.WARNING):
        birth_data = BirthData(**data_in)

    mock_get_coordinates.assert_called_once_with(city=VALID_CITY, country=VALID_COUNTRY)
    assert birth_data.latitude is None
    assert birth_data.longitude is None
    assert f"Geocoding failed for {VALID_CITY}, {VALID_COUNTRY}" in caplog.text
    assert "Mocked geocoding failure" in caplog.text

@patch('src.models.input_models.get_coordinates')
def test_geocoding_failure_geocoding_error_city_only(mock_get_coordinates, caplog):
    mock_get_coordinates.side_effect = GeocodingError("Mocked geocoding failure city only")
    data_in = {
        **BASE_VALID_DATA,
        "country_of_birth": EMPTY_COUNTRY,
    }
    with caplog.at_level(logging.WARNING):
        birth_data = BirthData(**data_in)

    mock_get_coordinates.assert_called_once_with(city=VALID_CITY)
    assert birth_data.latitude is None
    assert birth_data.longitude is None
    assert f"Geocoding failed for {VALID_CITY}" in caplog.text # Model logs "Geocoding failed for {city}"
    assert "Mocked geocoding failure city only" in caplog.text


@patch('src.models.input_models.get_coordinates')
def test_geocoding_failure_unexpected_error_city_country(mock_get_coordinates, caplog):
    mock_get_coordinates.side_effect = Exception("Unexpected mock error")
    data_in = {**BASE_VALID_DATA}

    with caplog.at_level(logging.ERROR): # Model logs as ERROR for unexpected exceptions
        birth_data = BirthData(**data_in)

    mock_get_coordinates.assert_called_once_with(city=VALID_CITY, country=VALID_COUNTRY)
    assert birth_data.latitude is None
    assert birth_data.longitude is None
    assert f"Unexpected error during geocoding for {VALID_CITY}, {VALID_COUNTRY}" in caplog.text
    assert "Unexpected mock error" in caplog.text

@patch('src.models.input_models.get_coordinates')
def test_geocoding_failure_unexpected_error_city_only(mock_get_coordinates, caplog):
    mock_get_coordinates.side_effect = Exception("Unexpected mock error city only")
    data_in = {
        **BASE_VALID_DATA,
        "country_of_birth": EMPTY_COUNTRY,
    }
    with caplog.at_level(logging.ERROR):
        birth_data = BirthData(**data_in)

    mock_get_coordinates.assert_called_once_with(city=VALID_CITY)
    assert birth_data.latitude is None
    assert birth_data.longitude is None
    assert f"Unexpected error during geocoding for {VALID_CITY}" in caplog.text
    assert "Unexpected mock error city only" in caplog.text


@patch('src.models.input_models.get_coordinates')
def test_coordinates_provided_no_geocoding_call(mock_get_coordinates):
    data_in = {
        **BASE_VALID_DATA,
        "latitude": PROVIDED_LAT,
        "longitude": PROVIDED_LON,
    }
    birth_data = BirthData(**data_in)

    mock_get_coordinates.assert_not_called()
    assert birth_data.latitude == PROVIDED_LAT
    assert birth_data.longitude == PROVIDED_LON
    assert birth_data.timezone == VALID_TIMEZONE

@patch('src.models.input_models.get_coordinates')
def test_insufficient_info_for_geocoding_no_city(mock_get_coordinates, caplog):
    # city_of_birth is required by Pydantic. To test the "city not provided" logic
    # within the root_validator, city_of_birth must be falsy (e.g. empty string).
    data_in = {
        **BASE_VALID_DATA,
        "city_of_birth": "", # Empty city string
        # country_of_birth is still present from BASE_VALID_DATA but 'city' being falsy takes precedence
    }
    with caplog.at_level(logging.WARNING):
        birth_data = BirthData(**data_in)

    mock_get_coordinates.assert_not_called()
    assert birth_data.latitude is None
    assert birth_data.longitude is None
    assert "City not provided, cannot geocode for latitude/longitude." in caplog.text

# Timezone validation tests
def test_valid_timezone_passes():
    # This is implicitly tested in most other tests if they use BASE_VALID_DATA.
    # Explicit test for clarity.
    data_in = {
        **BASE_VALID_DATA,
        "latitude": PROVIDED_LAT, # Provide coords to skip geocoding
        "longitude": PROVIDED_LON,
        "timezone": "America/New_York" # A different valid timezone
    }
    birth_data = BirthData(**data_in)
    assert birth_data.timezone == "America/New_York"

def test_invalid_timezone_raises_value_error():
    data_in = {
        **BASE_VALID_DATA,
        "latitude": PROVIDED_LAT, # Provide coords to skip geocoding
        "longitude": PROVIDED_LON,
        "timezone": INVALID_TIMEZONE
    }
    with pytest.raises(ValueError, match=f"Invalid timezone string: '{INVALID_TIMEZONE}'. Must be a valid IANA timezone."):
        BirthData(**data_in)

def test_missing_required_fields_raises_validation_error():
    # Test Pydantic's own validation for required fields before root_validator runs

    # Example: missing birth_date
    with pytest.raises(ValidationError, match="birth_date"):
        BirthData(
            # birth_date missing
            birth_time=VALID_BIRTH_TIME,
            city_of_birth=VALID_CITY,
            country_of_birth=VALID_COUNTRY,
            timezone=VALID_TIMEZONE
        )

    # Example: missing timezone
    with pytest.raises(ValidationError, match="timezone"):
        BirthData(
            birth_date=VALID_BIRTH_DATE,
            birth_time=VALID_BIRTH_TIME,
            city_of_birth=VALID_CITY,
            country_of_birth=VALID_COUNTRY,
            # timezone missing
        )

    # Example: missing city_of_birth
    with pytest.raises(ValidationError, match="city_of_birth"):
        BirthData(
            birth_date=VALID_BIRTH_DATE,
            birth_time=VALID_BIRTH_TIME,
            # city_of_birth missing
            country_of_birth=VALID_COUNTRY,
            timezone=VALID_TIMEZONE
        )