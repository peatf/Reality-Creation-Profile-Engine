# tests/human_design/test_client.py
import pytest
import httpx
import json
from datetime import date, time
from unittest.mock import AsyncMock, MagicMock

from src.models.input_models import BirthData
from src.human_design.client import get_human_design_chart, HUMAN_DESIGN_API_URL # Remove API key imports

# Consistent test birth data
TEST_BIRTH_DATA = BirthData(
    birth_date=date(1990, 5, 15),
    birth_time=time(14, 30, 0),
    latitude=34.0522,
    longitude=-118.2437,
    timezone='America/Los_Angeles',
    city_of_birth='Los Angeles', # Renamed from city
    country_of_birth='USA'       # Renamed from country
)

# Expected payload format
EXPECTED_PAYLOAD = {
    'birthdate': '15-May-90',
    'birthtime': '14:30',
    'location': 'Los Angeles, USA'
}

# Sample successful API response
SAMPLE_SUCCESS_RESPONSE = {
    "type": "Generator",
    "authority": "Sacral",
    "profile": "5/1",
    # ... other fields ...
}

@pytest.mark.asyncio
async def test_get_human_design_chart_success(mocker):
    """Test successful API call."""
    # Mock the response object
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = SAMPLE_SUCCESS_RESPONSE
    mock_response.raise_for_status = MagicMock() # Mock this method

    # Mock the httpx.AsyncClient and its post method
    mock_post = AsyncMock(return_value=mock_response)
    mock_client = MagicMock(spec=httpx.AsyncClient)
    mock_client.post = mock_post
    mock_client.__aenter__.return_value = mock_client # For async context manager

    mocker.patch('httpx.AsyncClient', return_value=mock_client)

    # Mock os.getenv to return fake API keys
    mock_getenv = mocker.patch('os.getenv')
    mock_getenv.side_effect = lambda key: 'FAKE_HD_KEY' if key == 'HD_API_KEY' else ('FAKE_GEO_KEY' if key == 'GEO_API_KEY' else None)

    # Call the function, unpacking the BirthData object
    result = await get_human_design_chart(
        birth_date=TEST_BIRTH_DATA.birth_date,
        birth_time=TEST_BIRTH_DATA.birth_time,
        city_of_birth=TEST_BIRTH_DATA.city_of_birth,
        country_of_birth=TEST_BIRTH_DATA.country_of_birth,
        latitude=TEST_BIRTH_DATA.latitude,
        longitude=TEST_BIRTH_DATA.longitude
    )

    # Assertions
    assert result == SAMPLE_SUCCESS_RESPONSE
    # Restore original os.getenv if necessary, though pytest handles fixture cleanup
    # mock_getenv.stop() # Not strictly needed with pytest fixtures

    mock_post.assert_awaited_once_with(
        HUMAN_DESIGN_API_URL,
        headers={
            'Content-Type': 'application/json',
            'HD-Api-Key': 'FAKE_HD_KEY', # Check against mocked key
            'HD-Geocode-Key': 'FAKE_GEO_KEY', # Check against mocked key
            'Accept': 'application/json'
        },
        content=json.dumps(EXPECTED_PAYLOAD),
        timeout=30.0
    )

@pytest.mark.asyncio
async def test_get_human_design_chart_http_error(mocker):
    """Test handling of HTTP status errors (e.g., 4xx, 5xx)."""
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 401 # Unauthorized
    mock_response.text = "Authentication failed"
    # Mock raise_for_status to raise the specific error
    mock_response.raise_for_status = MagicMock(side_effect=httpx.HTTPStatusError(
        "Client Error", request=MagicMock(), response=mock_response
    ))

    mock_post = AsyncMock(return_value=mock_response)
    mock_client = MagicMock(spec=httpx.AsyncClient)
    mock_client.post = mock_post
    mock_client.__aenter__.return_value = mock_client

    mocker.patch('httpx.AsyncClient', return_value=mock_client)

    # Mock os.getenv
    mock_getenv = mocker.patch('os.getenv')
    mock_getenv.side_effect = lambda key: 'FAKE_HD_KEY' if key == 'HD_API_KEY' else ('FAKE_GEO_KEY' if key == 'GEO_API_KEY' else None)
 
    result = await get_human_design_chart(
        birth_date=TEST_BIRTH_DATA.birth_date,
        birth_time=TEST_BIRTH_DATA.birth_time,
        city_of_birth=TEST_BIRTH_DATA.city_of_birth,
        country_of_birth=TEST_BIRTH_DATA.country_of_birth,
        latitude=TEST_BIRTH_DATA.latitude,
        longitude=TEST_BIRTH_DATA.longitude
    )

    assert result is None
    mock_post.assert_awaited_once() # Check that post was called

@pytest.mark.asyncio
async def test_get_human_design_chart_request_error(mocker):
    """Test handling of request errors (e.g., network issues)."""
    mock_post = AsyncMock(side_effect=httpx.RequestError("Network error"))
    mock_client = MagicMock(spec=httpx.AsyncClient)
    mock_client.post = mock_post
    mock_client.__aenter__.return_value = mock_client

    mocker.patch('httpx.AsyncClient', return_value=mock_client)

    # Mock os.getenv
    mock_getenv = mocker.patch('os.getenv')
    mock_getenv.side_effect = lambda key: 'FAKE_HD_KEY' if key == 'HD_API_KEY' else ('FAKE_GEO_KEY' if key == 'GEO_API_KEY' else None)
 
    result = await get_human_design_chart(
        birth_date=TEST_BIRTH_DATA.birth_date,
        birth_time=TEST_BIRTH_DATA.birth_time,
        city_of_birth=TEST_BIRTH_DATA.city_of_birth,
        country_of_birth=TEST_BIRTH_DATA.country_of_birth,
        latitude=TEST_BIRTH_DATA.latitude,
        longitude=TEST_BIRTH_DATA.longitude
    )

    assert result is None
    mock_post.assert_awaited_once()

@pytest.mark.asyncio
async def test_get_human_design_chart_unexpected_error(mocker):
    """Test handling of unexpected errors during the request."""
    mock_post = AsyncMock(side_effect=Exception("Something unexpected"))
    mock_client = MagicMock(spec=httpx.AsyncClient)
    mock_client.post = mock_post
    mock_client.__aenter__.return_value = mock_client

    mocker.patch('httpx.AsyncClient', return_value=mock_client)

    # Mock os.getenv
    mock_getenv = mocker.patch('os.getenv')
    mock_getenv.side_effect = lambda key: 'FAKE_HD_KEY' if key == 'HD_API_KEY' else ('FAKE_GEO_KEY' if key == 'GEO_API_KEY' else None)
 
    result = await get_human_design_chart(
        birth_date=TEST_BIRTH_DATA.birth_date,
        birth_time=TEST_BIRTH_DATA.birth_time,
        city_of_birth=TEST_BIRTH_DATA.city_of_birth,
        country_of_birth=TEST_BIRTH_DATA.country_of_birth,
        latitude=TEST_BIRTH_DATA.latitude,
        longitude=TEST_BIRTH_DATA.longitude
    )

    assert result is None
    mock_post.assert_awaited_once()

@pytest.mark.asyncio
async def test_get_human_design_chart_location_fallback(mocker): # Add mocker fixture
    """Test that location string uses coordinates if city/country are missing."""
    birth_data_no_city = BirthData(
        birth_date=date(1990, 5, 15),
        birth_time=time(14, 30, 0),
        latitude=34.0522,
        longitude=-118.2437,
        timezone='America/Los_Angeles',
        city=None, # Missing city
        country=None # Missing country
    )
    expected_payload_fallback = {
        'birthdate': '15-May-90',
        'birthtime': '14:30',
        'location': f"Lat:{birth_data_no_city.latitude}, Lon:{birth_data_no_city.longitude}"
    }

    mock_response = MagicMock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = SAMPLE_SUCCESS_RESPONSE
    mock_response.raise_for_status = MagicMock()

    mock_post = AsyncMock(return_value=mock_response)
    mock_client = MagicMock(spec=httpx.AsyncClient)
    mock_client.post = mock_post
    mock_client.__aenter__.return_value = mock_client

    mocker.patch('httpx.AsyncClient', return_value=mock_client)

    # Mock os.getenv
    mock_getenv = mocker.patch('os.getenv')
    mock_getenv.side_effect = lambda key: 'FAKE_HD_KEY' if key == 'HD_API_KEY' else ('FAKE_GEO_KEY' if key == 'GEO_API_KEY' else None)
 
    # Note: The client function expects city/country strings, even if None/empty.
    # The test setup uses None, which might cause issues if the client doesn't handle it.
    # Assuming the client handles None or empty strings appropriately for city/country.
    await get_human_design_chart(
        birth_date=birth_data_no_city.birth_date,
        birth_time=birth_data_no_city.birth_time,
        city_of_birth=birth_data_no_city.city, # Pass None as per test setup
        country_of_birth=birth_data_no_city.country, # Pass None as per test setup
        latitude=birth_data_no_city.latitude,
        longitude=birth_data_no_city.longitude
    )

    # Assert that the payload used the fallback location string
    mock_post.assert_awaited_once_with(
        HUMAN_DESIGN_API_URL,
        headers=mocker.ANY, # Don't need to check headers again
        content=json.dumps(expected_payload_fallback),
        timeout=30.0
    )