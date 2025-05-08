import pytest
from httpx import AsyncClient
from fastapi import status

# Assuming conftest.py is in the same directory or accessible in PYTHONPATH
# from ..app.main import CalculationRequest # If running from services/chart_calc/tests
# from app.schemas.astrology_schemas import AstrologyChartResponse # If running from services/chart_calc
from .fixtures.sample_chart_payloads import ( # Relative import from fixtures folder
    payload_standard,
    payload_leap_day,
    payload_southern_hemisphere,
    payload_extreme_north,
    payload_invalid_date,
    payload_invalid_latitude,
    payload_missing_field,
    valid_payloads
)
# Note: Adjust import paths if pytest is run from a different root directory.
# If running pytest from project root:
# from services.chart_calc.tests.fixtures.sample_chart_payloads import ...
# from services.chart_calc.app.schemas.astrology_schemas import AstrologyChartResponse


@pytest.mark.asyncio
async def test_health_check_astrology(test_client: AsyncClient):
    """Test the health check endpoint."""
    response = await test_client.get("/health")
    assert response.status_code == status.HTTP_200_OK
    assert response.json() == {"status": "ok"}

@pytest.mark.asyncio
@pytest.mark.parametrize("payload", valid_payloads)
async def test_calculate_astrology_valid_payloads(test_client: AsyncClient, payload: dict):
    """Test successful astrology calculation with various valid payloads."""
    response = await test_client.post("/calculate/astrology", json=payload)
    
    assert response.status_code == status.HTTP_200_OK
    data = response.json()
    
    # Basic response structure validation
    assert "request_data" in data
    assert data["request_data"]["birth_date"] == payload["birth_date"]
    assert "planetary_positions" in data
    assert isinstance(data["planetary_positions"], list)
    assert "house_cusps" in data
    assert isinstance(data["house_cusps"], list)
    assert "aspects" in data # Even if empty, the key should be there
    assert isinstance(data["aspects"], list)
    
    # Check if planets are returned (Skyfield service layer should populate this)
    # The exact number might vary if we add/remove minor bodies, but core planets should be there.
    # PLANETS_TO_CALCULATE in astrology_service.py has 10 entries.
    assert len(data["planetary_positions"]) >= 10 # Sun to Pluto
    
    # Check house cusps (placeholder logic returns 12)
    assert len(data["house_cusps"]) == 12

    # Further checks can be added for specific astrological rules if known outputs are defined
    # e.g., Sun sign for a given date, Ascendant sign for a given time/location.
    # This requires more detailed expected outputs for each payload.

@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload, expected_status_code, expected_detail_contains",
    [
        (payload_invalid_date, status.HTTP_422_UNPROCESSABLE_ENTITY, "Invalid date/time format"),
        (payload_invalid_latitude, status.HTTP_422_UNPROCESSABLE_ENTITY, "latitude"), # Pydantic error
        (payload_missing_field, status.HTTP_422_UNPROCESSABLE_ENTITY, "Field required"), # Pydantic error for missing birth_time
    ]
)
async def test_calculate_astrology_invalid_payloads(
    test_client: AsyncClient,
    payload: dict,
    expected_status_code: int,
    expected_detail_contains: str
):
    """Test astrology calculation with invalid payloads."""
    response = await test_client.post("/calculate/astrology", json=payload)
    assert response.status_code == expected_status_code
    if expected_detail_contains: # Some 422 errors might not have a simple string detail from service
        error_details = response.json().get("detail")
        if isinstance(error_details, list): # Pydantic validation errors
             assert any(expected_detail_contains.lower() in item.get("msg", "").lower() for item in error_details) or \
                    any(expected_detail_contains.lower() in str(item.get("loc", "")).lower() for item in error_details)
        elif isinstance(error_details, str): # Custom HTTPException details
            assert expected_detail_contains.lower() in error_details.lower()
        else:
            assert False, f"Unexpected error detail format: {error_details}"


# TODO: Add tests for edge cases like extreme latitudes if the calculation logic supports them robustly.
# TODO: Add tests for specific astrological outputs if known values are available (e.g. Sun in Aries on specific date).
# This would require more complex fixture data with expected results.