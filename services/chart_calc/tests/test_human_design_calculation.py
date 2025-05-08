import pytest
from httpx import AsyncClient
from fastapi import status

# Adjust imports based on how pytest is run
from .fixtures.sample_chart_payloads import (
    payload_standard,
    payload_leap_day,
    payload_invalid_date,
    payload_missing_field,
    valid_payloads
)
# from services.chart_calc.app.schemas.human_design_schemas import HumanDesignChartResponse

@pytest.mark.asyncio
async def test_health_check_hd(test_client: AsyncClient):
    """Test the health check endpoint (can be shared, but good for module completeness)."""
    response = await test_client.get("/health")
    assert response.status_code == status.HTTP_200_OK
    assert response.json() == {"status": "ok"}

@pytest.mark.asyncio
@pytest.mark.parametrize("payload", valid_payloads)
async def test_calculate_human_design_valid_payloads(test_client: AsyncClient, payload: dict):
    """Test successful Human Design calculation with various valid payloads."""
    response = await test_client.post("/calculate/human_design", json=payload)
    
    assert response.status_code == status.HTTP_200_OK
    data = response.json()

    # Basic response structure validation
    assert "request_data" in data
    assert data["request_data"]["birth_date"] == payload["birth_date"]
    assert "type" in data
    assert "authority" in data
    assert "profile" in data
    assert "definition" in data
    assert "conscious_sun_gate" in data
    assert "unconscious_sun_gate" in data
    assert "defined_centers" in data
    assert isinstance(data["defined_centers"], list)
    assert "open_centers" in data
    assert isinstance(data["open_centers"], list)
    assert "channels" in data
    assert isinstance(data["channels"], list)
    assert "gates" in data
    assert isinstance(data["gates"], list)

    # Check that gate activations list is populated
    # Number of activations depends on planets used in HD_PLANETS (human_design_service.py)
    # Current HD_PLANETS has 11 effective entries (Earth derived, Nodes are placeholders)
    # Each planet gives one conscious and one unconscious activation.
    # So, roughly 2 * (number of actual planets used)
    assert len(data["gates"]) >= 2 * 10 # Approximate, as Nodes are placeholders

    # Further checks can be added for specific HD rules if known outputs are defined.
    # E.g., Profile calculation based on Sun/Earth lines.
    # This requires more detailed expected outputs for each payload.
    # Example: Check profile format like "X/Y ..."
    assert "/" in data["profile"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload, expected_status_code, expected_detail_contains",
    [
        (payload_invalid_date, status.HTTP_422_UNPROCESSABLE_ENTITY, "Invalid date/time format"),
        (payload_missing_field, status.HTTP_422_UNPROCESSABLE_ENTITY, "Field required"),
    ]
)
async def test_calculate_human_design_invalid_payloads(
    test_client: AsyncClient,
    payload: dict,
    expected_status_code: int,
    expected_detail_contains: str
):
    """Test Human Design calculation with invalid payloads."""
    response = await test_client.post("/calculate/human_design", json=payload)
    assert response.status_code == expected_status_code
    if expected_detail_contains:
        error_details = response.json().get("detail")
        if isinstance(error_details, list): # Pydantic validation errors
             assert any(expected_detail_contains.lower() in item.get("msg", "").lower() for item in error_details) or \
                    any(expected_detail_contains.lower() in str(item.get("loc", "")).lower() for item in error_details)
        elif isinstance(error_details, str): # Custom HTTPException details
            assert expected_detail_contains.lower() in error_details.lower()
        else:
            assert False, f"Unexpected error detail format: {error_details}"

# TODO: Add tests for specific Human Design outputs (Type, Authority, Profile lines)
# This would require a more robust HD calculation engine or mock data with expected results.