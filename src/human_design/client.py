import logging
import httpx
import json # Import json for payload serialization
import os # Import os to potentially read from env vars later
from typing import Dict, Any, Optional
from datetime import date, time # Import date and time

logger = logging.getLogger(__name__)

# API Endpoint (Corrected based on user feedback)
HUMAN_DESIGN_API_URL = "https://api.humandesignapi.nl/v1/bodygraphs"

# API Keys should be set as environment variables:
# HD_API_KEY: Your HumanDesignAPI.nl key
# GEO_API_KEY: Your Google Geocoding API key (if needed by HD API)

async def get_human_design_chart(
    birth_date: date,
    birth_time: time,
    city_of_birth: str,
    country_of_birth: str,
    # latitude and longitude might be used implicitly by the API via geocode key,
    # but we accept them in case explicit passing becomes necessary later.
    latitude: float,
    longitude: float
) -> Optional[Dict[str, Any]]:
    """
    Fetches Human Design chart data from the external API using city/country.

    Args:
        birth_date: The date of birth.
        birth_time: The time of birth.
        city_of_birth: The city of birth.
        country_of_birth: The country of birth.
        latitude: Geocoded latitude (potentially used by API via geocode key).
        longitude: Geocoded longitude (potentially used by API via geocode key).

    Returns:
        A dictionary containing the parsed Human Design chart data,
        or None if the request fails or returns invalid data.
    """
    logger.info(f"Fetching Human Design chart for date: {birth_date}, time: {birth_time}, location: {city_of_birth}, {country_of_birth}")

    # --- Get API Keys from Environment Variables ---
    hd_api_key = os.getenv("HD_API_KEY")
    geo_api_key = os.getenv("GEO_API_KEY") # May not be strictly required by HD API itself

    if not hd_api_key:
        logger.error("HD_API_KEY environment variable not set. Cannot fetch Human Design chart.")
        return None
    # Optionally check for geo_api_key if you know it's always required
    # if not geo_api_key:
    #     logger.error("GEO_API_KEY environment variable not set.")
    #     return None

    # Prepare request payload based on API documentation/example
    # Format date as DD-Mon-YY (e.g., 15-May-90)
    # Format time as HH:MM (e.g., 14:30)
    # Construct location string using city and country
    location_str = f"{city_of_birth}, {country_of_birth}"
    logger.debug(f"Using location string for API: {location_str}")

    payload = {
        'birthdate': birth_date.strftime('%d-%b-%y'), # DD-Mon-YY format
        'birthtime': birth_time.strftime('%H:%M'),   # HH:MM format
        'location': location_str,
        # Explicit lat/lon are usually not needed if city/country and geocode key are provided.
        # Keep them commented out unless API documentation specifies otherwise.
        # 'latitude': latitude,
        # 'longitude': longitude,
        # Timezone is typically inferred by the API based on location and date/time.
    }

    headers = {
        'Content-Type': 'application/json',
        'HD-Api-Key': hd_api_key, # Reverted header name back to HD-Api-Key
        'HD-Geocode-Key': geo_api_key or "", # Use fetched key, provide empty string if None
        'Accept': 'application/json' # Good practice to specify accepted response type
    }

    async with httpx.AsyncClient() as client:
        try:
            logger.debug(f"Sending POST request to {HUMAN_DESIGN_API_URL} with payload: {payload}")
            response = await client.post(
                HUMAN_DESIGN_API_URL,
                headers=headers,
                content=json.dumps(payload), # Send data as JSON string in body
                timeout=30.0
            )
            response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)

            chart_data = response.json()
            logger.info(f"Successfully fetched Human Design chart data. Type: {chart_data.get('type', 'N/A')}")
            # TODO: Add validation here to ensure the response structure matches expectations
            return chart_data

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error occurred while fetching Human Design chart: {e.response.status_code} - {e.response.text}")
            return None
        except httpx.RequestError as e:
            logger.error(f"Request error occurred while fetching Human Design chart: {e}")
            return None
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}", exc_info=True)
            return None

# Example Usage (Updated for new function signature)
if __name__ == '__main__':
    import asyncio
    from datetime import date, time

    # Example birth data (replace with actual test data if API is live)
    example_birth_date = date(1990, 5, 15)
    example_birth_time = time(14, 30, 0)
    example_city = 'Los Angeles'
    example_country = 'USA'
    # Lat/Lon are needed for the function signature, even if API uses city/country
    example_latitude = 34.0522
    example_longitude = -118.2437

    async def main():
        print("Attempting to fetch Human Design chart...")
        chart = await get_human_design_chart(
            birth_date=example_birth_date,
            birth_time=example_birth_time,
            city_of_birth=example_city,
            country_of_birth=example_country,
            latitude=example_latitude,
            longitude=example_longitude
        )
        if chart:
            print("\n--- Fetched Human Design Chart Data (Sample) ---")
            # Print some key fields if available
            print(f"  Type: {chart.get('type')}")
            print(f"  Authority: {chart.get('authority')}")
            print(f"  Profile: {chart.get('profile')}")
            # print(json.dumps(chart, indent=2)) # Uncomment for full output
        else:
            print("Failed to fetch Human Design chart.")

    # Ensure HD_API_KEY and potentially GEO_API_KEY are set in environment
    if not os.getenv("HD_API_KEY"):
        print("Warning: HD_API_KEY environment variable not set. API call will likely fail.")
    asyncio.run(main())