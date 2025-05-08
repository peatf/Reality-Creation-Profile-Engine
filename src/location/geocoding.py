import googlemaps
import os
from typing import Optional, Tuple
from pydantic import BaseModel, Field
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load API Key from environment variable
API_KEY = os.getenv("GEO_API_KEY")
if not API_KEY:
    logger.warning("GEO_API_KEY environment variable not set. Geocoding will not work.")
    gmaps = None
else:
    try:
        gmaps = googlemaps.Client(key=API_KEY)
    except Exception as e:
        logger.error(f"Failed to initialize Google Maps client: {e}")
        gmaps = None

class GeocodingError(Exception):
    """Custom exception for geocoding errors."""
    pass

class GeocodingResult(BaseModel):
    """Model for storing geocoding results."""
    latitude: float = Field(..., description="Latitude in decimal degrees")
    longitude: float = Field(..., description="Longitude in decimal degrees")
    formatted_address: str = Field(..., description="Full address returned by the API")

def get_coordinates(city: str, country: Optional[str] = None) -> GeocodingResult:
    """
    Geocodes a city and optional country to latitude and longitude using Google Geocoding API.

    Args:
        city: The name of the city.
        country: The name of the country (optional, helps disambiguation).

    Returns:
        A GeocodingResult object containing latitude, longitude, and formatted address.

    Raises:
        GeocodingError: If the API key is missing, the API call fails,
                        or no results are found for the given location.
    """
    if not gmaps:
        raise GeocodingError("Google Maps client not initialized. Check API key.")

    address = f"{city}"
    if country:
        address += f", {country}"

    logger.info(f"Attempting to geocode address: {address}")
    try:
        geocode_result = gmaps.geocode(address)
    except googlemaps.exceptions.ApiError as e:
        logger.error(f"Google Geocoding API error: {e}")
        raise GeocodingError(f"Geocoding API error: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred during geocoding: {e}")
        raise GeocodingError(f"Unexpected error during geocoding: {e}")

    if not geocode_result:
        logger.warning(f"No geocoding results found for address: {address}")
        raise GeocodingError(f"No results found for '{address}'. Please check the city and country.")

    if len(geocode_result) > 1:
        logger.warning(f"Multiple geocoding results found for '{address}'. Using the first result.")
        # Consider adding logic here to handle ambiguity if needed,
        # e.g., by checking components or returning multiple options.

    first_result = geocode_result[0]
    location = first_result.get('geometry', {}).get('location')
    formatted_address = first_result.get('formatted_address', 'N/A')

    if not location or 'lat' not in location or 'lng' not in location:
        logger.error(f"Could not extract coordinates from geocoding result for: {address}")
        raise GeocodingError(f"Could not extract coordinates for '{address}'.")

    lat = location['lat']
    lng = location['lng']
    logger.info(f"Geocoding successful for '{address}': Lat={lat}, Lng={lng}, Address='{formatted_address}'")

    return GeocodingResult(latitude=lat, longitude=lng, formatted_address=formatted_address)

# Example usage (for testing purposes)
if __name__ == '__main__':
    try:
        # Make sure GEO_API_KEY is set in your environment for this to work
        coords_london = get_coordinates("London", "UK")
        print(f"London, UK: {coords_london}")

        coords_sf = get_coordinates("San Francisco", "USA")
        print(f"San Francisco, USA: {coords_sf}")

        # Example of a potentially ambiguous city
        coords_springfield = get_coordinates("Springfield")
        print(f"Springfield (first result): {coords_springfield}")

    except GeocodingError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")