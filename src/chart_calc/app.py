import logging
from fastapi import FastAPI, APIRouter, HTTPException, Depends, Body, Query # Added Query
from pydantic import BaseModel, field_validator # Added field_validator
from datetime import date, time, datetime # Added datetime

# Import necessary components from the main project structure
# Adjust paths if necessary based on how this service is run (e.g., Docker context)
from src.location import geocoding as geocoding_service
from src.location import timezone as timezone_service
from src.location.geocoding import GeocodingResult, GeocodingError
from src.location.timezone import HistoricalTimezoneInfo, TimezoneError # Corrected class name
from src.astrology import calculator as astro_calc
from src.astrology import schema_parser as astro_parser
from src.human_design import client as hd_client
from src.human_design import interpreter as hd_interpreter
from src.models.input_models import BirthData # Re-use the existing model if appropriate

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(title="Chart Calculation Service")

# --- Input Models for this Service ---

class AstroChartRequest(BaseModel):
    birth_date: date
    birth_time: time
    city_of_birth: str
    country_of_birth: str

class HumanDesignChartRequest(BaseModel):
    birth_date: date
    birth_time: time
    city_of_birth: str
    country_of_birth: str
    # HD client might need lat/lon directly if geocoding happens upstream
    # For now, let's assume this service does the geocoding like the original main.py
    # latitude: float | None = None
    # longitude: float | None = None


# --- Routers ---

astro_router = APIRouter(prefix="/astro", tags=["Astrology"])
hd_router = APIRouter(prefix="/human-design", tags=["Human Design"])


# --- Helper Function for Parsing Birthdate String ---
def parse_birthdate_string(birthdate_str: str) -> tuple[date, time]:
    """Parses YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS into date and time."""
    try:
        if 'T' in birthdate_str:
            dt_obj = datetime.fromisoformat(birthdate_str)
            return dt_obj.date(), dt_obj.time()
        else:
            # Assume noon if only date is provided
            date_obj = date.fromisoformat(birthdate_str)
            return date_obj, time(12, 0, 0)
    except ValueError:
        raise ValueError(f"Invalid birthdate format. Use YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS. Received: {birthdate_str}")
# --- Helper Function for Location ---
# This logic is duplicated from main.py; consider refactoring into a shared utility if complexity grows
async def get_location_and_timezone(city: str, country: str, birth_date: date, birth_time: time) -> tuple[GeocodingResult, HistoricalTimezoneInfo]: # Corrected type hint
    """Performs geocoding and historical timezone lookup."""
    try:
        logger.info(f"Geocoding location: {city}, {country}")
        geocoding_result = geocoding_service.get_coordinates(city=city, country=country)
        logger.info(f"Geocoding successful: Lat={geocoding_result.latitude}, Lon={geocoding_result.longitude}")

        logger.info(f"Looking up historical timezone for {geocoding_result.latitude}, {geocoding_result.longitude} at {birth_date} {birth_time}")
        timezone_info = timezone_service.get_historical_timezone_info(
            latitude=geocoding_result.latitude,
            longitude=geocoding_result.longitude,
            birth_date=birth_date,
            birth_time=birth_time
        )
        logger.info(f"Timezone lookup successful: {timezone_info.iana_timezone}, Offset: {timezone_info.utc_offset_str}")
        return geocoding_result, timezone_info

    except GeocodingError as e:
        logger.error(f"Geocoding failed: {e}")
        raise HTTPException(status_code=400, detail=f"Could not geocode location '{city}, {country}': {e}")
    except TimezoneError as e:
        logger.error(f"Timezone lookup failed: {e}")
        raise HTTPException(status_code=400, detail=f"Could not determine historical timezone: {e}")
    except Exception as e:
         logger.error(f"Unexpected error during location/timezone lookup: {e}", exc_info=True)
         raise HTTPException(status_code=500, detail=f"Internal server error during location lookup: {e}")


# --- Astrology Endpoint ---

@astro_router.post("/chart", summary="Calculate Astrological Chart Factors")
async def calculate_astrology_chart(request: AstroChartRequest = Body(...)):
    """
    Calculates astrological chart data based on birth details.
    Requires birth date, time, city, and country.
    Performs geocoding and timezone lookup internally.
    """
    logger.info(f"Received astrology chart request for: {request.city_of_birth}")
    try:
        geocoding_result, timezone_info = await get_location_and_timezone(
            city=request.city_of_birth,
            country=request.country_of_birth,
            birth_date=request.birth_date,
            birth_time=request.birth_time
        )

        # Astrology calculation (adapted from main.py)
        astro_chart_data = astro_calc.calculate_chart(
            birth_date=request.birth_date,
            birth_time=request.birth_time,
            latitude=geocoding_result.latitude,
            longitude=geocoding_result.longitude,
            timezone_info=timezone_info
        )
        astro_factors = astro_parser.get_relevant_factors(astro_chart_data) if astro_chart_data else None

        if astro_factors is None:
             logger.warning(f"Astrology calculation returned no data for {request.city_of_birth}")
             # Decide on appropriate response: empty dict, 204, or error?
             # Let's return an empty dict for now, assuming calculation might validly yield nothing.
             return {}

        logger.info(f"Successfully calculated astrology factors for {request.city_of_birth}")
        return astro_factors

    except HTTPException as e:
        # Re-raise HTTPExceptions from get_location_and_timezone
        raise e
    except Exception as e:
        logger.error(f"Error calculating astrology chart for {request.city_of_birth}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error during astrology calculation: {e}")


# --- Human Design Endpoint (Placeholder - to be implemented next) ---

# New GET endpoint for Astrology
@astro_router.get("/chart", summary="Calculate Astrological Chart Factors (GET)") # Changed path to match POST for consistency, but will use query params
async def get_astrology_chart_get(
    birthdate: str = Query(..., description="Birth date and optional time (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)"),
    lat: float = Query(..., description="Latitude"),
    lon: float = Query(..., description="Longitude")
):
    """
    Calculates astrological chart data based on birth date/time and coordinates.
    Requires birthdate (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS), latitude, and longitude.
    Performs timezone lookup internally. Assumes 12:00:00 if time is omitted.
    """
    logger.info(f"Received GET astrology chart request for: {birthdate}, Lat={lat}, Lon={lon}")
    try:
        birth_date_obj, birth_time_obj = parse_birthdate_string(birthdate)

        # Timezone lookup using lat/lon
        logger.info(f"Looking up historical timezone for {lat}, {lon} at {birth_date_obj} {birth_time_obj}")
        timezone_info = timezone_service.get_historical_timezone_info(
            latitude=lat,
            longitude=lon,
            birth_date=birth_date_obj,
            birth_time=birth_time_obj
        )
        logger.info(f"Timezone lookup successful: {timezone_info.iana_timezone}, Offset: {timezone_info.utc_offset_str}")

        # Astrology calculation using lat/lon and looked-up timezone
        astro_chart_data = astro_calc.calculate_chart(
            birth_date=birth_date_obj,
            birth_time=birth_time_obj,
            latitude=lat,
            longitude=lon,
            timezone_info=timezone_info
        )
        astro_factors = astro_parser.get_relevant_factors(astro_chart_data) if astro_chart_data else None

        if astro_factors is None:
             logger.warning(f"Astrology calculation returned no data for {birthdate}, Lat={lat}, Lon={lon}")
             return {} # Return empty dict if no data

        logger.info(f"Successfully calculated astrology factors via GET for {birthdate}")
        return astro_factors

    except ValueError as e: # Catch parsing errors
        logger.error(f"Date parsing error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except TimezoneError as e:
        logger.error(f"Timezone lookup failed: {e}")
        raise HTTPException(status_code=400, detail=f"Could not determine historical timezone: {e}")
    except Exception as e:
        logger.error(f"Error calculating astrology chart via GET: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error during astrology calculation: {e}")
@hd_router.post("/chart", summary="Calculate Human Design Chart Factors")
async def calculate_human_design_chart(request: HumanDesignChartRequest = Body(...)):
    """
    Calculates and interprets Human Design chart data based on birth details.
    Requires birth date, time, city, and country.
    Performs geocoding and timezone lookup internally.
    Calls external HD API and interprets the results.
    """
    logger.info(f"Received human design chart request for: {request.city_of_birth}")
    try:
        # HD calculation needs lat/lon, but the client might also need city/country
        # Let's perform geocoding here to get lat/lon for the client call
        geocoding_result, _ = await get_location_and_timezone( # Timezone not strictly needed by HD client? Check hd_client.py
            city=request.city_of_birth,
            country=request.country_of_birth,
            birth_date=request.birth_date,
            birth_time=request.birth_time
        )

        # Human Design calculation (adapted from main.py)
        hd_chart_data_raw = await hd_client.get_human_design_chart(
            birth_date=request.birth_date,
            birth_time=request.birth_time,
            city_of_birth=request.city_of_birth, # Pass original city/country
            country_of_birth=request.country_of_birth,
            latitude=geocoding_result.latitude, # Pass derived lat/lon
            longitude=geocoding_result.longitude
        )
        hd_interpreted = hd_interpreter.interpret_human_design_chart(hd_chart_data_raw) if hd_chart_data_raw else None

        if hd_interpreted is None:
            logger.warning(f"Human Design interpretation returned no data for {request.city_of_birth}")
            # Return empty dict similar to astrology
            return {}

        logger.info(f"Successfully interpreted human design chart for {request.city_of_birth}")
        return hd_interpreted

    except HTTPException as e:
        # Re-raise HTTPExceptions from get_location_and_timezone
        raise e
    except Exception as e:
        logger.error(f"Error calculating human design chart for {request.city_of_birth}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error during human design calculation: {e}")

# New GET endpoint for Human Design
@hd_router.get("/chart", summary="Calculate Human Design Chart Factors (GET)") # Changed path to match POST
async def get_human_design_chart_get(
    birthdate: str = Query(..., description="Birth date and optional time (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)"),
    lat: float = Query(..., description="Latitude"),
    lon: float = Query(..., description="Longitude")
):
    """
    Calculates and interprets Human Design chart data based on birth date/time and coordinates.
    Requires birthdate (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS), latitude, and longitude.
    Calls external HD API and interprets the results. Assumes 12:00:00 if time is omitted.
    NOTE: The underlying HD client might ideally want city/country, which are not provided here.
    """
    logger.info(f"Received GET human design chart request for: {birthdate}, Lat={lat}, Lon={lon}")
    try:
        birth_date_obj, birth_time_obj = parse_birthdate_string(birthdate)

        # Human Design calculation using lat/lon
        # Assuming hd_client can work sufficiently well without city/country for this GET request
        # If city/country are strictly required by the client, this endpoint needs adjustment (e.g., reverse geocoding)
        hd_chart_data_raw = await hd_client.get_human_design_chart(
            birth_date=birth_date_obj,
            birth_time=birth_time_obj,
            latitude=lat,
            longitude=lon,
            # Omitting city_of_birth and country_of_birth as they are not available
            city_of_birth=None, # Explicitly pass None or omit if client handles it
            country_of_birth=None
        )
        hd_interpreted = hd_interpreter.interpret_human_design_chart(hd_chart_data_raw) if hd_chart_data_raw else None

        if hd_interpreted is None:
            logger.warning(f"Human Design interpretation returned no data via GET for {birthdate}")
            return {}

        logger.info(f"Successfully interpreted human design chart via GET for {birthdate}")
        return hd_interpreted

    except ValueError as e: # Catch parsing errors
        logger.error(f"Date parsing error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e: # Catch potential errors from HD client or interpreter
        logger.error(f"Error calculating human design chart via GET: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error during human design calculation: {e}")

# Include routers in the app
app.include_router(astro_router)
app.include_router(hd_router)

@app.get("/", tags=["Health Check"])
async def read_root():
    """Basic health check for the chart calculation service."""
    logger.info("Chart Calculation Service: Root endpoint '/' accessed")
    return {"status": "ok", "message": "Chart Calculation Service is running."}

# Optional: Add main block for local running if needed
# if __name__ == "__main__":
#     import uvicorn
#     # Note: Running this directly might require adjusting import paths
#     # or ensuring the parent directory ('src') is in PYTHONPATH.
#     logger.info("--- chart_calc/app.py: Running in __main__ block ---")
#     uvicorn.run("src.chart_calc.app:app", host="0.0.0.0", port=8001, reload=True) # Use a different port locally