import logging
import uuid
from typing import Dict, Any
from fastapi import FastAPI, HTTPException, Depends, Query # Added Query
from fastapi.middleware.cors import CORSMiddleware
from rdflib import Graph
from sqlalchemy.orm import Session
from sqlalchemy.sql import text # Import text for raw SQL execution
from datetime import datetime # Import datetime
import httpx # Add httpx import

# Import database components
from src.db.database import SessionLocal # Assuming Base is not directly needed here, but SessionLocal is

from src.db.cache import cache_set, cache_get # Import cache functions
# Import models and modules from src
from src.models.input_models import ProfileCreateRequest
# Import new location services
from src.location import geocoding as geocoding_service
from src.location import timezone as timezone_service
from src.location.geocoding import GeocodingError
from src.location.timezone import TimezoneError
from src.knowledge_graph import population as kg_population
from src.synthesis import engine as synthesis_engine
from src.synthesis import narrator as synthesis_narrator # Import narrator
# Calculation/interpretation modules are now in chart_calc service
# Use absolute import from project root since 'services' is a top-level dir
from services.typology_engine import scorer as assessment_scorer
from src.messaging.kafka_client import send_event # Import Kafka client function
from src.routers import typology as typology_router # Import the new typology router
 
# Configure logging VERY early
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.info("--- main.py: Initial imports complete ---") # <-- ADDED LOG

# --- In-Memory Storage (Placeholder) ---
# Replace with persistent storage (e.g., database, file system) in production
# Store a dict containing graph and intermediate results needed for narration
user_profile_data: Dict[str, Dict[str, Any]] = {}
# Example structure:
# {
#   "profile_id_1": {
#       "graph": RDFLib Graph object,
#       "assessment_results": {...}, # Output from assessment_scorer
#       "astro_factors": {...},      # Output from astro_parser
#       "hd_interpreted": {...},     # Output from hd_interpreter
#       "geocoding_result": {...},   # Output from geocoding_service
#       "timezone_info": {...}       # Output from timezone_service
#   }
# }
# -----------------------------------------

logger.info("--- main.py: Defining FastAPI app ---") # <-- ADDED LOG
app = FastAPI(title="Reality Creation Profile Engine - Main API") # Give it a title

# --- HTTP Client for Chart Calc Service ---
# Define the base URL for the chart calculation service
# This assumes 'chart_calc' is resolvable (e.g., via Docker Compose)
CHART_CALC_SERVICE_URL = "http://chart_calc:8001" # Use port 8001 as defined in docker-compose
http_client = httpx.AsyncClient(base_url=CHART_CALC_SERVICE_URL)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)
 
# --- Include Routers ---
app.include_router(typology_router.router, prefix="/api/v1", tags=["typology"])
 
# --- Database Dependency ---
def get_db():
    logger.info("Attempting to create database session.")
    db = SessionLocal()
    logger.info("Database session created.")
    try:
        logger.info("Yielding database session.")
        yield db
        logger.info("Database session yield finished.")
    except Exception as e:
        logger.error(f"Error in get_db during yield: {e}", exc_info=True)
        # Reraise the exception so FastAPI handles it
        raise
    finally:
        logger.info("Closing database session.")
        db.close()
        logger.info("Database session closed.")
# -------------------------

@app.get("/", tags=["Health Check"])
async def read_root():
    """
    Root endpoint for basic health check.
    """
    logger.info("--- main.py: Root endpoint '/' accessed ---")
    # Send an event to Kafka
    event_payload = {"status": "ok", "endpoint": "/", "timestamp": str(datetime.utcnow())}
    send_event("test-events", event_payload)
    logger.info(f"Sent event to Kafka topic 'test-events': {event_payload}")
    return {"status": "ok", "message": "Reality Creation Profile Engine is running.", "event_sent": True}

@app.get("/health/db", tags=["Health Check"])
async def health_check_db(db: Session = Depends(get_db)):
    """
    Performs a database connection health check.
    """
    logger.info("--- main.py: DB Health Check endpoint '/health/db' accessed ---")
    try:
        # Perform a simple query to test DB connection
        result = db.execute(text("SELECT 1")).scalar_one()
        logger.info(f"DB health check successful (SELECT 1 returned: {result})")
        return {"status": "ok", "db_check": result}
    except Exception as e:
        logger.error(f"DB health check failed: {e}", exc_info=True)
        # Raise 503 Service Unavailable if DB connection fails
        raise HTTPException(status_code=503, detail=f"Database connection error: {e}")

@app.get("/health/cache", tags=["Health Check"])
async def health_check_cache():
    """
    Performs a cache connection health check by setting and getting a key.
    """
    logger.info("--- main.py: Cache Health Check endpoint '/health/cache' accessed ---")
    key = "ping"
    value = "pong"
    try:
        set_success = cache_set(key, value, expire_seconds=60) # Set with a short expiry
        if not set_success:
            logger.error("Cache health check failed: Could not set key 'ping'")
            raise HTTPException(status_code=503, detail="Cache error: Failed to set key")

        retrieved_value = cache_get(key)
        logger.info(f"Cache health check: Set '{key}'='{value}', Retrieved='{retrieved_value}'")

        if retrieved_value == value:
            return {"status": "ok", "cache_check": "set_get_successful", "value": retrieved_value}
        else:
            logger.error(f"Cache health check failed: Retrieved value '{retrieved_value}' does not match expected '{value}'")
            raise HTTPException(status_code=503, detail="Cache error: Value mismatch")
    except Exception as e:
        logger.error(f"Cache health check failed with exception: {e}", exc_info=True)
        # Raise 503 Service Unavailable if cache operation fails
        raise HTTPException(status_code=503, detail=f"Cache connection error: {e}")

# --- Chart Calculation Proxy Endpoints ---

@app.get("/astro", tags=["Chart Proxy"])
async def proxy_get_astrology_chart(
    birthdate: str = Query(..., description="Birth date and optional time (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)"),
    lat: float = Query(..., description="Latitude"),
    lon: float = Query(..., description="Longitude")
):
    """
    Proxies GET request for astrology chart calculation to the chart_calc service.
    """
    logger.info(f"Proxying GET /astro request to chart_calc: birthdate={birthdate}, lat={lat}, lon={lon}")
    try:
        # Forward the query parameters directly to the chart_calc service's GET endpoint
        response = await http_client.get(
            "/astro/chart", # Target the GET endpoint in chart_calc
            params={"birthdate": birthdate, "lat": lat, "lon": lon}
        )
        response.raise_for_status() # Raise exception for 4xx/5xx responses
        logger.info(f"Successfully proxied GET /astro request (status: {response.status_code})")
        return response.json() # Return the JSON response from chart_calc

    except httpx.RequestError as exc:
        logger.error(f"HTTP Request error proxying GET /astro: {exc}", exc_info=True)
        raise HTTPException(status_code=503, detail=f"Could not connect to chart calculation service: {exc}")
    except httpx.HTTPStatusError as exc:
        logger.error(f"HTTP Status error from chart_calc service during GET /astro proxy: {exc.response.status_code} - {exc.response.text}", exc_info=True)
        # Forward the status code and detail from the upstream service
        raise HTTPException(status_code=exc.response.status_code, detail=exc.response.json())
    except Exception as e:
         logger.error(f"Unexpected error proxying GET /astro: {e}", exc_info=True)
         raise HTTPException(status_code=500, detail=f"Internal server error proxying chart request: {e}")


@app.get("/human-design", tags=["Chart Proxy"])
async def proxy_get_human_design_chart(
    birthdate: str = Query(..., description="Birth date and optional time (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)"),
    lat: float = Query(..., description="Latitude"),
    lon: float = Query(..., description="Longitude")
):
    """
    Proxies GET request for human design chart calculation to the chart_calc service.
    """
    logger.info(f"Proxying GET /human-design request to chart_calc: birthdate={birthdate}, lat={lat}, lon={lon}")
    try:
        # Forward the query parameters directly to the chart_calc service's GET endpoint
        response = await http_client.get(
            "/human-design/chart", # Target the GET endpoint in chart_calc
            params={"birthdate": birthdate, "lat": lat, "lon": lon}
        )
        response.raise_for_status() # Raise exception for 4xx/5xx responses
        logger.info(f"Successfully proxied GET /human-design request (status: {response.status_code})")
        return response.json() # Return the JSON response from chart_calc

    except httpx.RequestError as exc:
        logger.error(f"HTTP Request error proxying GET /human-design: {exc}", exc_info=True)
        raise HTTPException(status_code=503, detail=f"Could not connect to chart calculation service: {exc}")
    except httpx.HTTPStatusError as exc:
        logger.error(f"HTTP Status error from chart_calc service during GET /human-design proxy: {exc.response.status_code} - {exc.response.text}", exc_info=True)
        # Forward the status code and detail from the upstream service
        raise HTTPException(status_code=exc.response.status_code, detail=exc.response.json())
    except Exception as e:
         logger.error(f"Unexpected error proxying GET /human-design: {e}", exc_info=True)
         raise HTTPException(status_code=500, detail=f"Internal server error proxying chart request: {e}")


# --- Profile Endpoints --- (Existing endpoints follow)
@app.post("/profile/create", tags=["Profile"], status_code=201)
async def create_profile(request_data: ProfileCreateRequest) -> Dict[str, str]:
    """
    Accepts birth data and assessment responses, generates the integrated
    knowledge graph, and returns a profile ID.
    """
    logger.info("--- main.py: '/profile/create' endpoint entered ---") # <-- ADDED LOG
    logger.info(f"Received request data: {request_data.birth_data.birth_date}") # <-- MODIFIED LOG
    try:
        logger.info("Starting data generation...")
        birth_data = request_data.birth_data # Convenience variable

        # --- 1. Geocoding and Timezone Lookup ---
        try:
            logger.info(f"Geocoding location: {birth_data.city_of_birth}, {birth_data.country_of_birth}")
            geocoding_result = geocoding_service.get_coordinates(
                city=birth_data.city_of_birth,
                country=birth_data.country_of_birth
            )
            logger.info(f"Geocoding successful: Lat={geocoding_result.latitude}, Lon={geocoding_result.longitude}")

            logger.info(f"Looking up historical timezone for {geocoding_result.latitude}, {geocoding_result.longitude} at {birth_data.birth_date} {birth_data.birth_time}")
            timezone_info = timezone_service.get_historical_timezone_info(
                latitude=geocoding_result.latitude,
                longitude=geocoding_result.longitude,
                birth_date=birth_data.birth_date,
                birth_time=birth_data.birth_time
            )
            logger.info(f"Timezone lookup successful: {timezone_info.iana_timezone}, Offset: {timezone_info.utc_offset_str}")

        except GeocodingError as e:
            logger.error(f"Geocoding failed: {e}")
            raise HTTPException(status_code=400, detail=f"Could not geocode location '{birth_data.city_of_birth}, {birth_data.country_of_birth}': {e}")
        except TimezoneError as e:
            logger.error(f"Timezone lookup failed: {e}")
            raise HTTPException(status_code=400, detail=f"Could not determine historical timezone: {e}")
        except Exception as e: # Catch unexpected errors during location lookup
             logger.error(f"Unexpected error during location/timezone lookup: {e}", exc_info=True)
             raise HTTPException(status_code=500, detail=f"Internal server error during location lookup: {e}")


        # --- 2. Generate other data components using derived location/time ---
        # Assessment (doesn't depend on location)
        assessment_results = assessment_scorer.generate_complete_results(
            request_data.assessment_responses.typology,
            request_data.assessment_responses.mastery
        )

        # --- Call Chart Calculation Service ---
        astro_factors = None
        hd_interpreted = None
        try:
            # Prepare payload for chart services (matching chart_calc/app.py models)
            chart_request_payload = {
                "birth_date": birth_data.birth_date.isoformat(),
                "birth_time": birth_data.birth_time.isoformat(),
                "city_of_birth": birth_data.city_of_birth,
                "country_of_birth": birth_data.country_of_birth,
            }
            logger.info(f"Calling chart_calc service for Astro: {chart_request_payload}")
            astro_response = await http_client.post("/astro/chart", json=chart_request_payload)
            astro_response.raise_for_status() # Raise exception for 4xx/5xx responses
            astro_factors = astro_response.json()
            logger.info(f"Astro chart calculation successful (status: {astro_response.status_code})")

            logger.info(f"Calling chart_calc service for Human Design: {chart_request_payload}")
            hd_response = await http_client.post("/human-design/chart", json=chart_request_payload)
            hd_response.raise_for_status() # Raise exception for 4xx/5xx responses
            hd_interpreted = hd_response.json()
            logger.info(f"Human Design chart calculation successful (status: {hd_response.status_code})")

        except httpx.RequestError as exc:
            logger.error(f"HTTP Request error calling chart calculation service: {exc}", exc_info=True)
            raise HTTPException(status_code=503, detail=f"Could not connect to chart calculation service: {exc}")
        except httpx.HTTPStatusError as exc:
            logger.error(f"HTTP Status error from chart calculation service: {exc.response.status_code} - {exc.response.text}", exc_info=True)
            # Forward the status code and detail if possible, otherwise use 502 Bad Gateway
            status_code = exc.response.status_code if 400 <= exc.response.status_code < 600 else 502
            detail = f"Error from chart calculation service: {exc.response.text}"
            try:
                # Attempt to parse detail from the upstream error response
                upstream_detail = exc.response.json().get("detail")
                if upstream_detail:
                    detail = f"Chart calculation service error: {upstream_detail}"
            except Exception:
                pass # Keep the original text if JSON parsing fails
            raise HTTPException(status_code=status_code, detail=detail)
        except Exception as e: # Catch unexpected errors during the service call
             logger.error(f"Unexpected error calling chart calculation service: {e}", exc_info=True)
             raise HTTPException(status_code=500, detail=f"Internal server error communicating with chart service: {e}")


        # --- 3. Populate Graph ---
        # Pass the original request and the derived location/timezone results
        # NOTE: The graph population logic might need adjustment if it previously relied
        # on the raw chart data (astro_chart_data, hd_chart_data_raw) which are no longer available here.
        # Assuming kg_population.create_user_profile_graph only needs the request, geo, and timezone.
        # If it needs the calculated factors, they are available in astro_factors and hd_interpreted.
        profile_graph = await kg_population.create_user_profile_graph(
            request=request_data,
            geocoding_result=geocoding_result,
            timezone_info=timezone_info
        )

        if profile_graph:
            # --- Determine Profile ID ---
            # Correctly call find_user_uri from the synthesis engine module
            user_uri_node = synthesis_engine.find_user_uri(profile_graph)
            if not user_uri_node:
                 # Fallback if user URI wasn't found (shouldn't happen ideally)
                 profile_id = f"profile_{uuid.uuid4()}"
                 logger.warning(f"User URI not found in graph, using generated ID: {profile_id}")
            else:
                 # Use the last part of the URI as the ID for simplicity
                 profile_id = str(user_uri_node).split('#')[-1] # Assumes RCPE namespace ends with #
                 if not profile_id or profile_id == str(user_uri_node): # Handle different namespace formats
                     profile_id = str(user_uri_node).split('/')[-1]


            # --- Store all relevant data (Graph + Intermediate Results) ---
            user_profile_data[profile_id] = {
                 "graph": profile_graph,
                 "assessment_results": assessment_results,
                 "astro_factors": astro_factors,
                 "hd_interpreted": hd_interpreted,
                 "geocoding_result": geocoding_result.dict(), # Store as dict
                 "timezone_info": timezone_info.dict()       # Store as dict
            }
            logger.info(f"Profile data created and stored successfully for ID: {profile_id}")
            return {"profile_id": profile_id, "status": "created"}
        else:
            logger.error("Profile graph creation failed.")
            raise HTTPException(status_code=500, detail="Failed to create profile graph.")

    except Exception as e:
        logger.error(f"Error during profile creation: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error during profile creation: {e}")


@app.get("/profile/{profile_id}", tags=["Profile"])
async def get_profile(profile_id: str) -> Dict[str, Any]:
    """
    Retrieves the synthesized insights for a given profile ID.
    """
    logger.info(f"--- main.py: '/profile/{profile_id}' endpoint entered ---") # <-- ADDED LOG
    logger.info(f"Attempting to retrieve data for profile ID: {profile_id}") # <-- MODIFIED LOG

    profile_data_entry = user_profile_data.get(profile_id)

    if not profile_data_entry:
        logger.warning(f"Profile ID not found: {profile_id}")
        raise HTTPException(status_code=404, detail=f"Profile with ID '{profile_id}' not found.")

    profile_graph = profile_data_entry.get("graph")
    if not profile_graph:
         logger.error(f"Graph data missing for profile ID: {profile_id}")
         raise HTTPException(status_code=500, detail="Internal server error: Profile graph data missing.")

    try:
        logger.info(f"Generating synthesized insights for profile ID: {profile_id}")
        synthesized_results = synthesis_engine.generate_synthesized_insights(profile_graph)

        if "error" in synthesized_results:
             logger.error(f"Error during synthesis for profile {profile_id}: {synthesized_results['error']}")
             raise HTTPException(status_code=500, detail=f"Error generating insights: {synthesized_results['error']}")

        logger.info(f"Formatting final output for profile ID: {profile_id}")

        # --- Debugging: Log data being passed to narrator ---
        retrieved_assessment = profile_data_entry.get("assessment_results")
        retrieved_astro = profile_data_entry.get("astro_factors")
        retrieved_hd = profile_data_entry.get("hd_interpreted")
        # Optionally retrieve geocoding/timezone results if narrator needs them
        # retrieved_geo = profile_data_entry.get("geocoding_result")
        # retrieved_tz = profile_data_entry.get("timezone_info")
        logger.debug(f"Data for narrator - Assessment: {type(retrieved_assessment)}")
        logger.debug(f"Data for narrator - Astro: {type(retrieved_astro)}") # Removed Value log for brevity
        logger.debug(f"Data for narrator - HD: {type(retrieved_hd)}")       # Removed Value log for brevity
        # ----------------------------------------------------

        # Pass all necessary data to the narrator (currently doesn't need geo/tz results directly)
        formatted_output = synthesis_narrator.format_synthesized_output(
            profile_id=profile_id,
            synthesis_results=synthesized_results,
            assessment_results=retrieved_assessment, # Use retrieved variable
            astro_factors=retrieved_astro,          # Use retrieved variable
            hd_interpreted=retrieved_hd             # Use retrieved variable
        )

        logger.info(f"Successfully generated and formatted response for profile ID: {profile_id}")
        return formatted_output
    except Exception as e:
        logger.error(f"Error during insight generation for profile {profile_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error during insight generation: {e}")


if __name__ == "__main__":
    import uvicorn
    # Ensure src is in PYTHONPATH or adjust import paths if running directly
    # Better to run with `uvicorn main:app --reload` from the project root directory
    logger.info("--- main.py: Running in __main__ block (for local dev) ---") # <-- ADDED LOG
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)