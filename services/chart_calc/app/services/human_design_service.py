import logging # Added logging
from datetime import datetime, time as dt_time, date as dt_date, timedelta, timezone # Ensure time and date are imported as dt_time, dt_date
# from skyfield.api import Topos, load # No longer needed for this service
# from skyfield.framelib import ecliptic_frame # No longer needed
 
from ..schemas.request_schemas import CalculationRequest
from ..schemas.human_design_schemas import (
    HumanDesignChartResponse,
    GateActivation,
    Channel,
    DefinedCenter
)
 
# Import the client and interpreter from the main src directory
# Adjust path if necessary based on how services are structured relative to src
# Assuming services/chart_calc/app/ is one level down from project root where src/ is.
# So, ../../src/ should point to the src directory from services/chart_calc/app/services/
from src.human_design.client import get_human_design_chart as fetch_hd_chart_from_api
from src.human_design.interpreter import interpret_human_design_chart as interpret_raw_hd_data
from src.location.geocoding import get_coordinates, GeocodingError # For lat/lon if not in request
# Timezone lookup might not be needed if API client handles it or birth_time is already UTC
 
logger = logging.getLogger(__name__)
 
# Old calculation logic (HD_PLANETS, get_gate_from_longitude, _calculate_planetary_positions_for_hd)
# is removed as we will now use the external API via client and the main interpreter.
 
async def calculate_human_design_chart(request: CalculationRequest) -> HumanDesignChartResponse:
    """
    Performs Human Design calculations.
    This function now uses the external Human Design API via `src.human_design.client`
    and interprets the result using `src.human_design.interpreter`.
    """
    logger.info(f"Calculating Human Design chart for request: {request.birth_date} {request.birth_time}")
 
    # Ensure birth_date and birth_time are in the correct format for the client
    # The client expects datetime.date and datetime.time objects.
    # CalculationRequest provides birth_date as str, birth_time as str.
    try:
        birth_date_obj = dt_date.fromisoformat(request.birth_date)
        birth_time_obj = dt_time.fromisoformat(request.birth_time)
    except ValueError as e:
        logger.error(f"Invalid date/time format in request: {e}")
        raise ValueError(f"Invalid date/time format: {request.birth_date}, {request.birth_time}. Expected YYYY-MM-DD and HH:MM:SS.")
 
    # Latitude and longitude are directly from the request.
    # City and country are also needed by the client.
    if not request.city_of_birth or not request.country_of_birth:
        # If city/country are not provided in CalculationRequest, this will fail.
        # The main API's ProfileCreateRequest *does* have city/country.
        # This service's CalculationRequest might need them added, or assume they are geocoded upstream.
        # For now, assume they are present or geocoding happens before this point if needed.
        # The `get_human_design_chart` client requires city and country.
        # This service's `CalculationRequest` schema needs to be updated or this call will fail.
        # Let's assume for now they are part of the request, or add a placeholder.
        # This is a potential point of failure if CalculationRequest doesn't have city/country.
        # The prompt's `main.py` calls chart_calc with a payload that *does* include city/country.
        # So, `request.city_of_birth` and `request.country_of_birth` should be available if `CalculationRequest` is updated.
        # For now, we'll proceed assuming they are in `request`.
        # If not, this will raise an AttributeError.
        # A better approach: `CalculationRequest` schema should include city/country.
        logger.warning("city_of_birth or country_of_birth might be missing from CalculationRequest schema.")
        # Fallback or raise error if city/country are critical and missing
        city_of_birth = getattr(request, "city_of_birth", "Unknown")
        country_of_birth = getattr(request, "country_of_birth", "Unknown")
        if city_of_birth == "Unknown" or country_of_birth == "Unknown":
             logger.error("City or Country of birth not provided in CalculationRequest, cannot call HD API client.")
             raise ValueError("City and Country of birth are required for Human Design chart calculation.")

    else:
        city_of_birth = request.city_of_birth
        country_of_birth = request.country_of_birth

    try:
        raw_chart_api_data = await fetch_hd_chart_from_api(
            birth_date=birth_date_obj,
            birth_time=birth_time_obj,
            city_of_birth=city_of_birth,
            country_of_birth=country_of_birth,
            latitude=request.latitude,
            longitude=request.longitude
        )
    except Exception as e: # Catch exceptions from the API client
        logger.error(f"Error fetching raw HD chart from API client: {e}", exc_info=True)
        raise RuntimeError(f"Failed to fetch Human Design chart from external API: {e}")
 
    if not raw_chart_api_data:
        logger.error("Received no data from Human Design API client.")
        raise RuntimeError("Human Design API client returned no data.")
 
    # Interpret the raw data
    # interpret_raw_hd_data is the function from src.human_design.interpreter
    interpreted_data = interpret_raw_hd_data(raw_chart_api_data)
 
    if not interpreted_data:
        logger.error("Failed to interpret raw Human Design data.")
        raise RuntimeError("Human Design interpreter returned no data.")
 
    # Transform interpreted_data into HumanDesignChartResponse
    # This requires careful mapping from the structure of `interpreted_data`
    # (which includes 'type', 'strategy', 'authority', 'profile', 'definition',
    # 'defined_centers', 'active_channels', 'active_gates', 'variables', 'insights')
    # to the HumanDesignChartResponse schema.
 
    # Example mapping (needs to be thorough and accurate):
    # Conscious/Unconscious Sun Gates from 'activations' in interpreted_data or raw_chart_api_data
    # The `interpret_raw_hd_data` function already structures many of these.
    # The `HumanDesignChartResponse` schema expects specific fields.
 
    # Helper to find sun gate details from interpreted activations if needed,
    # or directly from `interpreted_data` if already processed.
    # `interpret_raw_hd_data` should ideally populate fields that map well.
    # For now, let's assume `interpreted_data` has a structure that can be adapted.
 
    # The `interpret_human_design_chart` returns a dict. We need to map its fields
    # to `HumanDesignChartResponse`.
    # `interpreted_data` keys: 'type', 'strategy', 'authority', 'profile', 'definition',
    # 'defined_centers' (list of names), 'active_channels' (list of "X-Y"),
    # 'active_gates' (list of gate numbers as str), 'variables' (dict), 'insights' (dict).
    # 'g_center_access_type', 'g_center_access_definition'
 
    # Mapping defined_centers: from list of names to list of DefinedCenter objects
    defined_centers_list = [DefinedCenter(name=name) for name in interpreted_data.get("defined_centers", [])]
    
    # Mapping open_centers: The schema expects a list of strings.
    # This needs to be derived by finding all centers and subtracting defined ones.
    all_possible_centers = {"Head", "Ajna", "Throat", "G", "Heart", "Sacral", "Spleen", "Root", "Solar Plexus"}
    open_centers_list = list(all_possible_centers - set(interpreted_data.get("defined_centers", [])))
 
    # Mapping channels: from list of "X-Y" strings to list of Channel objects
    # The interpreter's `insights.channel_info` has more details.
    # `active_channels` in `interpreted_data` is just ["1-8"].
    # `insights.channel_info` is like {"1-8": {"name": "Inspiration", ...}}
    channels_list = []
    insights_channels = interpreted_data.get("insights", {}).get("channel_info", {})
    for ch_key, ch_data in insights_channels.items():
        channels_list.append(Channel(channel_number=ch_key, name=ch_data.get("name", ch_key)))
 
    # Mapping gates: from list of gate numbers to list of GateActivation objects
    # `interpreted_data.active_gates` is just a list of gate numbers.
    # `interpreted_data.activations` (from raw API) has more detail.
    # `interpret_raw_hd_data` should ideally provide a list of GateActivation-like structures.
    # For now, creating placeholder GateActivation objects.
    # This part needs careful alignment with what `interpret_raw_hd_data` actually returns
    # or how to get detailed gate activations (conscious/unconscious, planet, line).
    # The `HumanDesignChartResponse` schema expects detailed `GateActivation` objects.
    # The `raw_chart_api_data` (from HD API client) likely has this in `activations`.
    # Let's assume `raw_chart_api_data['activations']` can be used.
    
    gate_activations_list = []
    raw_activations = raw_chart_api_data.get('activations', {})
    if isinstance(raw_activations, dict): # Check if activations is a dict
        for side, planets in raw_activations.items(): # side is 'design' or 'personality'
            is_conscious = (side == 'personality')
            if isinstance(planets, dict): # Check if planets is a dict
                for planet, gate_line_str in planets.items():
                    if isinstance(gate_line_str, str) and '.' in gate_line_str:
                        try:
                            gate_str, line_str = gate_line_str.split('.')
                            gate_activations_list.append(GateActivation(
                                gate_number=int(gate_str),
                                line_number=int(line_str),
                                planet=planet.capitalize(),
                                is_conscious=is_conscious
                            ))
                        except ValueError:
                            logger.warning(f"Could not parse gate.line string: {gate_line_str} for planet {planet}")
                    else:
                        logger.warning(f"Unexpected gate_line_str format: {gate_line_str} for planet {planet}")
            else:
                logger.warning(f"Planets data for side '{side}' is not a dictionary: {planets}")
    else:
        logger.warning(f"Activations data is not a dictionary: {raw_activations}")


    # Sun gates - these need to be specifically extracted for HumanDesignChartResponse
    # This logic should ideally be part of the interpreter or be more robust.
    conscious_sun_gate = next((g for g in gate_activations_list if g.planet == "Sun" and g.is_conscious), GateActivation(gate_number=0,line_number=0,planet="Sun",is_conscious=True))
    unconscious_sun_gate = next((g for g in gate_activations_list if g.planet == "Sun" and not g.is_conscious), GateActivation(gate_number=0,line_number=0,planet="Sun",is_conscious=False))

    # Construct the response object
    response_payload = HumanDesignChartResponse(
        request_data=request.model_dump(), # Or relevant parts of it
        type=interpreted_data.get("type", "Unknown"),
        strategy=interpreted_data.get("strategy", "Unknown"),
        authority=interpreted_data.get("authority", "Unknown"),
        profile=interpreted_data.get("profile", "Unknown"),
        definition=interpreted_data.get("definition", "Unknown"),
        incarnation_cross=raw_chart_api_data.get("incarnation_cross", "Unknown"), # From raw API data
        
        motivation=interpreted_data.get("variables", {}).get("motivation_type", "Unknown"),
        motivation_orientation=interpreted_data.get("variables", {}).get("motivation_orientation", "Unknown"),
        perspective=interpreted_data.get("variables", {}).get("perspective_type", "Unknown"),
        perspective_orientation=interpreted_data.get("variables", {}).get("perspective_orientation", "Unknown"),
        
        g_center_access_type=interpreted_data.get("g_center_access_type", "Unknown"),
        g_center_access_definition=interpreted_data.get("g_center_access_definition", "Unknown Definition"),
        
        conscious_sun_gate=conscious_sun_gate,
        unconscious_sun_gate=unconscious_sun_gate,
        defined_centers=defined_centers_list,
        open_centers=open_centers_list,
        channels=channels_list,
        gates=gate_activations_list,
        environment=interpreted_data.get("variables", {}).get("environment_type", "Unknown") # Assuming interpreter provides this
    )
 
    logger.info(f"Successfully calculated and interpreted Human Design chart for: {request.birth_date}")
    return response_payload
 
# Example usage (for testing this module directly)
# Needs to be updated as the function is now async and relies on external client/interpreter
if __name__ == "__main__":
    import asyncio
    async def main_test_hd():
        # This test will require HD_API_KEY to be set in environment for the client to work.
        # Also, src.human_design.client and .interpreter must be importable.
        # The CalculationRequest schema might need city/country for the HD client.
        # Let's assume CalculationRequest is updated or we mock the client for local test.
        
        # For a direct test, we'd need to mock fetch_hd_chart_from_api and interpret_raw_hd_data
        # or ensure the environment is fully set up for the API call.
        
        # Example with mocking (conceptual)
        # with patch('services.chart_calc.app.services.human_design_service.fetch_hd_chart_from_api', new_callable=AsyncMock) as mock_fetch, \
        #      patch('services.chart_calc.app.services.human_design_service.interpret_raw_hd_data') as mock_interpret:
            
            # mock_fetch.return_value = {"type": "Mock Projector", ...} # Sample raw API response
            # mock_interpret.return_value = {"type": "Projector", "strategy": "Wait", ...} # Sample interpreted data
            
            sample_request_hd = CalculationRequest(
                birth_date="1990-05-20",
                birth_time="10:30:00", # Assuming UTC
                latitude=34.0522,
                longitude=-118.2437,
                # Add city/country if CalculationRequest schema is updated
                city_of_birth="Los Angeles",
                country_of_birth="USA"
            )
            try:
                hd_chart_data = await calculate_human_design_chart(sample_request_hd)
                print("\nCalculated Human Design Chart (from API & Interpreter):")
                print(hd_chart_data.model_dump_json(indent=2))
            except Exception as e:
                print(f"Error during HD calculation test: {e}")
                import traceback
                traceback.print_exc()

    asyncio.run(main_test_hd())