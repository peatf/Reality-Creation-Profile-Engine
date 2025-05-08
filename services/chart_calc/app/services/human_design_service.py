from datetime import datetime, timedelta, timezone
from skyfield.api import Topos, load
from skyfield.framelib import ecliptic_frame

from app.schemas.request_schemas import CalculationRequest # Import from new location
from app.schemas.human_design_schemas import (
    HumanDesignChartResponse,
    GateActivation,
    Channel,
    DefinedCenter
)

# Load ephemeris data (Skyfield will download it if not present)
eph = load('de421.bsp') # Consistent with astrology_service
ts = load.timescale()

# Planets relevant for Human Design (can be adjusted)
# Typically Sun, Earth, Moon, Nodes, Mercury, Venus, Mars, Jupiter, Saturn, Uranus, Neptune, Pluto
HD_PLANETS = {
    'Sun': eph['sun'],
    'Earth': eph['earth'], # Earth is 180 deg opposite Sun
    'Moon': eph['moon'],
    'North Node': None, # Placeholder - Node calculation is complex
    'South Node': None, # Placeholder - Node calculation is complex
    'Mercury': eph['mercury barycenter'],
    'Venus': eph['venus barycenter'],
    'Mars': eph['mars barycenter'],
    'Jupiter': eph['jupiter barycenter'],
    'Saturn': eph['saturn barycenter'],
    'Uranus': eph['uranus barycenter'],
    'Neptune': eph['neptune barycenter'],
    'Pluto': eph['pluto barycenter'],
}

# Simplified mapping of degrees to gates (highly abstract, real mapping is precise)
# Each gate is 360/64 = 5.625 degrees.
# This is a MAJOR simplification. Real HD software uses precise I Ching sequence mapping.
def get_gate_from_longitude(longitude_degrees: float) -> tuple[int, int]:
    """Highly simplified placeholder for gate and line from longitude."""
    gate_span = 360 / 64
    gate_number = int((longitude_degrees % 360) / gate_span) + 1
    
    # Line calculation is even more complex, based on smaller divisions within a gate.
    # Placeholder: divide gate into 6 lines
    line_span = gate_span / 6
    line_in_gate_degrees = (longitude_degrees % 360) % gate_span
    line_number = int(line_in_gate_degrees / line_span) + 1
    return gate_number, min(line_number, 6) # Ensure line is 1-6


async def _calculate_planetary_positions_for_hd(
    dt_utc: datetime,
    latitude: float,
    longitude: float
) -> list[tuple[str, float, float]]:
    """Helper to get planetary ecliptic longitudes for a given datetime."""
    t = ts.utc(dt_utc)
    # observer_location = Topos(latitude_degrees=latitude, longitude_degrees=longitude) # Not strictly needed for geocentric HD planets
    earth_body = eph['earth']
    
    positions = []
    for name, body_obj in HD_PLANETS.items():
        if body_obj is None: # For Nodes, which need special handling
            # Placeholder for Node calculation
            # For simplicity, we'll skip them or use dummy values if not implemented
            if name == "North Node":
                # Dummy value, real calculation needed
                positions.append((name, 0.0, 0.0)) # name, longitude, latitude
            continue

        if name == "Earth": # Earth is geocentric opposite of Sun
            sun_astrometric = earth_body.at(t).observe(eph['sun'])
            _, sun_ecl_lon, _ = sun_astrometric.frame_latlon(ecliptic_frame)
            earth_lon = (sun_ecl_lon.degrees + 180.0) % 360.0
            positions.append((name, earth_lon, 0.0)) # Earth's latitude is 0 by this definition
        else:
            astrometric = earth_body.at(t).observe(body_obj)
            ecl_lat, ecl_lon, _ = astrometric.frame_latlon(ecliptic_frame)
            positions.append((name, ecl_lon.degrees, ecl_lat.degrees))
            
    return positions


async def calculate_human_design_chart(request: CalculationRequest) -> HumanDesignChartResponse:
    """
    Performs Human Design calculations.
    This is a highly simplified placeholder. Real HD calculations are very complex.
    """
    try:
        birth_dt_str = f"{request.birth_date} {request.birth_time}"
        birth_datetime_naive = datetime.strptime(birth_dt_str, "%Y-%m-%d %H:%M:%S")
        birth_datetime_aware_utc = birth_datetime_naive.replace(tzinfo=timezone.utc)
    except ValueError as e:
        raise ValueError(f"Invalid date/time format: {e}")

    # Design date is approximately 88-89 days (or 88 degrees of Sun's travel) before birth
    # A common approximation is 88 days.
    # Ensure design_datetime_utc is also timezone-aware if birth_datetime_aware_utc is.
    design_datetime_aware_utc = birth_datetime_aware_utc - timedelta(days=88) # Simplified

    conscious_positions = await _calculate_planetary_positions_for_hd(
        birth_datetime_aware_utc, request.latitude, request.longitude
    )
    unconscious_positions = await _calculate_planetary_positions_for_hd(
        design_datetime_aware_utc, request.latitude, request.longitude
    )

    all_gate_activations: list[GateActivation] = []
    conscious_sun_gate_details = None
    unconscious_sun_gate_details = None

    for planet_name, lon, _ in conscious_positions:
        gate, line = get_gate_from_longitude(lon)
        act = GateActivation(gate_number=gate, line_number=line, planet=planet_name, is_conscious=True)
        all_gate_activations.append(act)
        if planet_name == "Sun":
            conscious_sun_gate_details = act
            
    for planet_name, lon, _ in unconscious_positions:
        gate, line = get_gate_from_longitude(lon)
        act = GateActivation(gate_number=gate, line_number=line, planet=planet_name, is_conscious=False)
        all_gate_activations.append(act)
        if planet_name == "Sun":
            unconscious_sun_gate_details = act
            
    if not conscious_sun_gate_details: # Should always be found
        conscious_sun_gate_details = GateActivation(gate_number=0, line_number=0, planet="Sun", is_conscious=True) # Fallback
    if not unconscious_sun_gate_details: # Should always be found
        unconscious_sun_gate_details = GateActivation(gate_number=0, line_number=0, planet="Sun", is_conscious=False) # Fallback


    # --- Placeholder logic for Type, Authority, Profile, Centers, Channels ---
    # These are derived from the pattern of defined centers, which come from defined gates forming channels.
    # This is extremely complex and requires a full HD system graph.
    
    # Example: If Sacral center is defined -> Generator/MG. If not -> Non-Sacral.
    # Example: If Solar Plexus is defined and is highest in authority order -> Emotional Authority.
    
    defined_centers_data = [DefinedCenter(name="Sacral"), DefinedCenter(name="Root")] # Dummy
    open_centers_data = ["Head", "Ajna", "Throat", "G", "Heart", "Spleen", "Solar Plexus"] # Dummy
    
    # Channels are formed by two specific gates being activated (one at each end of the channel)
    channels_data = [Channel(channel_number="1-8", name="Inspiration")] # Dummy
    
    # Profile is from lines of Conscious Sun/Earth and Unconscious Sun/Earth
    # E.g. Conscious Sun Line 1, Unconscious Sun Line 3 -> 1/3 Profile
    profile_str = f"{conscious_sun_gate_details.line_number if conscious_sun_gate_details else 'X'}/{unconscious_sun_gate_details.line_number if unconscious_sun_gate_details else 'X'}"


    return HumanDesignChartResponse(
        request_data=request.model_dump(),
        type="Manifesting Generator", # Placeholder
        strategy="Responding", # Placeholder - Derived from Type
        authority="Sacral", # Placeholder - Derived from defined centers and hierarchy
        profile=f"{profile_str} Placeholder Profile Name", # Placeholder - Derived from Sun/Earth gate lines
        definition="Single Definition", # Placeholder - Derived from how defined centers connect
        incarnation_cross="Right Angle Cross of The Sphinx (Placeholder)", # Placeholder - Derived from Sun/Earth gates
        
        # Motivation and Perspective would typically be derived from specific gate activations (e.g., PHS variables)
        # or directly from a more comprehensive HD calculation engine.
        # For now, using placeholders. The orientation (Left/Right) would be parsed from the full term name.
        motivation="Hope – Theist (Left / Strategic)", # Placeholder
        motivation_orientation="Left", # Placeholder - Parsed from the motivation term
        perspective="Possibility (Left – Focused)", # Placeholder
        perspective_orientation="Left", # Placeholder - Parsed from the perspective term
        
        conscious_sun_gate=conscious_sun_gate_details,
        unconscious_sun_gate=unconscious_sun_gate_details,
        defined_centers=defined_centers_data,
        open_centers=open_centers_data,
        channels=channels_data,
        gates=all_gate_activations,
        environment="Caves - Selective (Placeholder)" # Placeholder - PHS variable
    )

# Example usage (for testing this module directly)
if __name__ == "__main__":
    import asyncio
    async def main_test_hd():
        sample_request_hd = CalculationRequest(
            birth_date="1990-05-20",
            birth_time="10:30:00", # Assuming UTC
            latitude=34.0522,
            longitude=-118.2437
        )
        try:
            hd_chart_data = await calculate_human_design_chart(sample_request_hd)
            print("\nCalculated Human Design Chart (Simplified):")
            print(f"  Type: {hd_chart_data.type}")
            print(f"  Authority: {hd_chart_data.authority}")
            print(f"  Profile: {hd_chart_data.profile}")
            print(f"  Conscious Sun: Gate {hd_chart_data.conscious_sun_gate.gate_number}.{hd_chart_data.conscious_sun_gate.line_number}")
            print(f"  Unconscious Sun: Gate {hd_chart_data.unconscious_sun_gate.gate_number}.{hd_chart_data.unconscious_sun_gate.line_number}")
            print(f"  Defined Centers: {[dc.name for dc in hd_chart_data.defined_centers]}")
            print(f"  Channels: {[ch.channel_number for ch in hd_chart_data.channels]}")
            # print("\n  All Gate Activations:")
            # for gate_act in hd_chart_data.gates:
            #     side = "Personality (Conscious)" if gate_act.is_conscious else "Design (Unconscious)"
            #     print(f"    Gate {gate_act.gate_number}.{gate_act.line_number} ({gate_act.planet}) - {side}")

        except Exception as e:
            print(f"Error during HD calculation: {e}")
            import traceback
            traceback.print_exc()

    asyncio.run(main_test_hd())