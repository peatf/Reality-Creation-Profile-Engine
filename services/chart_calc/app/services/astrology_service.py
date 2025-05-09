from datetime import datetime, timezone
from skyfield.api import Topos, load
from skyfield.framelib import ecliptic_frame

from ..schemas.request_schemas import CalculationRequest # Use relative import
from ..schemas.astrology_schemas import AstrologyChartResponse, PlanetaryPosition, HouseCusp, Aspect # Use relative import

# Load ephemeris data (Skyfield will download it if not present)
# Using DE421, a common JPL ephemeris. DE440 or DE441 are newer alternatives.
eph = load('de421.bsp')
ts = load.timescale()

# Define celestial bodies for calculation
# This list can be expanded (e.g., Chiron, Lilith, etc.)
PLANETS_TO_CALCULATE = {
    'Sun': eph['sun'],
    'Moon': eph['moon'],
    'Mercury': eph['mercury barycenter'], # Using barycenter for inner planets
    'Venus': eph['venus barycenter'],
    'Mars': eph['mars barycenter'],
    'Jupiter': eph['jupiter barycenter'],
    'Saturn': eph['saturn barycenter'],
    'Uranus': eph['uranus barycenter'],
    'Neptune': eph['neptune barycenter'],
    'Pluto': eph['pluto barycenter'],
    # 'North Node': # Calculation for True Node is more complex, often derived
}

ZODIAC_SIGNS = [
    "Aries", "Taurus", "Gemini", "Cancer", "Leo", "Virgo",
    "Libra", "Scorpio", "Sagittarius", "Capricorn", "Aquarius", "Pisces"
]

def get_zodiac_sign(longitude_degrees: float) -> tuple[str, float]:
    """Calculates zodiac sign and longitude within the sign."""
    sign_index = int(longitude_degrees // 30)
    sign_longitude = longitude_degrees % 30
    return ZODIAC_SIGNS[sign_index % 12], sign_longitude

async def calculate_astrological_chart(request: CalculationRequest) -> AstrologyChartResponse:
    """
    Performs astrological calculations based on birth details.
    This is a simplified placeholder. Real astrological calculations are complex.
    """
    try:
        birth_dt_str = f"{request.birth_date} {request.birth_time}"
        # Assuming UTC time is provided as per prompt. If local, timezone conversion is needed.
        birth_datetime_naive = datetime.strptime(birth_dt_str, "%Y-%m-%d %H:%M:%S")
        birth_datetime_utc = birth_datetime_naive.replace(tzinfo=timezone.utc)
        t = ts.utc(birth_datetime_utc)
    except ValueError as e:
        # This should ideally be caught by Pydantic validation in main.py or here with more specific error
        raise ValueError(f"Invalid date/time format: {e}")

    # Define observer location
    observer_location = Topos(latitude_degrees=request.latitude, longitude_degrees=request.longitude)
    earth = eph['earth']

    planetary_positions_data: list[PlanetaryPosition] = []

    for name, body in PLANETS_TO_CALCULATE.items():
        astrometric = earth.at(t).observe(body)
        ra, dec, distance = astrometric.radec()
        ecl_lat, ecl_lon, ecl_dist = astrometric.frame_latlon(ecliptic_frame)
        
        # Skyfield longitude is in Angle object, convert to degrees
        longitude_degrees = ecl_lon.degrees
        
        # Calculate speed (simplified - apparent daily motion)
        # For accurate speed, calculate position at t and t+delta_t
        # This is a placeholder for speed calculation
        speed_placeholder = 0.0 
        if name == "Sun": speed_placeholder = 0.985
        elif name == "Moon": speed_placeholder = 13.176
        
        # Placeholder for retrograde - requires comparing speed over time
        is_retrograde = False

        sign, sign_lon = get_zodiac_sign(longitude_degrees)

        planetary_positions_data.append(
            PlanetaryPosition(
                name=name,
                longitude=longitude_degrees,
                latitude=ecl_lat.degrees,
                speed=speed_placeholder, # Placeholder
                sign=sign,
                sign_longitude=sign_lon,
                house=None, # House calculation is separate and complex
                retrograde=is_retrograde # Placeholder
            )
        )

    # Placeholder for North Node (True Node)
    # Calculation is more involved, often using specific algorithms or libraries
    north_node_placeholder = None
    # north_node_placeholder = PlanetaryPosition(name="North Node", longitude=0.0, latitude=0.0, speed=0.0, sign="Aries", sign_longitude=0.0, retrograde=True)


    # Placeholder for House Cusps (e.g., Placidus system)
    # This requires complex calculations based on time, latitude, and chosen house system.
    house_cusps_data: list[HouseCusp] = []
    for i in range(1, 13):
        # Dummy data
        cusp_lon = (i * 30 + request.longitude) % 360 
        sign, sign_lon = get_zodiac_sign(cusp_lon)
        house_cusps_data.append(HouseCusp(house_number=i, longitude=cusp_lon, sign=sign, sign_longitude=sign_lon))
    
    # Placeholder for Aspects
    # Requires iterating through pairs of planets and calculating angular separation.
    aspects_data: list[Aspect] = []
    # Example: Sun trine Moon (dummy)
    # if len(planetary_positions_data) >= 2:
    #     aspects_data.append(Aspect(planet1="Sun", planet2="Moon", aspect_type="Trine", orb=1.0, is_applying=True))


    return AstrologyChartResponse(
        request_data=request.model_dump(),
        planetary_positions=planetary_positions_data,
        house_cusps=house_cusps_data,
        aspects=aspects_data,
        north_node=north_node_placeholder
    )

# Example usage (for testing this module directly)
if __name__ == "__main__":
    import asyncio
    async def main_test():
        sample_request = CalculationRequest(
            birth_date="1990-05-20",
            birth_time="10:30:00", # Assuming UTC
            latitude=34.0522,    # Los Angeles
            longitude=-118.2437
        )
        try:
            chart_data = await calculate_astrological_chart(sample_request)
            print("Calculated Astrology Chart:")
            for planet in chart_data.planetary_positions:
                print(f"  {planet.name}: {planet.longitude:.2f}° ({planet.sign} {planet.sign_longitude:.2f}°), Speed: {planet.speed}, Retro: {planet.retrograde}")
            print("\nHouse Cusps:")
            for cusp in chart_data.house_cusps:
                print(f"  House {cusp.house_number}: {cusp.longitude:.2f}° ({cusp.sign} {cusp.sign_longitude:.2f}°)")
            if chart_data.north_node:
                 print(f"\nNorth Node: {chart_data.north_node.longitude:.2f}° ({chart_data.north_node.sign} {chart_data.north_node.sign_longitude:.2f}°)")
        except Exception as e:
            print(f"Error during calculation: {e}")

    asyncio.run(main_test())