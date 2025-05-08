import logging
import os
import math
from datetime import datetime, timezone, date, time
from typing import Dict, Any, Optional, List

import numpy as np
import pytz
from jplephem.spk import SPK
from skyfield.api import load, Topos, Loader, N, W, E # Import Topos for location, N/W/E for coordinates
from skyfield.timelib import Time
from skyfield.framelib import ecliptic_frame
from skyfield.almanac import find_discrete, risings_and_settings, moon_nodes # Use find_discrete instead of find_intersections
from skyfield.constants import AU_KM # For distance units if needed later

from ..location.timezone import HistoricalTimezoneInfo # Import the result model

logger = logging.getLogger(__name__)

# --- Constants ---

# JPL Ephemeris file - jplephem can download this automatically if not found locally
# Using DE421 as a common choice, DE440 is newer but larger.
# Determine the project root directory based on the current file's location
# __file__ is src/astrology/calculator.py
# os.path.dirname(__file__) is src/astrology
# os.path.join(..., '..', '..') goes up two levels to the project root
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
EPHEMERIS_FILENAME = 'de421.bsp'
EPHEMERIS_PATH = os.path.join(DATA_DIR, EPHEMERIS_FILENAME)

# Skyfield Loader - explicitly point to the directory containing the ephemeris file
# Check if the data directory exists before creating the loader
if not os.path.isdir(DATA_DIR):
    # Fallback or raise an error if the data directory isn't found
    # For now, let's log a warning and attempt the original path as a fallback,
    # although this might still fail in the test environment.
    # A better approach might be to raise a configuration error.
    logger.warning(f"Data directory not found at calculated path: {DATA_DIR}. Falling back to '/app/data'.")
    DATA_DIR_FOR_LOADER = '/app/data' # Original path as fallback
else:
    DATA_DIR_FOR_LOADER = DATA_DIR

data_loader = Loader(DATA_DIR_FOR_LOADER, verbose=False) # Set verbose=False to potentially reduce download attempts/logs
ts = data_loader.timescale() # Use timescale from our specific loader

# Mapping from our desired object names to Skyfield/jplephem names/indices
# jplephem uses indices for planets (0=Mercury, 1=Venus, 2=Earth-Moon Barycenter, 3=Mars, etc.)
# and names for Sun, Moon, Earth. Skyfield provides convenient names.
SKYFIELD_OBJECTS = {
    "Sun": 'sun',
    "Moon": 'moon',
    "Mercury": 'mercury',
    "Venus": 'venus',
    "Mars": 'mars',
    "Jupiter": 'jupiter barycenter', # Use barycenter for outer planets
    "Saturn": 'saturn barycenter',
    "Uranus": 'uranus barycenter',
    "Neptune": 'neptune barycenter',
    "Pluto": 'pluto barycenter',
    # Nodes require specific calculation, not directly in JPL ephemeris bodies
    "North Node": None, # Placeholder
}

# Define aspects and orbs (degrees) - same as before
ASPECTS = {
    "Conjunction": (0, 8.0),
    "Opposition": (180, 8.0),
    "Square": (90, 7.0),
    "Trine": (120, 7.0),
    "Sextile": (60, 6.0),
}

# Zodiac signs ordered - same as before
SIGNS = ["Aries", "Taurus", "Gemini", "Cancer", "Leo", "Virgo", "Libra",
         "Scorpio", "Sagittarius", "Capricorn", "Aquarius", "Pisces"]

# --- Helper Functions ---

def get_sign(longitude: float) -> str:
    """Determines the zodiac sign for a given longitude."""
    # Normalize longitude to 0-360
    lon_normalized = longitude % 360
    return SIGNS[math.floor(lon_normalized / 30)]

def get_sign_longitude(longitude: float) -> float:
    """Calculates the longitude within the sign (0-30 degrees)."""
    return longitude % 30

def calculate_aspect(lon1: float, lon2: float) -> Optional[Dict[str, Any]]:
    """Calculates the aspect and orb between two longitudes."""
    # Normalize longitudes first
    lon1 %= 360
    lon2 %= 360
    delta = abs(lon1 - lon2)
    if delta > 180:
        delta = 360 - delta

    for name, (angle, orb_val) in ASPECTS.items():
        if abs(delta - angle) <= orb_val:
            orb_precise = abs(delta - angle)
            # TODO: Implement applying/separating check using speeds if needed
            is_applying = None
            return {
                "type": name,
                "orb": round(orb_precise, 2),
                "applying": is_applying
            }
    return None

# --- Main Calculation Function ---

def calculate_chart(
    birth_date: date,
    birth_time: time,
    latitude: float,
    longitude: float,
    timezone_info: HistoricalTimezoneInfo
) -> Optional[Dict[str, Any]]:
    """
    Calculates the astrological chart using jplephem and skyfield.
    Focuses on geocentric ecliptic positions and aspects.
    Uses pre-calculated latitude, longitude, and historically accurate localized datetime.

    Args:
        birth_date: The date of birth.
        birth_time: The time of birth.
        latitude: Geocoded latitude.
        longitude: Geocoded longitude.
        timezone_info: Object containing the IANA timezone and the historically
                       accurate localized datetime.

    Returns:
        A dictionary containing calculated chart data, or None if errors occur.
    """
    logger.info(f"Calculating chart for date: {birth_date}, time: {birth_time}, lat: {latitude:.4f}, lon: {longitude:.4f}, tz: {timezone_info.iana_timezone}")

    try:
        # 1. Load Ephemeris using the explicit loader and filename
        # The Loader was initialized with the DATA_DIR, so we just need the filename.
        planets = data_loader(EPHEMERIS_FILENAME) # Pass just the filename
        earth = planets['earth']
        # Define observer location (geocentric)
        observer = earth

        # 2. Use the pre-localized datetime to create Skyfield Time object (TDB scale)
        # The localized_datetime from timezone_info already has the correct historical offset applied.
        t = ts.from_datetime(timezone_info.localized_datetime)
        logger.debug(f"Using localized datetime: {timezone_info.localized_datetime}")
        logger.debug(f"Skyfield Time (TDB): {t.tdb_strftime('%Y-%m-%d %H:%M:%S')} TDB")

        # Initialize variables for North Node data
        north_node_celestial_data = None
        north_node_output_data = None

        # 3. Calculate Geocentric Ecliptic Positions for Planets
        objects_data = {}
        object_longitudes = {}
        for name, sf_name in SKYFIELD_OBJECTS.items():
            if sf_name is None: # Skip objects like Nodes for now
                 logger.warning(f"Calculation for {name} not implemented yet.")
                 objects_data[name] = None
                 continue
            try:
                # Get the body object from the loaded ephemeris
                body = planets[sf_name]
                # Calculate astrometric position relative to Earth, then get ecliptic coords
                astrometric = observer.at(t).observe(body)
                ecl = astrometric.apparent().ecliptic_latlon(epoch='date') # Use epoch='date' for tropical zodiac

                lon = ecl[1].degrees
                lat = ecl[0].degrees
                # Calculate speed (approximate change over a small time delta, e.g., 1 hour)
                t_plus_hour = ts.tdb(jd=(t.tdb + 1.0/24.0))
                astrometric_plus = observer.at(t_plus_hour).observe(body)
                ecl_plus = astrometric_plus.apparent().ecliptic_latlon(epoch='date')
                lon_plus = ecl_plus[1].degrees
                # Handle longitude wrapping around 360
                delta_lon = (lon_plus - lon + 180) % 360 - 180
                speed = delta_lon * 24.0 # degrees per day

                sign = get_sign(lon)
                sign_lon = get_sign_longitude(lon)

                objects_data[name] = {
                    "id": name,
                    "sign": sign,
                    "sign_lon": round(sign_lon, 2),
                    "lon": round(lon, 4),
                    "lat": round(lat, 4),
                    "speed": round(speed, 4),
                    "house": None # Placeholder - house calc needs separate implementation
                }
                object_longitudes[name] = lon
                logger.debug(f"Calculated {name}: Sign={sign}, Lon={lon:.2f}, Lat={lat:.2f}, Speed={speed:.4f}")

            except Exception as obj_err:
                logger.warning(f"Could not calculate position for {name}: {obj_err}", exc_info=True)
                objects_data[name] = None

        # --- Calculate North Node (Ascending Lunar Node) ---
        # The North Node (True Node) is the ascending node, where the Moon's orbit crosses the ecliptic plane
        # moving from south to north. Skyfield's moon_nodes function returns times of all lunar nodal crossings.
        # We filter for the ascending node (flag == 1) that occurred most recently at or before the birth time.
        t0_nn = ts.tt(jd=t.tt - 30)  # Time window: 30 days before birth time
        t1_nn = ts.tt(jd=t.tt + 30)  # Time window: 30 days after birth time
        logger.debug(f"Searching for lunar nodes between {t0_nn.utc_iso()} and {t1_nn.utc_iso()}")

        try:
            node_times, node_flags = find_discrete(t0_nn, t1_nn, moon_nodes(planets))
            
            # Filter for ascending nodes (flag == 1) at or before the birth time t
            ascending_node_indices = np.where((node_flags == 1) & (node_times.tt <= t.tt))[0]

            if ascending_node_indices.size > 0:
                # Get the most recent ascending node
                latest_ascending_node_index = ascending_node_indices[-1]
                n_node_time = node_times[latest_ascending_node_index]
                logger.info(f"Found North Node (ascending) crossing at: {n_node_time.utc_iso()}")

                # Calculate ecliptic longitude of the Moon at this node time.
                # This gives the longitude of the North Node itself.
                moon_at_node = planets['earth'].at(n_node_time).observe(planets['moon'])
                # Use epoch='date' for tropical zodiac, consistent with other calculations
                ecl_coords = moon_at_node.ecliptic_latlon(epoch='date')
                
                nn_lon = ecl_coords[1].degrees
                # nn_lat = ecl_coords[0].degrees # Latitude of the node is effectively 0 on the ecliptic

                north_node_celestial_data = {
                    "time_obj": n_node_time,  # Skyfield Time object, for potential internal use
                    "time_iso": n_node_time.utc_iso(), # ISO formatted time string for output
                    "lon": nn_lon,
                    "sign": get_sign(nn_lon)
                    # House will be calculated and added later
                }
                logger.debug(f"North Node calculated: Lon={nn_lon:.4f}, Sign={north_node_celestial_data['sign']}, Time={north_node_celestial_data['time_iso']}")
            else:
                logger.warning(f"No ascending (North) lunar node found within ±30 days at or before birth time {t.utc_iso()}. North Node will not be included.")
        except Exception as nn_err:
            logger.error(f"Error calculating North Node: {nn_err}", exc_info=True)
            north_node_celestial_data = None # Ensure it's None on error
        # --- End North Node Calculation ---

        # 4. Calculate Angles (Asc, MC) and Whole Sign Houses
        observer_loc = Topos(latitude_degrees=latitude, longitude_degrees=longitude)
        logger.debug(f"Observer location set: Lat={latitude:.4f}, Lon={longitude:.4f}")

        # Calculate apparent ecliptic position of the Sun for obliquity
        sun_apparent = observer.at(t).observe(planets['sun']).apparent()
        # Correct index for declination is 1, not 2
        ecliptic_obliquity = sun_apparent.radec(epoch='date')[1].degrees # Get obliquity from dec

        # Calculate Local Apparent Sidereal Time (LAST)
        # gmst = Greenwich Mean Sidereal Time
        # equation_of_the_equinoxes = correction from mean to apparent
        # longitude_radians = observer longitude
        # last_radians = t.gast + observer_loc.longitude.radians # gast = gmst + eqeq
        # Skyfield's t.gast is Apparent Sidereal Time at Greenwich
        last_hours = t.gast + longitude / 15.0 # Use the passed longitude (in degrees / 15 deg/hr)
        last_degrees = last_hours * 15.0 % 360
        logger.debug(f"Calculated LAST: {last_degrees:.4f} degrees")

        # Calculate Ascendant and MC using formulas involving LAST and latitude
        # Formulas are complex, involving spherical trigonometry.
        # Using a simplified approach based on common formulas (may need refinement/library for high precision)
        # Ram = Right Ascension of the Midheaven = LAST
        ramc_rad = math.radians(last_degrees)
        lat_rad = math.radians(latitude) # Use the passed latitude
        obl_rad = math.radians(ecliptic_obliquity) # Use obliquity calculated earlier

        # MC Longitude Calculation
        mc_lon = math.degrees(math.atan2(math.sin(ramc_rad), math.cos(ramc_rad) * math.cos(obl_rad) - math.tan(obl_rad) * math.sin(obl_rad))) % 360
        logger.debug(f"Calculated MC Longitude: {mc_lon:.4f}")

        # Ascendant Longitude Calculation
        asc_lon = math.degrees(math.atan2(math.cos(ramc_rad), - (math.sin(ramc_rad) * math.cos(obl_rad) + math.tan(lat_rad) * math.sin(obl_rad)))) % 360
        logger.debug(f"Calculated Ascendant Longitude: {asc_lon:.4f}")

        angles_data = {}
        angles_data["Asc"] = {
            "id": "Asc", "sign": get_sign(asc_lon), "sign_lon": round(get_sign_longitude(asc_lon), 2), "lon": round(asc_lon, 4)
        }
        angles_data["MC"] = {
            "id": "MC", "sign": get_sign(mc_lon), "sign_lon": round(get_sign_longitude(mc_lon), 2), "lon": round(mc_lon, 4)
        }
        logger.info(f"Calculated ASC: {angles_data['Asc']['sign']} {angles_data['Asc']['sign_lon']:.2f}, MC: {angles_data['MC']['sign']} {angles_data['MC']['sign_lon']:.2f}")

        # Calculate Whole Sign House Cusps (Corrected Logic)
        # 1. Find the start of the rising sign (0 degrees of the sign)
        rising_sign_start_lon = math.floor(asc_lon / 30.0) * 30.0

        # 2. Calculate all 12 cusps starting from 0 degrees of the rising sign
        house_cusps_lon = [(rising_sign_start_lon + 30 * i) % 360 for i in range(12)]

        # 3. Populate houses_data dictionary
        houses_data = {}
        for i in range(12):
            house_num = i + 1
            lon = house_cusps_lon[i]
            sign = get_sign(lon)
            # Sign longitude for whole sign houses is always 0 by definition
            sign_lon = 0.0
            houses_data[f'House{house_num}'] = {
                "id": f'House{house_num}',
                "sign": sign,
                "sign_lon": round(sign_lon, 2), # Will be 0.00
                "lon": round(lon, 4)
            }
            logger.debug(f"Calculated Whole Sign House {house_num}: Sign={sign}, Lon={lon:.2f} (Cusp at 0°)")

        # Assign house placements to objects based on the true Whole Sign system
        # Use the first house cusp (0 degrees of the rising sign) as the reference
        first_house_cusp_lon = house_cusps_lon[0] # This is rising_sign_start_lon
        for name, data in objects_data.items():
            if data and 'lon' in data:
                obj_lon = data['lon']
                # Find which 30-degree segment (relative to the FIRST HOUSE CUSP) the object falls into
                house_num = math.floor(((obj_lon - first_house_cusp_lon + 360) % 360) / 30) + 1
                data['house'] = house_num
                logger.debug(f"Assigned House {house_num} to {name} (relative to {first_house_cusp_lon:.2f})")

        # Assign house to North Node if its celestial data was calculated
        if north_node_celestial_data:
            nn_lon_for_house = north_node_celestial_data['lon']
            # Use the same whole sign house logic; first_house_cusp_lon is available here
            nn_house = math.floor(((nn_lon_for_house - first_house_cusp_lon + 360) % 360) / 30) + 1
            
            north_node_output_data = {
                "time": north_node_celestial_data['time_iso'],
                "longitude": round(north_node_celestial_data['lon'], 4),
                "sign": north_node_celestial_data['sign'],
                "house": nn_house
            }
            logger.debug(f"North Node (Lon: {nn_lon_for_house:.2f}) assigned to House {nn_house}")
        else:
            logger.info("North Node data not available, so no house assignment for it.")
            north_node_output_data = None


        # Add angles to longitudes for aspect calculation
        object_longitudes["Asc"] = asc_lon
        object_longitudes["MC"] = mc_lon

        # 5. Calculate Aspects (using calculated longitudes including Asc/MC)
        aspects_data = []
        obj_ids_for_aspects = list(object_longitudes.keys())
        for i in range(len(obj_ids_for_aspects)):
            for j in range(i + 1, len(obj_ids_for_aspects)):
                obj1_id = obj_ids_for_aspects[i]
                obj2_id = obj_ids_for_aspects[j]
                # Ensure both longitudes were successfully calculated
                if obj1_id in object_longitudes and obj2_id in object_longitudes:
                    lon1 = object_longitudes[obj1_id]
                    lon2 = object_longitudes[obj2_id]
                    aspect_info = calculate_aspect(lon1, lon2)
                    if aspect_info:
                        aspect_entry = {
                            "obj1": obj1_id,
                            "obj2": obj2_id,
                            "type": aspect_info["type"],
                            "orb": aspect_info["orb"],
                            "applying": aspect_info["applying"] # Will be None for now
                        }
                        aspects_data.append(aspect_entry)
                        logger.debug(f"Found Aspect: {obj1_id} {aspect_info['type']} {obj2_id} (Orb: {aspect_info['orb']:.2f})")

        chart_result = {
            "objects": objects_data,
            "houses": houses_data, # Placeholder
            "angles": angles_data, # Placeholder
            "aspects": aspects_data,
            "north_node": north_node_output_data # Add North Node data to the chart result
        }

        logger.info(f"Chart calculation successful using jplephem/skyfield. Found {len(aspects_data)} major aspects. North Node included: {'Yes' if north_node_output_data else 'No'}")
        return chart_result

    except Exception as e:
        logger.error(f"Error calculating astrological chart with jplephem/skyfield: {e}", exc_info=True)
        return None

# Example Usage (Commented out as it requires geocoding/timezone lookup first)
# if __name__ == '__main__':
#     # --- This section needs to be updated to use the new services ---
#     # 1. Define city/country
#     # 2. Call get_coordinates(city, country) -> GeocodingResult (lat, lon)
#     # 3. Define birth_date, birth_time
#     # 4. Call get_historical_timezone_info(lat, lon, birth_date, birth_time) -> HistoricalTimezoneInfo (localized_dt, iana_tz)
#     # 5. Call calculate_chart(birth_date, birth_time, lat, lon, timezone_info)
#
#     # Example placeholder data (replace with actual service calls)
#     example_birth_date = date(1990, 5, 15)
#     example_birth_time = time(14, 30, 0)
#     example_latitude = 34.0522  # Los Angeles (example)
#     example_longitude = -118.2437 # Los Angeles (example)
#     # Manually create a timezone_info object for testing (replace with actual call)
#     try:
#         from src.location.timezone import get_historical_timezone_info # Temporary import for testing structure
#         example_tz_info = get_historical_timezone_info(example_latitude, example_longitude, example_birth_date, example_birth_time)
#
#         calculated_chart = calculate_chart(
#             birth_date=example_birth_date,
#             birth_time=example_birth_time,
#             latitude=example_latitude,
#             longitude=example_longitude,
#             timezone_info=example_tz_info
#         )
#
#         if calculated_chart:
#             print("\n--- Calculated Chart Data (jplephem/skyfield) ---")
#             # (Rest of the printing logic remains the same)
#             print("\nObjects:")
#             for obj, data in calculated_chart['objects'].items():
#                 if data:
#                     print(f"  {obj}: Sign={data['sign']}, Lon={data['lon']:.2f}, SignLon={data['sign_lon']:.2f}, Speed={data['speed']:.4f}, House={data.get('house', 'N/A')}")
#                 else:
#                     print(f"  {obj}: Calculation failed or not implemented")
#
#             print("\nHouses (Whole Sign):")
#             for house, data in calculated_chart['houses'].items():
#                  if data:
#                     print(f"  {house}: Sign={data['sign']}, Lon={data['lon']:.2f}")
#                  else:
#                     print(f"  {house}: Calculation failed")
#
#             print("\nAngles:")
#             for angle, data in calculated_chart['angles'].items():
#                  if data:
#                     print(f"  {angle}: Sign={data['sign']}, Lon={data['lon']:.2f}")
#                  else:
#                     print(f"  {angle}: Calculation failed")
#
#             print("\nAspects (Including Angles):")
#             if calculated_chart['aspects']:
#                 for aspect in calculated_chart['aspects']:
#                     print(f"  {aspect['obj1']} {aspect['type']} {aspect['obj2']} (Orb: {aspect['orb']:.2f})")
#             else:
#                 print("  No major aspects found with specified orbs.")
#         else:
#             print("Chart calculation failed.")
#
#     except ImportError:
#         print("Could not import timezone service for example usage.")
#     except Exception as e:
#         print(f"An error occurred during example usage: {e}")