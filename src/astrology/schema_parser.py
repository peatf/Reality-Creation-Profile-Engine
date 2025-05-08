# Note: This file previously contained logic to parse a JSON schema
# from Astrological Manifestation Analyst Construction.md.
# This functionality has been removed as the primary source for
# astrology definitions is now src/astrology/definitions.py,
# which loads data directly from enginedef.json.
# Any remaining functions in this file serve other purposes within the astrology module.

import logging
from typing import Dict, Any, List # Removed Optional

# Removed: import json, re, os

logger = logging.getLogger(__name__)

# Removed: SCHEMA_FILE_PATH constant
# Removed: PARSED_ASTROLOGY_SCHEMA global variable
# Removed: _extract_and_clean_json function
# Removed: clean_json_string function
# Removed: load_astrology_schema function

# --- Factor Extraction ---

# Mapping Ascendant signs to their traditional rulers
# Note: Modern rulers (Uranus, Neptune, Pluto) could be added if needed
TRADITIONAL_RULERS = {
    "Aries": "Mars",
    "Taurus": "Venus",
    "Gemini": "Mercury",
    "Cancer": "Moon",
    "Leo": "Sun",
    "Virgo": "Mercury",
    "Libra": "Venus",
    "Scorpio": "Mars", # Traditional ruler (Pluto is modern)
    "Sagittarius": "Jupiter",
    "Capricorn": "Saturn",
    "Aquarius": "Saturn", # Traditional ruler (Uranus is modern)
    "Pisces": "Jupiter", # Traditional ruler (Neptune is modern)
}

def get_relevant_factors(chart_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extracts relevant astrological factors from calculated chart data.

    Args:
        chart_data: Dictionary containing calculated chart data (from calculator.py).

    Returns:
        A dictionary containing the extracted factors relevant for synthesis.
    """
    logger.info("Extracting relevant astrological factors...")
    extracted_factors = {}
    # Removed: schema = load_astrology_schema() call

    if not chart_data:
        logger.error("Invalid chart_data provided for factor extraction.")
        return extracted_factors
    # Removed: Check if schema exists (if not schema:)

    # Use default lists directly as schema is no longer loaded
    # Removed: schema_components_list = ...
    planets_to_extract = ["Sun", "Moon", "Mercury", "Venus", "Mars", "Jupiter", "Neptune"] # Removed filtering based on schema
    nodes_to_extract = ["North Node"] # Add South Node if needed
    angles_to_extract = ["Ascendant", "MC"] # MC might not be in schema, but useful context

    # --- Extract Planetary Data ---
    for planet_name in planets_to_extract:
        planet_data = chart_data.get("objects", {}).get(planet_name)
        if planet_data:
            extracted_factors[planet_name] = {
                "sign": planet_data.get("sign"),
                "house": planet_data.get("house"),
                "longitude": planet_data.get("lon"),
                # Aspects need to be calculated separately and added here later
                "aspects": [] # Placeholder
            }
            logger.debug(f"Extracted {planet_name}: Sign={planet_data.get('sign')}, House={planet_data.get('house')}")
        else:
             logger.warning(f"Data for {planet_name} not found in chart_data.")
             extracted_factors[planet_name] = None

    # --- Extract Ascendant Data ---
    # Check both "angles" and "houses" for Ascendant data as structure might vary
    asc_data = chart_data.get("angles", {}).get("Asc")
    if not asc_data:
        # Fallback check in houses if not found in angles
        house1_data = chart_data.get("houses", {}).get("House1")
        if house1_data and house1_data.get("id") == "Asc": # Check if House1 represents Ascendant
             asc_data = house1_data
             logger.debug("Found Ascendant data in 'houses' structure.")

    if asc_data:
        extracted_factors["Ascendant"] = {
            "sign": asc_data.get("sign"),
            "longitude": asc_data.get("lon"),
            # Aspects need to be calculated separately and added here later
            "aspects": [] # Placeholder
        }
        logger.debug(f"Extracted Ascendant: Sign={asc_data.get('sign')}")
    else:
        logger.warning("Data for Ascendant not found in chart_data (checked angles['Asc'] and houses['House1']).")
        extracted_factors["Ascendant"] = None

    # --- Extract North Node Data ---
    nn_data = chart_data.get("north_node") # North Node is now a top-level key
    if nn_data:
        extracted_factors["North Node"] = {
            "sign": nn_data.get("sign"),
            "house": nn_data.get("house"),
            "longitude": nn_data.get("longitude"), # Key is "longitude" in calculator output
            # Aspects need to be calculated separately and added here later
            "aspects": [] # Placeholder
        }
        logger.debug(f"Extracted North Node: Sign={nn_data.get('sign')}, House={nn_data.get('house')}")
    else:
        logger.warning("Data for North Node not found in chart_data.")
        extracted_factors["North Node"] = None

    # --- Extract Chart Ruler ---
    asc_sign = extracted_factors.get("Ascendant", {}).get("sign")
    if asc_sign and asc_sign in TRADITIONAL_RULERS:
        ruler_name = TRADITIONAL_RULERS[asc_sign]
        ruler_data = chart_data.get("objects", {}).get(ruler_name)
        if ruler_data:
            extracted_factors["Chart Ruler"] = {
                "name": ruler_name,
                "sign": ruler_data.get("sign"),
                "house": ruler_data.get("house"),
                "longitude": ruler_data.get("lon"),
                "aspects": [] # Placeholder
            }
            logger.debug(f"Extracted Chart Ruler ({ruler_name}): Sign={ruler_data.get('sign')}, House={ruler_data.get('house')}")
        else:
            logger.warning(f"Data for Chart Ruler ({ruler_name}) not found in chart_data.")
            extracted_factors["Chart Ruler"] = None
    else:
        # Log only if Ascendant sign was actually found but not in rulers dict
        if asc_sign:
             logger.warning(f"Ascendant sign '{asc_sign}' not found in TRADITIONAL_RULERS map.")
        else:
             logger.warning(f"Could not determine Chart Ruler because Ascendant sign is missing.")
        extracted_factors["Chart Ruler"] = None

    # --- TODO: Extract Aspects ---
    # Aspect calculation needs to be implemented (likely in calculator.py or a separate aspect module)
    # Once calculated, aspects should be added to the relevant planet/angle entries above.

    logger.info("Finished extracting relevant factors.")
    return extracted_factors

# Removed: if __name__ == '__main__': block