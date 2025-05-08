# src/knowledge_graph/population.py
# Handles populating the RDF knowledge graph with user profile data.

import logging
import uuid
from typing import Dict, Any, Optional
from rdflib import Graph, Literal, URIRef
from rdflib.namespace import RDF, XSD
from rdflib.namespace import XSD
import asyncio
from datetime import date, time, datetime, timedelta # Import more datetime types

# Import ontology definitions and calculation/interpretation modules
from . import ontology as ont
# Import the new location service result models
from ..location.geocoding import GeocodingResult, GeocodingError
from ..location.timezone import HistoricalTimezoneInfo, TimezoneError
from ..models.input_models import ProfileCreateRequest, AssessmentResponses, BirthData # Keep BirthData for request structure in main and example
from ..astrology import calculator as astro_calc
from ..astrology import schema_parser as astro_parser # To get relevant factors based on schema
from ..human_design import client as hd_client
from ..human_design import interpreter as hd_interpreter
# Use absolute import from project root since 'services' is a top-level dir
from services.typology_engine import scorer as assessment_scorer
from services.typology_engine import results_generator as assessment_results # For result text if needed later

logger = logging.getLogger(__name__)

# Define potential new ontology terms here for clarity (add to ontology.py later)
ont.cityOfBirth = ont.RCPE.cityOfBirth
ont.countryOfBirth = ont.RCPE.countryOfBirth
ont.derivedLatitude = ont.RCPE.derivedLatitude
ont.derivedLongitude = ont.RCPE.derivedLongitude
ont.ianaTimezone = ont.RCPE.ianaTimezone
ont.utcOffsetSeconds = ont.RCPE.utcOffsetSeconds
ont.geocodingFormattedAddress = ont.RCPE.geocodingFormattedAddress


async def create_user_profile_graph(
    request: ProfileCreateRequest,
    geocoding_result: GeocodingResult,
    timezone_info: HistoricalTimezoneInfo
) -> Optional[Graph]:
    """
    Creates and populates an RDF graph for a user profile based on input data
    and pre-calculated location/timezone information.

    Args:
        request: The ProfileCreateRequest containing original birth date/time,
                 city/country, and assessment responses.
        geocoding_result: The result from the geocoding service.
        timezone_info: The result from the historical timezone service.


    Returns:
        An RDFLib Graph populated with the user's profile data, or None if errors occur.
    """
    logger.info("Starting user profile graph creation...")
    # Extract original birth data parts needed
    birth_date = request.birth_data.birth_date
    birth_time = request.birth_data.birth_time
    city_of_birth = request.birth_data.city_of_birth
    country_of_birth = request.birth_data.country_of_birth
    assessment_responses = request.assessment_responses

    # Initialize graph and bind namespaces
    g = ont.initialize_ontology_graph() # Start with base ontology definitions
    user_id = f"user_{uuid.uuid4()}" # Generate a unique ID for the user
    user_uri = ont.RCPE[user_id]
    g.add((user_uri, RDF.type, ont.User))
    # Add original user input location to User node
    g.add((user_uri, ont.cityOfBirth, Literal(city_of_birth)))
    g.add((user_uri, ont.countryOfBirth, Literal(country_of_birth)))
    logger.info(f"Created User URI: {user_uri} for {city_of_birth}, {country_of_birth}")

    # --- 1. Process Astrological Data ---
    try:
        logger.info("Calculating astrological chart using derived data...")
        # Call calculator with derived data
        astro_chart_data = astro_calc.calculate_chart(
            birth_date=birth_date,
            birth_time=birth_time,
            latitude=geocoding_result.latitude,
            longitude=geocoding_result.longitude,
            timezone_info=timezone_info
        )
        if astro_chart_data:
            logger.info("Astrology chart calculated. Populating graph...")
            astro_chart_uri = ont.RCPE[f"{user_id}_astro_chart"]
            g.add((user_uri, ont.hasAstrologyChart, astro_chart_uri))
            g.add((astro_chart_uri, RDF.type, ont.AstrologicalChart))

            # Add derived location/timezone data to the AstrologicalChart node
            g.add((astro_chart_uri, ont.derivedLatitude, Literal(geocoding_result.latitude, datatype=XSD.float)))
            g.add((astro_chart_uri, ont.derivedLongitude, Literal(geocoding_result.longitude, datatype=XSD.float)))
            g.add((astro_chart_uri, ont.ianaTimezone, Literal(timezone_info.iana_timezone)))
            g.add((astro_chart_uri, ont.utcOffsetSeconds, Literal(timezone_info.utc_offset_seconds, datatype=XSD.integer)))
            g.add((astro_chart_uri, ont.geocodingFormattedAddress, Literal(geocoding_result.formatted_address)))


            # Add Objects (Planets, Nodes)
            for obj_name, obj_data in astro_chart_data.get("objects", {}).items():
                if obj_data:
                    obj_uri = ont.RCPE[f"{user_id}_astro_{obj_name.replace(' ', '_')}"]
                    g.add((astro_chart_uri, ont.hasObject, obj_uri))
                    g.add((obj_uri, RDF.type, ont.AstrologicalObject)) # Could specialize (Planet, Node)
                    g.add((obj_uri, ont.name, Literal(obj_name)))
                    g.add((obj_uri, ont.isInSign, Literal(obj_data.get("sign")))) # Link to Sign URI later if needed
                    g.add((obj_uri, ont.longitude, Literal(obj_data.get("lon"), datatype=XSD.float)))
                    if obj_data.get("house"):
                        house_uri = ont.RCPE[f"House_{obj_data.get('house')}"] # Link to generic House URI
                        g.add((obj_uri, ont.isInHouse, house_uri))

            # Add Angles (Asc, MC)
            for angle_name, angle_data in astro_chart_data.get("angles", {}).items():
                 if angle_data:
                    angle_uri = ont.RCPE[f"{user_id}_astro_{angle_name}"]
                    g.add((astro_chart_uri, ont.hasAngle, angle_uri))
                    g.add((angle_uri, RDF.type, ont.Angle))
                    g.add((angle_uri, ont.name, Literal(angle_name)))
                    g.add((angle_uri, ont.isInSign, Literal(angle_data.get("sign"))))
                    g.add((angle_uri, ont.longitude, Literal(angle_data.get("lon"), datatype=XSD.float)))

            # Add House Cusps
            for house_name, house_data in astro_chart_data.get("houses", {}).items():
                 if house_data:
                    house_uri = ont.RCPE[f"{user_id}_astro_{house_name}"]
                    g.add((astro_chart_uri, ont.hasHouseCusp, house_uri))
                    g.add((house_uri, RDF.type, ont.AstrologicalHouse))
                    g.add((house_uri, ont.name, Literal(house_name)))
                    g.add((house_uri, ont.houseNumber, Literal(int(house_name.replace('House','')), datatype=XSD.integer)))
                    g.add((house_uri, ont.isInSign, Literal(house_data.get("sign"))))
                    g.add((house_uri, ont.longitude, Literal(house_data.get("lon"), datatype=XSD.float)))

            # Add Aspects
            for aspect_data in astro_chart_data.get("aspects", []):
                aspect_uri = ont.RCPE[f"{user_id}_aspect_{uuid.uuid4()}"]
                g.add((astro_chart_uri, ont.hasAspect, aspect_uri))
                g.add((aspect_uri, RDF.type, ont.Aspect))
                g.add((aspect_uri, ont.aspectType, Literal(aspect_data.get("type"))))
                g.add((aspect_uri, ont.orb, Literal(aspect_data.get("orb"), datatype=XSD.float)))
                g.add((aspect_uri, ont.isApplying, Literal(aspect_data.get("applying"), datatype=XSD.boolean)))
                # Link involved objects
                obj1_uri = ont.RCPE[f"{user_id}_astro_{aspect_data.get('obj1', '').replace(' ', '_')}"]
                obj2_uri = ont.RCPE[f"{user_id}_astro_{aspect_data.get('obj2', '').replace(' ', '_')}"]
                g.add((aspect_uri, ont.involvesObject, obj1_uri))
                g.add((aspect_uri, ont.involvesObject, obj2_uri))

            logger.info("Astrology data added to graph.")
        else:
            logger.warning("Astrology chart calculation failed or returned no data.")
    except Exception as e:
        logger.error(f"Error processing astrology data: {e}", exc_info=True)

    # --- 2. Process Human Design Data ---
    try:
        logger.info("Fetching Human Design chart using derived data...")
        # Use await here as the client function is async
        # Call client with derived data and original city/country
        hd_chart_data_raw = await hd_client.get_human_design_chart(
            birth_date=birth_date,
            birth_time=birth_time,
            city_of_birth=city_of_birth,
            country_of_birth=country_of_birth,
            latitude=geocoding_result.latitude,
            longitude=geocoding_result.longitude
        )
        if hd_chart_data_raw:
            logger.info("Human Design chart fetched. Interpreting and populating graph...")
            hd_chart_interpreted = hd_interpreter.interpret_human_design_chart(hd_chart_data_raw)

            hd_chart_uri = ont.RCPE[f"{user_id}_hd_chart"]
            g.add((user_uri, ont.hasHumanDesignChart, hd_chart_uri))
            g.add((hd_chart_uri, RDF.type, ont.HumanDesignChart))

            # Add HD Type, Authority, Profile
            if hd_chart_interpreted.get('type'):
                type_uri = ont.RCPE[f"HDType_{hd_chart_interpreted['type'].replace(' ', '_')}"]
                g.add((hd_chart_uri, ont.hasHDType, type_uri))
                g.add((type_uri, RDF.type, ont.HDType))
                g.add((type_uri, ont.name, Literal(hd_chart_interpreted['type'])))
            if hd_chart_interpreted.get('authority'):
                auth_uri = ont.RCPE[f"HDAuthority_{hd_chart_interpreted['authority'].replace(' ', '_')}"]
                g.add((hd_chart_uri, ont.hasHDAuthority, auth_uri))
                g.add((auth_uri, RDF.type, ont.HDAuthority))
                g.add((auth_uri, ont.name, Literal(hd_chart_interpreted['authority'])))
            if hd_chart_interpreted.get('profile'):
                prof_uri = ont.RCPE[f"HDProfile_{hd_chart_interpreted['profile'].replace('/', '_')}"]
                g.add((hd_chart_uri, ont.hasHDProfile, prof_uri))
                g.add((prof_uri, RDF.type, ont.HDProfile))
                g.add((prof_uri, ont.name, Literal(hd_chart_interpreted['profile'])))

            # TODO: Add Centers, Gates, Channels, Variables based on interpreted data structure
            # Example for Centers (assuming 'defined_centers' is a list of names):
            # for center_name in hd_chart_interpreted.get('defined_centers', []):
            #     center_uri = ont.RCPE[f"{user_id}_hd_center_{center_name.replace(' ', '_')}"]
            #     g.add((hd_chart_uri, ont.hasHDCenter, center_uri))
            #     g.add((center_uri, RDF.type, ont.HDCenter))
            #     g.add((center_uri, ont.centerName, Literal(center_name)))
            #     g.add((center_uri, ont.isDefined, Literal(True, datatype=XSD.boolean)))
            # Need to handle undefined centers too.

            logger.info("Human Design data added to graph.")
        else:
            logger.warning("Human Design chart fetch failed or returned no data.")
    except Exception as e:
        logger.error(f"Error processing Human Design data: {e}", exc_info=True)

    # --- 3. Process Assessment Data ---
    try:
        logger.info("Calculating assessment results...")
        # Pass both typology and mastery responses
        assessment_results = assessment_scorer.generate_complete_results(
            assessment_responses.typology,
            assessment_responses.mastery
        )
        if assessment_results:
            logger.info("Assessment results calculated. Populating graph...")
            psych_profile_uri = ont.RCPE[f"{user_id}_psych_profile"]
            g.add((user_uri, ont.hasProfile, psych_profile_uri))
            g.add((psych_profile_uri, RDF.type, ont.PsychologicalProfile))

            # Add Typology Pair
            typology_pair = assessment_results.get('typologyPair', {})
            if typology_pair and typology_pair.get('key'):
                typology_uri = ont.RCPE[f"Typology_{typology_pair['key']}"]
                g.add((psych_profile_uri, ont.hasTypology, typology_uri))
                g.add((typology_uri, RDF.type, ont.AssessmentTypology))
                g.add((typology_uri, ont.typologyName, Literal(typology_pair.get('name', typology_pair['key']))))
                # Link primary/secondary components if needed

            # Add Spectrum Placements
            for spec_id, placement_val in assessment_results.get('spectrumPlacements', {}).items():
                placement_uri = ont.RCPE[f"{user_id}_placement_{spec_id}"]
                g.add((psych_profile_uri, ont.hasSpectrumPlacement, placement_uri))
                g.add((placement_uri, RDF.type, ont.SpectrumPlacement))
                g.add((placement_uri, ont.onSpectrum, ont.RCPE[f"Spectrum_{spec_id}"])) # Link to generic spectrum URI
                g.add((placement_uri, ont.hasPlacementValue, Literal(placement_val)))

            # Add Dominant Mastery Values
            for category, values in assessment_results.get('dominantValues', {}).items():
                 for value in values:
                     mastery_uri = ont.RCPE[f"MasteryValue_{value}"]
                     g.add((psych_profile_uri, ont.hasDominantMasteryValue, mastery_uri))
                     g.add((mastery_uri, RDF.type, ont.MasteryValue))
                     g.add((mastery_uri, ont.value, Literal(value)))
                     # Could add property linking to category (e.g., ont.masteryCategory Literal(category))

            # Add Energy Focus
            energy_focus = assessment_results.get('energyFocus', {})
            if energy_focus:
                 focus_str = f"Expansion: {energy_focus.get('expansionScore', 50)}%, Contraction: {energy_focus.get('contractionScore', 50)}%"
                 g.add((psych_profile_uri, ont.hasEnergyFocus, Literal(focus_str)))


            logger.info("Assessment data added to graph.")
        else:
            logger.warning("Assessment results calculation failed or returned no data.")
    except Exception as e:
        logger.error(f"Error processing assessment data: {e}", exc_info=True)


    logger.info(f"User profile graph created successfully with {len(g)} triples.")
    return g

# Example Usage (Updated for new function signature)
if __name__ == '__main__':
    import asyncio
    from ..models.input_models import BirthData, AssessmentResponses # Keep BirthData for request structure
    from datetime import date, time, datetime, timedelta # Import more datetime types

    # Sample Input Data
    # Create BirthData with new fields for the request object
    test_birth_data = BirthData(
        birth_date=date(1990, 5, 15), # Use date object
        birth_time=time(14, 30, 0),   # Use time object
        city_of_birth="Los Angeles",
        country_of_birth="USA"
    )
    test_assessment_responses = AssessmentResponses(
        typology={
            'cognitive-q1': 'left', 'cognitive-q2': 'balanced',
            'perceptual-q1': 'right', 'perceptual-q2': 'right',
            'kinetic-q1': 'left', 'kinetic-q2': 'left',
            'choice-q1': 'balanced', 'choice-q2': 'left',
            'resonance-q1': 'right', 'resonance-q2': 'balanced',
            'rhythm-q1': 'balanced', 'rhythm-q2': 'right'
        },
        mastery={
            'core-q1': 'creative-expression', 'core-q2': 'craft-mastery', 'core-q3': 'passion-inspiration',
            'growth-q1': 'action-challenge', 'growth-q2': 'action-gap', 'growth-q3': 'momentum-resistance',
            'alignment-q1': 'accept-cycles', 'alignment-q2': 'control-consistency',
            'energy-q1': 'intuitive-instincts', 'energy-q2': 'rigid-routines', 'energy-q3': 'spontaneous-productivity', 'energy-q4': 'dynamic-environment'
        }
    )
    test_request = ProfileCreateRequest(
        birth_data=test_birth_data,
        assessment_responses=test_assessment_responses
    )

    async def run_test():
        print("Creating user profile graph (with dummy location/timezone data)...")

        # Create dummy geocoding/timezone results for the new signature
        dummy_geo_result = GeocodingResult(latitude=34.0522, longitude=-118.2437, formatted_address="Los Angeles, CA, USA")

        # Create a dummy localized datetime for timezone_info
        # This requires pytz for a realistic example
        try:
            import pytz
            la_tz = pytz.timezone('America/Los_Angeles')
            # Combine date and time from test_birth_data
            naive_dt = datetime.combine(test_birth_data.birth_date, test_birth_data.birth_time)
            localized_dt = la_tz.localize(naive_dt)
            offset_seconds = int(localized_dt.utcoffset().total_seconds())
            dummy_iana_tz = 'America/Los_Angeles'
        except ImportError:
            # Fallback if pytz not available in test environment
            print("Warning: pytz not found, using simplified dummy timezone info.")
            localized_dt = datetime.now() # Very basic fallback
            offset_seconds = -25200 # Example offset for LA PDT (-7 hours)
            dummy_iana_tz = 'Etc/GMT+7' # Example fallback IANA

        dummy_tz_info = HistoricalTimezoneInfo(
            iana_timezone=dummy_iana_tz,
            localized_datetime=localized_dt,
            utc_offset_seconds=offset_seconds
        )

        # Call the updated function
        profile_graph = await create_user_profile_graph(
            request=test_request,
            geocoding_result=dummy_geo_result,
            timezone_info=dummy_tz_info
        )

        if profile_graph:
            print(f"\n--- Generated Profile Graph ({len(profile_graph)} triples) ---")
            # Print graph in Turtle format for readability
            print(profile_graph.serialize(format="turtle"))
        else:
            print("Failed to create profile graph.")

    asyncio.run(run_test())