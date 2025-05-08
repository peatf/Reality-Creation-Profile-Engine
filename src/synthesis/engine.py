# src/synthesis/engine.py
# Core logic for synthesizing insights from the populated knowledge graph.

import logging
import json # Added import
from typing import Dict, Any, Optional, List
from rdflib import Graph, URIRef, Literal
from rdflib.namespace import RDF # Added import

from ..human_design.interpreter import HD_KNOWLEDGE_BASE
from ..astrology.definitions import get_astro_definition

# Import ontology definitions
from ..knowledge_graph import ontology as ont

logger = logging.getLogger(__name__)

# --- Placeholder for Knowledge Graph Access ---
# In a real application, this would interact with a graph database or storage layer.
# For now, we assume the populated graph is passed directly.

# --- Query Functions ---

def find_user_uri(graph: Graph) -> Optional[URIRef]:
    """Finds the main User URI in the graph."""
    for s, p, o in graph.triples((None, RDF.type, ont.User)):
        return s
    return None

def get_typology_pair_key(graph: Graph, user_uri: URIRef) -> Optional[str]:
    """Queries the graph for the user's assessment typology pair key."""
    try:
        profile_uri = graph.value(user_uri, ont.hasProfile)
        if profile_uri:
            typology_uri = graph.value(profile_uri, ont.hasTypology)
            if typology_uri:
                # Assuming the key is stored as part of the URI or a literal name
                # Adjust based on how population.py stores it
                name = graph.value(typology_uri, ont.typologyName)
                if name:
                    # Need to convert name back to key format (e.g., "Grounded Visionary" -> "structured-fluid")
                    # This requires reversing the mapping or storing the key directly.
                    # For now, returning the name as a placeholder.
                    # TODO: Store and retrieve the actual key (e.g., "structured-fluid")
                    return str(name) # Placeholder - return name for now
        return None
    except Exception as e:
        logger.error(f"Error querying typology pair key: {e}", exc_info=True)
        return None

def get_hd_type(graph: Graph, user_uri: URIRef) -> Optional[str]:
    """Queries the graph for the user's Human Design Type."""
    try:
        hd_chart_uri = graph.value(user_uri, ont.hasHumanDesignChart)
        if hd_chart_uri:
            hd_type_uri = graph.value(hd_chart_uri, ont.hasHDType)
            if hd_type_uri:
                type_name = graph.value(hd_type_uri, ont.name)
                return str(type_name) if type_name is not None else None # Check before str()
        return None
    except Exception as e:
        logger.error(f"Error querying HD Type: {e}", exc_info=True)
        return None

def get_astro_placement(graph: Graph, user_uri: URIRef, object_name: str) -> Optional[Dict[str, Any]]:
    """Queries the graph for a specific astrological object's placement."""
    try:
        astro_chart_uri = graph.value(user_uri, ont.hasAstrologyChart)
        if astro_chart_uri:
            # Find the object URI by name
            obj_uri = None
            # Construct the expected URI based on population logic
            expected_obj_uri = ont.RCPE[f"{str(user_uri).split('#')[-1]}_astro_{object_name.replace(' ', '_')}"]
            # Check if the expected URI exists and has the correct type
            if (expected_obj_uri, RDF.type, ont.AstrologicalObject) in graph or \
               (expected_obj_uri, RDF.type, ont.Angle) in graph:
                 obj_uri = expected_obj_uri

            # Fallback search if direct URI construction fails (less efficient)
            if not obj_uri:
                logger.debug(f"Expected URI {expected_obj_uri} not found, searching by name...")
                for s, p, o in graph.triples((None, ont.name, Literal(object_name))):
                     # Check if this object belongs to the user's chart
                     if (astro_chart_uri, ont.hasObject, s) in graph or \
                        (astro_chart_uri, ont.hasAngle, s) in graph:
                         obj_uri = s
                         logger.debug(f"Found object URI by name search: {obj_uri}")
                         break

            if obj_uri:
                sign_literal = graph.value(obj_uri, ont.isInSign)
                house_uri = graph.value(obj_uri, ont.isInHouse)
                house_literal = graph.value(house_uri, ont.houseNumber) if house_uri else None
                lon_literal = graph.value(obj_uri, ont.longitude)

                return {
                    "sign": str(sign_literal) if sign_literal else None,
                    "house": int(house_literal) if house_literal else None,
                    "longitude": float(lon_literal) if lon_literal else None,
                    # TODO: Add aspects later by querying aspects involving obj_uri
                }
        logger.warning(f"Could not find astrology chart or object '{object_name}' for user {user_uri}")
        return None
    except Exception as e:
        logger.error(f"Error querying placement for {object_name}: {e}", exc_info=True)
        return None

# --- Synthesis Rule Logic (Placeholders) ---

def apply_synthesis_rules(graph: Graph, user_uri: URIRef) -> List[Dict[str, Any]]:
    """
    Applies synthesis rules to the populated graph to generate insights.
    This is a placeholder and needs to be implemented based on the defined
    integration strategies and ontology mappings.
    """
    insights = []
    logger.info(f"Applying synthesis rules for user: {user_uri}")

    # Example Rule 1: Combine Assessment Typology and HD Type
    # TODO: Fix get_typology_pair_key to retrieve the actual key
    typology_key_placeholder = get_typology_pair_key(graph, user_uri) # Using name as placeholder key for now
    hd_type = get_hd_type(graph, user_uri)

    if typology_key_placeholder and hd_type:
        hd_type_info = HD_KNOWLEDGE_BASE.get("types", {}).get(hd_type, {})
        strategy = hd_type_info.get("strategy", "following your unique design") # Default text
        signature = hd_type_info.get("signature", "alignment") # Default text

        # Enhanced insight using loaded data
        insight_text = (
            f"As a '{typology_key_placeholder}' with a Human Design Type of '{hd_type}', "
            f"your core approach to manifestation involves {strategy}. "
            f"Leaning into this brings your signature feeling of {signature}."
        )
        insights.append({
            "id": "SYN001",
            "text": insight_text,
            "category": "Core Approach",
            "derived_from": [
                f"Typology: {typology_key_placeholder}",
                f"HD Type: {hd_type} (Strategy: {strategy}, Signature: {signature})"
            ]
        })
        logger.debug(f"Applied Rule SYN001: Typology={typology_key_placeholder}, HD Type={hd_type}")

    # Example Rule 2: Check Sun Sign and Resonance Field Placement
    sun_placement = get_astro_placement(graph, user_uri, "Sun")
    # TODO: Implement get_spectrum_placement function
    # resonance_placement_value = get_spectrum_placement(graph, user_uri, "resonance-field")

    # --- Example of using get_astro_definition (within commented block until resonance_placement is ready) ---
    # if sun_placement and resonance_placement_value:
    #     sun_sign = sun_placement.get("sign")
    #     # Example condition:
    #     if sun_sign == "Leo" and resonance_placement_value == "Expressive":
    #         astro_def = get_astro_definition(f"{sun_sign}") # Get definition for the sign
    #         sign_essence = ""
    #         if astro_def and astro_def.get("definition"):
    #             # Extract a short essence, e.g., first sentence.
    #             sign_essence = ". " + astro_def.get("definition").split('.')[0] + "."

    #         insight_text = (
    #             f"Your {sun_sign} Sun's natural radiance{sign_essence} Combined with your "
    #             f"'{resonance_placement_value}' resonance field, this suggests manifestation "
    #             f"thrives through bold, heart-centered expression."
    #         )
    #         insights.append({
    #             "id": "SYN002",
    #             "text": insight_text,
    #             "category": "Emotional Patterns",
    #             "derived_from": [
    #                 f"Sun Sign: {sun_sign}",
    #                 f"Spectrum Placement: Resonance Field={resonance_placement_value}",
    #                 f"Astro Definition Used: {sun_sign}"
    #             ]
    #         })
    #         logger.debug(f"Applied Rule SYN002: {sun_sign} Sun + {resonance_placement_value} Resonance")
    # --- End Example ---


    # TODO: Implement many more rules based on the research documents and integration strategy.
    # - Map assessment spectrums to Astro/HD factors.
    # - Check for specific Astro aspects influencing HD channels.
    # - Correlate HD Variables with Assessment results.
    # - Link factors to the 5 Manifestation Dimensions.

    if not insights:
        insights.append({
            "id": "SYN_DEFAULT",
            "text": "Further synthesized insights require more detailed rule implementation based on your unique profile combination.",
            "category": "General",
            "derived_from": []
        })

    logger.info(f"Generated {len(insights)} synthesized insights.")
    return insights

# --- Main Synthesis Function ---

def generate_synthesized_insights(graph: Graph) -> Dict[str, Any]:
    """
    Orchestrates the generation of synthesized insights from a user's profile graph.

    Args:
        graph: The populated RDFLib Graph for the user.

    Returns:
        A dictionary containing the list of synthesized insights.
    """
    logger.info("Starting synthesis engine...")
    output = {"synthesized_insights": []}

    user_uri = find_user_uri(graph)
    if not user_uri:
        logger.error("Could not find User URI in the graph. Cannot generate insights.")
        output["error"] = "User not found in graph."
        return output

    try:
        synthesized_insights = apply_synthesis_rules(graph, user_uri)
        output["synthesized_insights"] = synthesized_insights
        logger.info("Synthesis engine finished successfully.")
    except Exception as e:
        logger.error(f"Error during insight synthesis: {e}", exc_info=True)
        output["error"] = "Failed to generate synthesized insights."
        output["synthesized_insights"] = [] # Ensure it's always a list

    return output

# Example Usage (for testing purposes)
if __name__ == '__main__':
    # This example requires a populated graph.
    # We'd typically load a graph generated by population.py here.
    # For now, just showing the structure.
    print("Synthesis Engine Structure:")
    print("- Contains functions to query the graph (get_typology_pair_key, get_hd_type, etc.)")
    print("- Contains apply_synthesis_rules (placeholder for rule logic)")
    print("- Contains generate_synthesized_insights (main orchestration function)")

    # Example of creating a minimal graph and running (will produce default insight)
    from ..knowledge_graph import ontology as ont # Relative import for testing
    g_test = ont.initialize_ontology_graph()
    test_user_uri = ont.RCPE["test_user_1"]
    g_test.add((test_user_uri, RDF.type, ont.User))
    print("\nRunning synthesis on minimal test graph:")
    test_output = generate_synthesized_insights(g_test)
    print(json.dumps(test_output, indent=2))