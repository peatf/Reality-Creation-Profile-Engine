import json
# import os # No longer needed for enginedef.json path
import logging
# import re # No longer needed for parsing term_name
from typing import Dict, Any, Optional, List
from ..constants import KnowledgeBaseKeys
from src.knowledge_graph.loaders import get_hd_definition as load_hd_json_file # Renamed for clarity

logger = logging.getLogger(__name__)

# --- Knowledge Base Structure ---
# Define the expected structure, used for initialization and fallback
DEFAULT_HD_KB_STRUCTURE = {
    KnowledgeBaseKeys.TYPES.value: {},
    KnowledgeBaseKeys.AUTHORITIES.value: {}, # Will be empty if no authorities.json or similar
    KnowledgeBaseKeys.STRATEGIES.value: {}, # Will be empty if no strategies.json or similar
    KnowledgeBaseKeys.PROFILES.value: {},
    KnowledgeBaseKeys.CENTERS.value: {},
    KnowledgeBaseKeys.GATES.value: {},
    KnowledgeBaseKeys.CHANNELS.value: {},
    KnowledgeBaseKeys.VARIABLES.value: {
        KnowledgeBaseKeys.DETERMINATION.value: {},
        KnowledgeBaseKeys.COGNITION.value: {},
        KnowledgeBaseKeys.MOTIVATION.value: {},
        KnowledgeBaseKeys.PERSPECTIVE.value: {},
        KnowledgeBaseKeys.ENVIRONMENT.value: {}
    },
    KnowledgeBaseKeys.MANIFESTATION_MECHANICS.value: {}, # Likely remains empty unless populated from a new file
    KnowledgeBaseKeys.G_CENTER_ACCESS_DETAILS.value: {}
    # Add a key for "crosses" if it's to be formally part of the KB
    # "crosses": {}
}

# --- Knowledge Base Loading and Transformation ---

# Constants like AUTHORITY_MAPPING, HD_TYPES, HD_STRATEGIES,
# MOTIVATION_TERM_NAMES, PERSPECTIVE_TERM_NAMES, VARIABLE_TYPES
# were used for parsing the flat list from enginedef.json.
# If the new JSON files are directly structured (e.g., HD_gates.json is already {"1": def, "2": def}),
# these constants may no longer be needed for the transformation itself.
# They might still be useful for validation or other logic elsewhere.
# For now, they are commented out to reflect their reduced role in loading.

# Mapping for authorities from JSON term_name to internal keys
# This might still be needed if the loaded Authority definitions don't use the desired keys directly
# or if external data uses these term names. Uncommenting for test compatibility.
AUTHORITY_MAPPING = {
    "Emotional Authority": "Emotional",
    "Sacral Authority": "Sacral",
    "Splenic Authority": "Splenic",
    "Ego Authority": "Ego",
    "Self-Projected Authority": "Self-Projected",
    "Environmental Authority": "None (Environmental/Mental)",
    "Lunar Authority": "None (Lunar)"
}
#
# # Known HD Types for direct matching
# HD_TYPES_SET = {"Manifestor", "Generator", "Manifesting Generator", "Projector", "Reflector"} # Renamed to avoid conflict
# HD_STRATEGIES_SET = {"Informing", "Responding", "Waiting for the Invitation", "Waiting a Lunar Cycle"} # Renamed
#
# MOTIVATION_TERM_NAMES = { ... } # Likely not needed if motivation.json is well-structured
# PERSPECTIVE_TERM_NAMES = { ... } # Likely not needed if perspective.json is well-structured
#
# # Known Variable Types for old regex matching
# OLD_VARIABLE_REGEX_KEYS = {
#     KnowledgeBaseKeys.DETERMINATION.value,
#     KnowledgeBaseKeys.COGNITION.value,
#     KnowledgeBaseKeys.ENVIRONMENT.value
# }


def transform_knowledge_base() -> Dict[str, Any]:
    """
    Loads Human Design definitions from individual JSON files in HDDefinitions
    and structures them into the knowledge base.
    """
    # Start with a deep copy of the default structure
    hd_data = {k: (v.copy() if isinstance(v, dict) else v) for k, v in DEFAULT_HD_KB_STRUCTURE.items()}
    if KnowledgeBaseKeys.VARIABLES.value in hd_data and isinstance(hd_data[KnowledgeBaseKeys.VARIABLES.value], dict):
        hd_data[KnowledgeBaseKeys.VARIABLES.value] = {
            k: (v.copy() if isinstance(v, dict) else v)
            for k, v in hd_data[KnowledgeBaseKeys.VARIABLES.value].items()
        }
    else: # Should not happen if DEFAULT_HD_KB_STRUCTURE is correct
        hd_data[KnowledgeBaseKeys.VARIABLES.value] = {}


    logger.info("Loading Human Design definitions from individual JSON files...")

    # Load main categories - assuming JSON files return dicts ready for assignment
    hd_data[KnowledgeBaseKeys.CENTERS.value] = load_hd_json_file("centers") or {}
    hd_data[KnowledgeBaseKeys.GATES.value] = load_hd_json_file("HD_gates") or {}
    hd_data[KnowledgeBaseKeys.CHANNELS.value] = load_hd_json_file("HD_channels") or {}
    hd_data[KnowledgeBaseKeys.PROFILES.value] = load_hd_json_file("profiles") or {}
    hd_data[KnowledgeBaseKeys.G_CENTER_ACCESS_DETAILS.value] = load_hd_json_file("g_center_access") or {}
    
    # Types, Strategies, Authorities
    # Assuming HD_Types.json contains only type definitions.
    # Strategies and Authorities will remain empty unless dedicated files are loaded
    # or HD_Types.json (or another file) is structured to include them and logic is added here to parse.
    hd_data[KnowledgeBaseKeys.TYPES.value] = load_hd_json_file("HD_Types") or {}
    
    # Example if strategies and authorities had their own files:
    # hd_data[KnowledgeBaseKeys.STRATEGIES.value] = load_hd_json_file("strategies") or {}
    # hd_data[KnowledgeBaseKeys.AUTHORITIES.value] = load_hd_json_file("authorities") or {}
    # Since these files are not listed in the prompt, these keys will likely remain empty.

    # Load Variables
    variables_target = hd_data.setdefault(KnowledgeBaseKeys.VARIABLES.value, {}) # Ensure 'variables' key exists
    variables_target[KnowledgeBaseKeys.DETERMINATION.value] = load_hd_json_file("determination") or {}
    variables_target[KnowledgeBaseKeys.COGNITION.value] = load_hd_json_file("cognition") or {}
    variables_target[KnowledgeBaseKeys.MOTIVATION.value] = load_hd_json_file("motivation") or {}
    variables_target[KnowledgeBaseKeys.PERSPECTIVE.value] = load_hd_json_file("perspective") or {}
    variables_target[KnowledgeBaseKeys.ENVIRONMENT.value] = load_hd_json_file("environment") or {}

    # Load Crosses (if this is a new addition to the KB)
    loaded_crosses = load_hd_json_file("crosses")
    if loaded_crosses:
        # Decide where to store crosses, e.g., hd_data["crosses"] = loaded_crosses
        # For now, just log it if not part of DEFAULT_HD_KB_STRUCTURE
        if "crosses" not in hd_data: # Assuming "crosses" is not yet a standard KB key
             logger.info(f"Loaded 'crosses.json' with {len(loaded_crosses)} entries. This category is not in DEFAULT_HD_KB_STRUCTURE.")
             # If you want to add it dynamically:
             # hd_data["crosses"] = loaded_crosses
        else: # If "crosses" was added to DEFAULT_HD_KB_STRUCTURE
             hd_data["crosses"] = loaded_crosses

    logger.info(f"Knowledge base transformation complete. Loaded data for gates: {len(hd_data[KnowledgeBaseKeys.GATES.value])}, channels: {len(hd_data[KnowledgeBaseKeys.CHANNELS.value])}, etc.")
    return hd_data

# Load and transform the knowledge base on module import
HD_KNOWLEDGE_BASE: Dict[str, Any] = {} # Initialize empty

try:
    logger.info("Attempting to load and transform Knowledge Base from individual HDDefinition files...")
    HD_KNOWLEDGE_BASE = transform_knowledge_base()
    logger.info(f"Successfully loaded and transformed Human Design knowledge base.")
    # Perform a quick validation
    if not HD_KNOWLEDGE_BASE.get(KnowledgeBaseKeys.GATES.value) or not HD_KNOWLEDGE_BASE.get(KnowledgeBaseKeys.CENTERS.value):
        logger.warning("HD Knowledge Base might be missing critical data (e.g., gates or centers are empty). Check loader paths and JSON files.")

except Exception as e:
    logger.error(f"Fatal error loading/transforming Human Design knowledge base: {e}", exc_info=True)
    logger.info("Falling back to DEFAULT_HD_KB_STRUCTURE due to loading error.")
    # Deep copy DEFAULT_HD_KB_STRUCTURE on fallback
    HD_KNOWLEDGE_BASE = {k: (v.copy() if isinstance(v, dict) else v) for k, v in DEFAULT_HD_KB_STRUCTURE.items()}
    if KnowledgeBaseKeys.VARIABLES.value in HD_KNOWLEDGE_BASE and isinstance(HD_KNOWLEDGE_BASE[KnowledgeBaseKeys.VARIABLES.value], dict):
        HD_KNOWLEDGE_BASE[KnowledgeBaseKeys.VARIABLES.value] = {
            k: (v.copy() if isinstance(v, dict) else v)
            for k, v in HD_KNOWLEDGE_BASE[KnowledgeBaseKeys.VARIABLES.value].items()
        }


# --- Helper Functions ---

def validate_knowledge_base(kb: Dict[str, Any]) -> bool:
    """Validate knowledge base has required top-level structure."""
    # Check if all main keys defined in the Enum exist in the loaded KB
    # OLD_VARIABLE_REGEX_KEYS is no longer relevant for this validation if it's removed
    required_keys = {k.value for k in KnowledgeBaseKeys if k.value != KnowledgeBaseKeys.VARIABLES.value} # Check top level keys excluding 'variables' itself initially
    required_keys.add(KnowledgeBaseKeys.VARIABLES.value) # Add the main variables key

    # Remove keys that might be conditionally empty if their files don't exist (e.g. authorities, strategies)
    # This makes validation more lenient for those parts if data isn't migrated yet.
    # For now, we expect all keys from DEFAULT_HD_KB_STRUCTURE to be present.

    is_valid = all(key in kb for key in required_keys)
    if not is_valid:
        missing = required_keys - kb.keys()
        logger.warning(f"Knowledge base validation failed. Missing top-level keys: {missing}")
    
    # Validate nested variable structure
    if KnowledgeBaseKeys.VARIABLES.value in kb and isinstance(kb[KnowledgeBaseKeys.VARIABLES.value], dict):
        vars_kb = kb[KnowledgeBaseKeys.VARIABLES.value]
        required_vars_keys = {
            KnowledgeBaseKeys.DETERMINATION.value, KnowledgeBaseKeys.COGNITION.value,
            KnowledgeBaseKeys.MOTIVATION.value, KnowledgeBaseKeys.PERSPECTIVE.value,
            KnowledgeBaseKeys.ENVIRONMENT.value
        }
        vars_valid = all(v_key in vars_kb for v_key in required_vars_keys)
        if not vars_valid:
            missing_vars = required_vars_keys - vars_kb.keys()
            logger.warning(f"Knowledge base validation failed for 'variables' sub-keys. Missing: {missing_vars}")
            is_valid = False # Overall validation fails if variables structure is incomplete
    elif KnowledgeBaseKeys.VARIABLES.value not in kb:
        logger.warning(f"Knowledge base validation failed: '{KnowledgeBaseKeys.VARIABLES.value}' key is missing.")
        is_valid = False


    return is_valid

def get_hd_definition(category: KnowledgeBaseKeys, key: str) -> Optional[Dict[str, Any]]:
    """Safe lookup for HD definitions using Enum keys."""
    if not category or not key:
        return None
    # Handle nested variable lookups specifically if needed, or assume top-level category access
    if category in VARIABLE_TYPES:
         # This function is designed for top-level categories like GATES, CHANNELS etc.
         # Accessing nested variables requires knowing the variable type first.
         # Example: HD_KNOWLEDGE_BASE.get(KnowledgeBaseKeys.VARIABLES.value, {}).get(category.value, {}).get(key)
         logger.warning(f"get_hd_definition called with a variable sub-key '{category.name}'. Use direct nested access for variables.")
         return None # Or implement nested lookup logic here if desired

    # Standard lookup for top-level categories
    return HD_KNOWLEDGE_BASE.get(category.value, {}).get(key)


# --- Main Interpretation Function ---

def interpret_human_design_chart(chart_data: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Interprets the raw Human Design chart data fetched from the API.
    Expects the API response format: a dictionary representing the chart object.

    Args:
        chart_data: The raw chart data dictionary, or None.

    Returns:
        A dictionary containing interpreted insights and key HD factors, or None if input is invalid or None.
    """
    logger.info("Interpreting Human Design chart data...")

    if not chart_data or not isinstance(chart_data, dict):
        logger.error(f"Invalid input: Expected a dictionary for chart_data, received {type(chart_data)}.")
        return None

    interpreted_results = {}

    # Extract key fields from the API response
    hd_type = chart_data.get('type')
    hd_strategy = chart_data.get('strategy') # Added strategy
    hd_authority = chart_data.get('authority')
    hd_profile = chart_data.get('profile')
    definition = chart_data.get('definition')
    defined_centers = chart_data.get('centers', []) # List of defined center names
    active_channels_short = chart_data.get('channels_short', []) # e.g., ["1-8", "13-33"]
    active_channels_long = chart_data.get('channels_long', []) # e.g., ["Inspiration (1-8)"]
    active_gates = chart_data.get('gates', []) # List of active gate numbers as strings
    variables_str = chart_data.get('variables') # e.g., "PRR DRL"
    cognition = chart_data.get('cognition')
    determination = chart_data.get('determination')
    motivation = chart_data.get('motivation')
    transference = chart_data.get('transference')
    perspective = chart_data.get('perspective')
    distraction = chart_data.get('distraction')
    activations = chart_data.get('activations', {}) # Contains design/personality activations

    # Populate interpreted results
    interpreted_results['type'] = hd_type
    interpreted_results['strategy'] = hd_strategy # Added strategy
    interpreted_results['authority'] = hd_authority
    interpreted_results['profile'] = hd_profile
    interpreted_results['definition'] = definition
    interpreted_results['defined_centers'] = defined_centers
    interpreted_results['active_channels'] = active_channels_short # Use short format for consistency
    interpreted_results['active_gates'] = active_gates
    interpreted_results['activations'] = activations # Keep raw activations if needed

    # Determine G Center Access
    g_center_access_info = None
    g_center_access_type_string = None
    if hd_type and defined_centers is not None: # defined_centers can be an empty list
        g_center_is_defined = "G" in defined_centers
        type_for_id = hd_type.lower().replace(" ", "_")
        status_for_id = "defined" if g_center_is_defined else "undefined"

        # Reflectors always have an undefined G center according to the provided JSON
        if type_for_id == "reflector":
            status_for_id = "undefined"

        g_center_access_id = f"g_center_access_{type_for_id}_{status_for_id}"
        
        g_center_access_data = HD_KNOWLEDGE_BASE.get(KnowledgeBaseKeys.G_CENTER_ACCESS_DETAILS.value, {}).get(g_center_access_id)
        if g_center_access_data:
            g_center_access_info = g_center_access_data
            g_center_access_type_string = g_center_access_data.get("subtype", f"{hd_type} {status_for_id.capitalize()}")
            interpreted_results['g_center_access_type'] = g_center_access_type_string
            interpreted_results['g_center_access_definition'] = g_center_access_data.get("definition")

    # Parse Variables string (e.g., "PRR DRL") if present
    # P = Perspective, R = Motivation (Awareness), D = Determination, L = Environment
    # Left/Right indicated by L/R
    variables_parsed = {}
    if variables_str and len(variables_str.split()) == 2:
        pers_mot, det_env = variables_str.split()
        if len(pers_mot) == 3 and len(det_env) == 3:
             variables_parsed = {
                 "perspective_orientation": "Right" if pers_mot[1] == 'R' else "Left",
                 "motivation_orientation": "Right" if pers_mot[2] == 'R' else "Left",
                 "determination_orientation": "Right" if det_env[1] == 'R' else "Left",
                 "environment_orientation": "Right" if det_env[2] == 'R' else "Left",
                 "determination_type": determination, # From separate field
                 "cognition_type": cognition, # From separate field
                 "perspective_type": perspective, # From separate field
                 "motivation_type": motivation, # From separate field
                 "environment_type": None # API doesn't seem to return environment type directly
             }
    interpreted_results['variables'] = variables_parsed

    # --- Add Interpretive Insights from Knowledge Base ---
    insights = {}
    if hd_type:
        insights["type_info"] = get_hd_definition(KnowledgeBaseKeys.TYPES, hd_type) or {}
    if hd_strategy: # Added strategy info
        insights["strategy_info"] = get_hd_definition(KnowledgeBaseKeys.STRATEGIES, hd_strategy) or {}
    if hd_authority:
         # Handle potential variations like "Emotional - Solar Plexus" vs "Emotional"
         auth_key = hd_authority.split('-')[0].strip() if '-' in hd_authority else hd_authority
         insights["authority_info"] = get_hd_definition(KnowledgeBaseKeys.AUTHORITIES, auth_key) or {}
    if hd_profile:
        insights["profile_info"] = get_hd_definition(KnowledgeBaseKeys.PROFILES, hd_profile) or {}

    # Add descriptions for all centers, noting their state (defined/undefined) for the user
    insights["center_info"] = {}
    all_center_keys = [ # These are the keys as stored in HD_KNOWLEDGE_BASE (e.g., "Head", not "Head Center")
        "Head", "Ajna", "Throat", "G", "Heart",
        "Solar Plexus", "Sacral", "Spleen", "Root"
    ]
    user_defined_center_names = set(defined_centers) # For efficient lookup

    for center_key in all_center_keys:
        center_definitions_for_key = HD_KNOWLEDGE_BASE.get(KnowledgeBaseKeys.CENTERS.value, {}).get(center_key, {})
        if isinstance(center_definitions_for_key, dict):
            if center_key in user_defined_center_names:
                # This center is defined for the user
                insights["center_info"][center_key] = center_definitions_for_key.get("Defined", {})
            else:
                # This center is undefined (open) for the user
                insights["center_info"][center_key] = center_definitions_for_key.get("Undefined", {})
        else:
            # Fallback if the structure isn't the new {"Defined": ..., "Undefined": ...}
            # This might happen if a general "CenterName" definition was stored without state
            insights["center_info"][center_key] = center_definitions_for_key # Store whatever was found
            logger.warning(f"Center '{center_key}' in HD_KNOWLEDGE_BASE does not have the expected 'Defined'/'Undefined' structure. Using direct value.")

    # Add descriptions for active channels
    insights["channel_info"] = {}
    for channel_key in active_channels_short:
        insights["channel_info"][channel_key] = get_hd_definition(KnowledgeBaseKeys.CHANNELS, channel_key) or {}

    # Add variable descriptions (using direct nested access as get_hd_definition is for top-level)
    variables_base = HD_KNOWLEDGE_BASE.get(KnowledgeBaseKeys.VARIABLES.value, {})
    insights["variable_info"] = {
        "determination": variables_base.get(KnowledgeBaseKeys.DETERMINATION.value, {}).get(determination) if determination else {}, # Use chart_data.get('determination') as key
        "cognition": variables_base.get(KnowledgeBaseKeys.COGNITION.value, {}).get(cognition) if cognition else {}, # Use chart_data.get('cognition') as key
        "perspective": variables_base.get(KnowledgeBaseKeys.PERSPECTIVE.value, {}).get(perspective) if perspective else {}, # Use chart_data.get('perspective') (full term name) as key
        "motivation": variables_base.get(KnowledgeBaseKeys.MOTIVATION.value, {}).get(motivation) if motivation else {}, # Use chart_data.get('motivation') (full term name) as key
        # Add environment when data is available
        "environment": variables_base.get(KnowledgeBaseKeys.ENVIRONMENT.value, {}).get(variables_parsed.get("environment_type")) if variables_parsed.get("environment_type") else {} # Assuming env type is parsed
    }

    # Add manifestation insights
    insights["manifestation_notes"] = _get_manifestation_insights(hd_type, hd_strategy, hd_authority, variables_parsed)

    if g_center_access_info:
        insights["g_center_access_info"] = g_center_access_info

    interpreted_results['insights'] = insights

    logger.info("Finished interpreting Human Design chart.")
    return interpreted_results

def _get_manifestation_insights(hd_type: Optional[str], hd_strategy_term: Optional[str], hd_authority: Optional[str], variables: Dict[str, Any]) -> List[str]:
    """
    Placeholder function to generate manifestation insights based on HD factors.
    """
    insights = []
    # TODO: Implement richer logic based on HD_KNOWLEDGE_BASE["manifestation_mechanics"]
    # and the Variable mechanics document.

    # Example based on Strategy/Authority:
    # hd_strategy_term is now passed as an argument
    strategy_description = None
    if hd_strategy_term:
        strategy_info = get_hd_definition(KnowledgeBaseKeys.STRATEGIES, hd_strategy_term) or {}
        strategy_description = strategy_info.get("definition") # Or "role_in_manifestation"

    if hd_authority:
        auth_key = hd_authority.split('-')[0].strip()
        auth_info = get_hd_definition(KnowledgeBaseKeys.AUTHORITIES, auth_key) or {}
        authority_description = auth_info.get("definition") # Or "role_in_manifestation"

        if strategy_description and authority_description:
            insights.append(f"Core Manifestation Mechanic: Operate according to your Strategy ({hd_strategy_term}: {strategy_description}) and make decisions using your Authority ({hd_authority}: {authority_description}).")
        elif strategy_description:
            insights.append(f"Core Manifestation Mechanic: Operate according to your Strategy ({hd_strategy_term}: {strategy_description}).")
        elif authority_description: # Should not happen if strategy is always present with type
            insights.append(f"Core Manifestation Mechanic: Make decisions using your Authority ({hd_authority}: {authority_description}).")


    # Example based on Variables:
    if variables:
        det_type = variables.get("determination_type")
        env_type = variables.get("environment_type") # Placeholder
        pers_type = variables.get("perspective_type")
        mot_type = variables.get("motivation_type")

        if det_type:
             insights.append(f"Body Alignment: Following your Determination ({det_type}) supports optimal brain function and energy for manifestation.")
        # Add insights for Environment, Perspective, Motivation based on research docs

    if not insights:
        insights.append("Follow your Strategy and Authority for aligned manifestation.") # Default

    return insights

# Example Usage (for testing purposes)
if __name__ == '__main__':
    # Example raw chart data list (matching API response format)
    test_chart_data_list = [{
        "type": "Projector",
        "profile": "1/3",
        "channels_short": ["1-8", "13-33", "24-61", "28-38", "30-41"],
        "centers": ["Ajna", "G", "Head", "Root", "Solar Plexus", "Spleen", "Throat"],
        "strategy": "Wait for the Invitation",
        "authority": "Emotional", # Assuming API might return just "Emotional"
        "incarnation_cross": "Right Angle Cross of Contagion (8/14 | 30/29)",
        "definition": "Triple Split Definition",
        "signature": "Success",
        "not_self_theme": "Bitterness",
        "cognition": "Inner Vision",
        "determination": "Cold Thirst",
        "variables": "PRR DRL", # P=Pers, R=Motiv | D=Determ, R=Env -> Pers(L) Motiv(R) | Determ(R) Env(L)
        "motivation": "Hope",
        "transference": "Hope", # Should be Fear based on Color 2? API might be simplified.
        "perspective": "Probability",
        "distraction": "Possibility", # Should be Personal based on Color 5? API might be simplified.
        "circuitries": "Individual, Collective Abstract",
        "channels_long": ["Inspiration (1-8)", "The Prodigal (13-33)", "Awareness (24-61)", "Struggle (28-38)", "Recognition (30-41)"],
        "gates": ["8", "14", "19", "33", "41", "24", "22", "21", "39", "61", "58", "38", "1", "30", "29", "13", "7", "28", "15"],
        "activations": {
            "design": {"sun": "30.3", "earth": "29.3", "north_node": "13.4", "south_node": "7.4", "moon": "28.3", "merucy": "41.4", "venus": "61.2", "mars": "38.4", "jupiter": "15.3", "saturn": "61.1", "uranus": "58.5", "nepture": "38.5", "pluto": "1.5"},
            "personality": {"sun": "8.1", "earth": "14.1", "north_node": "19.3", "south_node": "33.3", "moon": "41.1", "mercury": "24.1", "venus": "21.5", "mars": "22.2", "jupiter": "39.1", "saturn": "61.5", "uranus": "58.6", "neptune": "38.6", "pluto": "1.4"}
        }
    }]

    interpreted = interpret_human_design_chart(test_chart_data_list)
    if interpreted:
        print("\n--- Interpreted Human Design Data (Sample) ---")
        import json
        # Exclude raw activations for brevity in example print
        interpreted_to_print = {k: v for k, v in interpreted.items() if k != 'activations'}
        print(json.dumps(interpreted_to_print, indent=2))
    else:
        print("Failed to interpret Human Design chart.")