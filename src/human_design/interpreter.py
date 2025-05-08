import json
import os
import logging
import re
from typing import Dict, Any, Optional, List
from ..constants import KnowledgeBaseKeys # Added import

logger = logging.getLogger(__name__)

# --- Knowledge Base Structure ---
# Define the expected structure, used for initialization and fallback
DEFAULT_HD_KB_STRUCTURE = {
    KnowledgeBaseKeys.TYPES.value: {},
    KnowledgeBaseKeys.AUTHORITIES.value: {},
    KnowledgeBaseKeys.STRATEGIES.value: {},
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
    KnowledgeBaseKeys.MANIFESTATION_MECHANICS.value: {},
    KnowledgeBaseKeys.G_CENTER_ACCESS_DETAILS.value: {}
}

# --- Knowledge Base Loading and Transformation ---

# Mapping for authorities from JSON term_name to internal keys
AUTHORITY_MAPPING = {
    "Emotional Authority": "Emotional",
    "Sacral Authority": "Sacral",
    "Splenic Authority": "Splenic",
    "Ego Authority": "Ego",
    "Self-Projected Authority": "Self-Projected",
    "Environmental Authority": "None (Environmental/Mental)",
    "Lunar Authority": "None (Lunar)"
}

# Known HD Types for direct matching
HD_TYPES = {"Manifestor", "Generator", "Manifesting Generator", "Projector", "Reflector"}
HD_STRATEGIES = {"Informing", "Responding", "Waiting for the Invitation", "Waiting a Lunar Cycle"}

MOTIVATION_TERM_NAMES = {
    "Fear – Communalist (Left / Strategic)", "Fear – Separatist (Right / Receptive)",
    "Hope – Theist (Left / Strategic)", "Hope – Antitheist (Right / Receptive)",
    "Desire – Leader (Left / Strategic)", "Desire – Follower (Right / Receptive)",
    "Need – Master (Left / Strategic)", "Need – Novice (Right / Receptive)",
    "Guilt – Conditioner (Left / Strategic)", "Guilt – Conditioned (Right / Receptive)",
    "Innocence – Observer (Left / Strategic)", "Innocence – Observed (Right / Receptive)"
}

PERSPECTIVE_TERM_NAMES = {
    "Survival (Left – Focused)", "Survival (Right – Peripheral)",
    "Possibility (Left – Focused)", "Possibility (Right – Peripheral)",
    "Perspective (Left – Focused)", "Perspective (Right – Peripheral)",
    "Understanding (Left – Focused)", "Understanding (Right – Peripheral)",
    "Evaluation (Left – Focused)", "Evaluation (Right – Peripheral)",
    "Judgment (Left – Focused)", "Judgment (Right – Peripheral)"
}

# Known Variable Types for nesting (original categories like "Determination - X")
VARIABLE_TYPES = {
    KnowledgeBaseKeys.DETERMINATION.value,
    KnowledgeBaseKeys.COGNITION.value,
    KnowledgeBaseKeys.ENVIRONMENT.value
    # Motivation and Perspective are now handled by specific term matching
    # but we keep them here if some old format "Motivation - X" might still exist.
    # If not, they can be removed from this set.
    # For now, let's assume the new terms are primary and the regex is secondary.
    # KnowledgeBaseKeys.MOTIVATION.value, # Handled by MOTIVATION_TERM_NAMES
    # KnowledgeBaseKeys.PERSPECTIVE.value # Handled by PERSPECTIVE_TERM_NAMES
}

def transform_knowledge_base(definitions: Dict[str, Any]) -> Dict[str, Any]:
    """Transform raw JSON definitions into structured knowledge base."""
    # Start with a deep copy of the default structure to avoid modifying it
    hd_data = {k: (v.copy() if isinstance(v, dict) else v) for k, v in DEFAULT_HD_KB_STRUCTURE.items()}
    hd_data[KnowledgeBaseKeys.VARIABLES.value] = {
        k: v.copy() for k, v in DEFAULT_HD_KB_STRUCTURE[KnowledgeBaseKeys.VARIABLES.value].items()
    }

    # Process the new top-level "centers" array first
    raw_centers = definitions.get("centers", [])
    if raw_centers: # Ensure it's not None and is iterable
        logger.info(f"Processing {len(raw_centers)} raw Center definitions from top-level 'centers' array...")
        for center_data in raw_centers:
            name = center_data.get("center_name")
            defined_data = center_data.get("defined")
            undefined_data = center_data.get("undefined")

            if name and defined_data is not None and undefined_data is not None:
                hd_data[KnowledgeBaseKeys.CENTERS.value].setdefault(name, {})
                hd_data[KnowledgeBaseKeys.CENTERS.value][name]["Defined"] = defined_data
                hd_data[KnowledgeBaseKeys.CENTERS.value][name]["Undefined"] = undefined_data
            else:
                logger.warning(f"Skipping center due to missing 'center_name', 'defined', or 'undefined' data: {center_data}")

    hd_definitions = definitions.get("humanDesign", []) # These are other definitions like gates, channels etc.
    logger.info(f"Processing {len(hd_definitions)} raw Human Design definitions from 'humanDesign' array...")

    for item in hd_definitions:
        term = item.get("term_name")
        if not term:
            logger.warning("Found definition item without 'term_name'. Skipping.")
            continue

        # Keep all other fields like 'definition', 'role_in_manifestation', etc.
        definition_data = {k: v for k, v in item.items() if k != "term_name"}
        matched = False

        # Gates (e.g., "Gate 1")
        gate_match = re.match(r"^Gate (\d+)$", term)
        if gate_match:
            gate_num = gate_match.group(1)
            hd_data[KnowledgeBaseKeys.GATES.value][gate_num] = definition_data
            matched = True
            continue

        # Channels (e.g., "Channel 1–8", "Channel 34-20")
        channel_match = re.match(r"^Channel (\d+)[–-](\d+)", term)
        if channel_match:
            ch_key = f"{channel_match.group(1)}-{channel_match.group(2)}" # Standardize
            hd_data[KnowledgeBaseKeys.CHANNELS.value][ch_key] = definition_data
            matched = True
            continue

        # Profiles (e.g., "Profile 1/3")
        profile_match = re.match(r"^Profile (\d/\d)$", term)
        if profile_match:
            prof_key = profile_match.group(1)
            hd_data[KnowledgeBaseKeys.PROFILES.value][prof_key] = definition_data
            matched = True
            continue

        # Types (Exact match)
        if term in HD_TYPES:
            hd_data[KnowledgeBaseKeys.TYPES.value][term] = definition_data
            matched = True
            continue

        # Strategies (Exact match)
        if term in HD_STRATEGIES:
            hd_data[KnowledgeBaseKeys.STRATEGIES.value][term] = definition_data
            matched = True
            continue

        # Authorities (Map from JSON name)
        if term in AUTHORITY_MAPPING:
            auth_key = AUTHORITY_MAPPING[term]
            hd_data[KnowledgeBaseKeys.AUTHORITIES.value][auth_key] = definition_data
            matched = True
            continue

        # New Motivation Terms (Exact match)
        if term in MOTIVATION_TERM_NAMES:
            hd_data[KnowledgeBaseKeys.VARIABLES.value].setdefault(KnowledgeBaseKeys.MOTIVATION.value, {})[term] = definition_data
            matched = True
            continue

        # New Perspective Terms (Exact match)
        if term in PERSPECTIVE_TERM_NAMES:
            hd_data[KnowledgeBaseKeys.VARIABLES.value].setdefault(KnowledgeBaseKeys.PERSPECTIVE.value, {})[term] = definition_data
            matched = True
            continue

        # Centers are now processed from the top-level "centers" array.
        # The old logic below looking for "X Center" with "state" in "humanDesign" items is now redundant.
        # if " Center" in term and item.get("state"):
        #     ... (old parsing logic removed) ...
        # elif " Center" in term: # Fallback for general center terms without state
        #     ... (old parsing logic removed) ...

        # Variables (e.g., "Cognition - Inner Vision")
        # Variables (e.g., "Cognition - Inner Vision") - This handles older/other variable formats
        variable_match = re.match(r"^(Determination|Cognition|Environment) - (.+)$", term) # Removed Motivation/Perspective from regex
        if variable_match:
            var_type_candidate = variable_match.group(1).lower()
            subtype = variable_match.group(2)
            # Ensure var_type_candidate is a valid key from KnowledgeBaseKeys enum if it's in VARIABLE_TYPES
            if var_type_candidate in VARIABLE_TYPES: # VARIABLE_TYPES now only contains determination, cognition, environment
                hd_data[KnowledgeBaseKeys.VARIABLES.value].setdefault(var_type_candidate, {})[subtype] = definition_data
                matched = True
                continue
            else:
                # This case should ideally not be hit if VARIABLE_TYPES is correctly defined
                logger.warning(f"Matched variable prefix '{var_type_candidate}' for term '{term}' but it's not in the configured VARIABLE_TYPES. Skipping.")


        # G Center Access Details by Type and Definition state
        elif item.get("category") == "G Center Access":
            item_id = item.get("id")
            if item_id:
                # Data to store for G Center Access, excluding id and category as they are structural
                g_center_access_item_data = {
                    k: v for k, v in item.items()
                    if k not in ["id", "category", "term_name"] # term_name is unlikely but good to exclude
                }
                hd_data[KnowledgeBaseKeys.G_CENTER_ACCESS_DETAILS.value][item_id] = g_center_access_item_data
                matched = True
                continue

        if not matched:
            # If term is None (because item had no 'term_name' but also no 'category' for G Center Access)
            # or if term was present but didn't match any category.
            actual_identifier = term if term else item.get("id", "Unknown item")
            logger.debug(f"Item '{actual_identifier}' from enginedef.json did not match specific HD category patterns. It might be a general term or require new handling.")

    logger.info(f"Knowledge base transformation complete. Found {len(hd_data[KnowledgeBaseKeys.GATES.value])} gates, {len(hd_data[KnowledgeBaseKeys.CHANNELS.value])} channels, etc.")
    return hd_data

# Load and transform the knowledge base on module import
HD_KNOWLEDGE_BASE: Dict[str, Any] = DEFAULT_HD_KB_STRUCTURE.copy() # Initialize with default

try:
    kb_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'knowledge_graph', 'enginedef.json'))
    logger.info(f"Attempting to load Knowledge Base from: {kb_path}")
    with open(kb_path, 'r', encoding='utf-8') as f:
        raw_definitions = json.load(f)
    HD_KNOWLEDGE_BASE = transform_knowledge_base(raw_definitions) # Use the transformation function
    logger.info(f"Successfully loaded and transformed knowledge base from {kb_path}")

except FileNotFoundError:
    logger.error(f"Knowledge base file not found at {kb_path}. Using default empty structure.")
    # HD_KNOWLEDGE_BASE remains as DEFAULT_HD_KB_STRUCTURE
except json.JSONDecodeError as e:
    logger.error(f"Error decoding JSON from {kb_path}: {e}. Using default empty structure.")
    # HD_KNOWLEDGE_BASE remains as DEFAULT_HD_KB_STRUCTURE
except Exception as e:
    logger.error(f"Error loading/transforming knowledge base: {e}", exc_info=True)
    # HD_KNOWLEDGE_BASE remains as DEFAULT_HD_KB_STRUCTURE (already initialized)

# --- Helper Functions ---

def validate_knowledge_base(kb: Dict[str, Any]) -> bool:
    """Validate knowledge base has required top-level structure."""
    # Check if all main keys defined in the Enum exist in the loaded KB
    required_keys = {k.value for k in KnowledgeBaseKeys if k not in VARIABLE_TYPES and k != KnowledgeBaseKeys.VARIABLES} # Check top level keys
    required_keys.add(KnowledgeBaseKeys.VARIABLES.value) # Add the main variables key
    is_valid = all(key in kb for key in required_keys)
    if not is_valid:
        missing = required_keys - kb.keys()
        logger.warning(f"Knowledge base validation failed. Missing top-level keys: {missing}")
    # Optionally, add deeper validation (e.g., check variable sub-keys)
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