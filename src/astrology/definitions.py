import json
import os
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

ASTROLOGY_DEFINITIONS: Dict[str, Dict[str, Any]] = {}

try:
    # Construct path relative to this file (__file__) to find enginedef.json
    kb_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'knowledge_graph', 'enginedef.json'))
    logger.info(f"Attempting to load Astrology definitions from: {kb_path}")
    with open(kb_path, 'r', encoding='utf-8') as f:
        all_definitions = json.load(f)

    astrology_data_list = all_definitions.get("astrology", [])
    logger.info(f"Successfully loaded {len(astrology_data_list)} Astrology definitions from {kb_path}")

    # Transform list into a dictionary keyed by term_name
    for item in astrology_data_list:
        term_name = item.get("term_name")
        if term_name:
            # Store the entire definition object, keyed by its name
            ASTROLOGY_DEFINITIONS[term_name] = item
        else:
            logger.warning("Found astrology definition item without 'term_name'. Skipping.")

except FileNotFoundError:
    logger.error(f"Knowledge base file not found at {kb_path}. ASTROLOGY_DEFINITIONS will be empty.")
    ASTROLOGY_DEFINITIONS = {} # Ensure it's an empty dict on error
except json.JSONDecodeError as e:
    logger.error(f"Error decoding JSON from {kb_path}: {e}. ASTROLOGY_DEFINITIONS will be empty.")
    ASTROLOGY_DEFINITIONS = {} # Ensure it's an empty dict on error
except Exception as e:
    logger.error(f"An unexpected error occurred loading astrology definitions from {kb_path}: {e}. ASTROLOGY_DEFINITIONS will be empty.", exc_info=True)
    ASTROLOGY_DEFINITIONS = {} # Ensure it's an empty dict on error

logger.info(f"ASTROLOGY_DEFINITIONS populated with {len(ASTROLOGY_DEFINITIONS)} entries.")

def get_astro_definition(term_name: str) -> Optional[Dict[str, Any]]:
    """Get astrology definition by term name with safe lookup."""
    if not term_name:
        logger.debug("Attempted to get astrology definition with empty term_name.")
        return None
    definition = ASTROLOGY_DEFINITIONS.get(term_name)
    if definition is None:
        logger.debug(f"Astrology definition for term '{term_name}' not found.")
    return definition

# Example Usage (for testing purposes)
if __name__ == '__main__':
    print(f"Loaded {len(ASTROLOGY_DEFINITIONS)} astrology definitions.")

    # Test cases
    term1 = "Ascendant"
    def1 = get_astro_definition(term1)
    if def1:
        print(f"\nDefinition for '{term1}':")
        print(json.dumps(def1, indent=2))
    else:
        print(f"\nDefinition for '{term1}' not found.")

    term2 = "Mars in Cancer"
    def2 = get_astro_definition(term2)
    if def2:
        print(f"\nDefinition for '{term2}':")
        print(json.dumps(def2, indent=2))
    else:
        print(f"\nDefinition for '{term2}' not found.")

    term3 = "NonExistentTerm"
    def3 = get_astro_definition(term3)
    if def3:
        print(f"\nDefinition for '{term3}':")
        print(json.dumps(def3, indent=2))
    else:
        print(f"\nDefinition for '{term3}' not found.")