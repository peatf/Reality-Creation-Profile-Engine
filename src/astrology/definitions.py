import json
import logging
from typing import Dict, Any, Optional, List
from src.knowledge_graph.loaders import get_astro_definition as load_astro_json_file

logger = logging.getLogger(__name__)

# Cache for loaded astrology definition files (keyed by file basename e.g., "planet_in_sign")
ASTROLOGY_DEFINITIONS_CACHE: Dict[str, Dict[str, Any]] = {}

# List of AstroDefinition file basenames to search.
# The .json extension is appended by the loader.
ASTRO_DEFINITION_FILES: List[str] = [
    "chartruler_in_house",
    "chartruler_in_planets",
    "northnode_in_houses",
    "northnode_in_signs",
    "planet_hardaspects",
    "planet_in_house",
    "planet_in_sign",
    "planet_softaspects",
    "signs_in_houses"
    # Add other AstroDefinition file basenames here if new ones are created
]

def get_astro_definition(term_name: str) -> Optional[Dict[str, Any]]:
    """
    Get astrology definition by term name.
    It searches through predefined AstroDefinition JSON files, loading them on demand
    and caching them for subsequent requests.
    """
    if not term_name:
        logger.debug("Attempted to get astrology definition with empty term_name.")
        return None

    for file_basename in ASTRO_DEFINITION_FILES:
        definition_file_content = ASTROLOGY_DEFINITIONS_CACHE.get(file_basename)

        if definition_file_content is None:
            logger.debug(f"Cache miss for '{file_basename}'. Attempting to load.")
            try:
                definition_file_content = load_astro_json_file(file_basename)
                if definition_file_content is not None:
                    ASTROLOGY_DEFINITIONS_CACHE[file_basename] = definition_file_content
                    logger.info(f"Successfully loaded and cached '{file_basename}.json'.")
                else:
                    # Store None to prevent repeated load attempts for missing/invalid files
                    ASTROLOGY_DEFINITIONS_CACHE[file_basename] = {} # Store empty dict to mark as tried
                    logger.warning(f"Failed to load '{file_basename}.json' or file is empty. It will not be re-attempted for this term search.")
            except Exception as e:
                logger.error(f"Unexpected error loading '{file_basename}.json': {e}", exc_info=True)
                ASTROLOGY_DEFINITIONS_CACHE[file_basename] = {} # Store empty dict on error
                definition_file_content = {} # Ensure it's an empty dict

        # Ensure definition_file_content is a dictionary before searching
        if isinstance(definition_file_content, dict):
            if term_name in definition_file_content:
                logger.debug(f"Found term '{term_name}' in '{file_basename}.json'.")
                return definition_file_content[term_name]
        elif definition_file_content is not None: # Loaded but not a dict
             logger.warning(f"Content of '{file_basename}.json' is not a dictionary. Skipping search for '{term_name}' in this file.")


    logger.debug(f"Astrology definition for term '{term_name}' not found in any specified AstroDefinition files.")
    return None

# Example Usage (for testing purposes)
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO) # Use INFO for cleaner test output, DEBUG for dev
    logger.info("Starting astrology definition tests with new loader.")

    # Test cases
    # Note: These term names must exist as keys in the respective JSON files.
    # Example: "Sun in Aries" should be a key in "planet_in_sign.json"
    
    test_terms = {
        "planet_in_sign": "Sun in Aries",  # Assuming this exists in planet_in_sign.json
        "planet_hardaspects": "Sun square Moon", # Assuming this exists in planet_hardaspects.json
        "signs_in_houses": "Aries in House 1", # Assuming this exists in signs_in_houses.json
        "non_existent_term": "This Term Does Not Exist Anywhere",
        "empty_term": ""
    }

    for file_hint, term in test_terms.items():
        print(f"\n--- Testing term: '{term}' (expected in files like '{file_hint}.json') ---")
        definition = get_astro_definition(term)
        if definition:
            print(f"Definition for '{term}':")
            print(json.dumps(definition, indent=2))
        else:
            print(f"Definition for '{term}' not found or term was empty.")

    # Test caching (second call for the same term should be faster and use cache)
    print("\n--- Testing caching for 'Sun in Aries' ---")
    def_cached = get_astro_definition("Sun in Aries")
    if def_cached:
        print("Successfully retrieved 'Sun in Aries' again (should be cached).")
    else:
        print("'Sun in Aries' not found on second attempt.")
    
    # Test a term from a different file to ensure multiple files can be loaded
    print("\n--- Testing term from another potential file: 'Mars opposition Saturn' (planet_hardaspects.json) ---")
    def_another_file = get_astro_definition("Mars opposition Saturn")
    if def_another_file:
        print(f"Definition for 'Mars opposition Saturn':")
        print(json.dumps(def_another_file, indent=2))
    else:
        print("'Mars opposition Saturn' not found.")

    logger.info("Finished astrology definition tests.")