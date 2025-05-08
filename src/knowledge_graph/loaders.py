import json
import os
import logging

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ASTRO_DEFINITIONS_DIR = os.path.join(BASE_DIR, 'AstroDefinitions')
HD_DEFINITIONS_DIR = os.path.join(BASE_DIR, 'HDDefinitions')

def get_astro_definition(definition_name: str):
    """
    Loads a specific astrological definition from the AstroDefinitions directory.
    Example: get_astro_definition("planet_in_sign")
    """
    file_path = os.path.join(ASTRO_DEFINITIONS_DIR, f"{definition_name}.json")
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        logger.debug(f"Successfully loaded AstroDefinition: {definition_name} from {file_path}")
        return data
    except FileNotFoundError:
        logger.error(f"AstroDefinition file not found: {file_path}")
        return None
    except json.JSONDecodeError:
        logger.error(f"Error decoding JSON from AstroDefinition file: {file_path}")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred while loading AstroDefinition {definition_name}: {e}")
        return None

def get_hd_definition(definition_name: str):
    """
    Loads a specific Human Design definition from the HDDefinitions directory.
    Example: get_hd_definition("HD_channels")
    """
    file_path = os.path.join(HD_DEFINITIONS_DIR, f"{definition_name}.json")
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        logger.debug(f"Successfully loaded HDDefinition: {definition_name} from {file_path}")
        return data
    except FileNotFoundError:
        logger.error(f"HDDefinition file not found: {file_path}")
        return None
    except json.JSONDecodeError:
        logger.error(f"Error decoding JSON from HDDefinition file: {file_path}")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred while loading HDDefinition {definition_name}: {e}")
        return None

if __name__ == '__main__':
    # Example usage for testing
    logging.basicConfig(level=logging.DEBUG)
    
    planet_in_sign = get_astro_definition("planet_in_sign")
    if planet_in_sign:
        logger.info(f"Loaded planet_in_sign definition (first 5 items): {list(planet_in_sign.items())[:5]}")

    hd_channels = get_hd_definition("HD_channels")
    if hd_channels:
        logger.info(f"Loaded HD_channels definition (first 5 items): {list(hd_channels.items())[:5]}")

    # Test non-existent file
    non_existent_astro = get_astro_definition("non_existent_def")
    if non_existent_astro is None:
        logger.info("Correctly handled non-existent astro definition.")

    non_existent_hd = get_hd_definition("non_existent_def")
    if non_existent_hd is None:
        logger.info("Correctly handled non-existent HD definition.")