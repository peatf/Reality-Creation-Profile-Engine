# src/constants.py
from enum import Enum

class KnowledgeBaseKeys(Enum):
    TYPES = "types"
    AUTHORITIES = "authorities"
    STRATEGIES = "strategies"
    PROFILES = "profiles"
    GATES = "gates"
    CHANNELS = "channels"
    VARIABLES = "variables_info"
    # Sub-keys for variables_info, also useful as constants
    DETERMINATION = "determination"
    COGNITION = "cognition"
    MOTIVATION = "motivation"
    PERSPECTIVE = "perspective"
    ENVIRONMENT = "environment"
    # Other top-level keys
    MANIFESTATION_MECHANICS = "manifestation_mechanics"
    CENTERS = "centers"
    G_CENTER_ACCESS_DETAILS = "g_center_access_details" # For the new G Center Access by HD Type and Definition