# src/synthesis/narrator.py
# Formats synthesized insights and other profile data into user-friendly text.

import logging
from typing import Dict, Any, List, Optional

# Import knowledge base accessors
from ..human_design.interpreter import HD_KNOWLEDGE_BASE, KnowledgeBaseKeys
from ..astrology.definitions import get_astro_definition

# Import descriptive text data from the assessment results generator
# (We might need to refactor this later to avoid circular dependencies if narrator needs scorer data)
# Use absolute imports from project root since 'services' is a top-level dir
from services.typology_engine.results_generator import (
    TYPOLOGY_PAIRS,
    IDEAL_APPROACHES,
    COMMON_MISALIGNMENTS,
    # We might need more maps here depending on how detailed the narrative gets
)
# Import assessment scorer to potentially regenerate parts if needed
from services.typology_engine import scorer as assessment_scorer


logger = logging.getLogger(__name__)

def format_synthesized_output(
    profile_id: str,
    synthesis_results: Dict[str, Any],
    assessment_results: Optional[Dict[str, Any]] = None, # Pass full assessment results
    astro_factors: Optional[Dict[str, Any]] = None, # Pass extracted astro factors
    hd_interpreted: Optional[Dict[str, Any]] = None # Pass interpreted HD data
    ) -> Dict[str, Any]:
    """
    Formats the synthesized insights and profile data into a user-friendly structure.

    Args:
        profile_id: The ID of the profile being processed.
        synthesis_results: The output from synthesis.engine.generate_synthesized_insights.
        assessment_results: The output from assessment.scorer.generate_complete_results.
        astro_factors: The output from astrology.schema_parser.get_relevant_factors.
        hd_interpreted: The output from human_design.interpreter.interpret_human_design_chart.

    Returns:
        A dictionary containing formatted results suitable for the API response.
        May include Markdown formatting within text fields.
    """
    logger.info(f"Formatting output for profile ID: {profile_id}")
    formatted_output = {
        "profile_id": profile_id,
        "summary": {},
        "details": {},
        "strategies": {},
        "synthesized_insights": synthesis_results.get("synthesized_insights", []) # Include raw insights
    }

    # --- Populate Summary ---
    if assessment_results and assessment_results.get('typologyPair'):
        typology_pair = assessment_results['typologyPair']
        pair_key = typology_pair.get('key')
        pair_data = TYPOLOGY_PAIRS.get(pair_key, {})
        formatted_output["summary"]["typology_name"] = pair_data.get("name", "Unknown Typology")
        formatted_output["summary"]["typology_description"] = pair_data.get("description", "No description available.")
        # Add HD Type and definition if available
        if hd_interpreted and hd_interpreted.get('type'):
            hd_type = hd_interpreted['type']
            hd_strategy = hd_interpreted.get('strategy') # Get strategy
            hd_authority = hd_interpreted.get('authority') # Get authority
            hd_variables = hd_interpreted.get('variables', {})
            motivation_type = hd_variables.get('motivation_type') # Full term name
            perspective_type = hd_variables.get('perspective_type') # Full term name

            formatted_output["summary"]["human_design_type"] = hd_type
            type_info = HD_KNOWLEDGE_BASE.get(KnowledgeBaseKeys.TYPES.value, {}).get(hd_type, {})
            type_def = type_info.get("definition", "No definition available for Type.")
            formatted_output["summary"]["human_design_type_definition"] = type_def

            if hd_strategy:
                formatted_output["summary"]["human_design_strategy"] = hd_strategy
                strategy_info = HD_KNOWLEDGE_BASE.get(KnowledgeBaseKeys.STRATEGIES.value, {}).get(hd_strategy, {})
                strategy_def = strategy_info.get("definition", "No definition available for Strategy.")
                formatted_output["summary"]["human_design_strategy_definition"] = strategy_def

            if hd_authority:
                formatted_output["summary"]["human_design_authority"] = hd_authority
                # The interpreter already maps authority keys like "Emotional"
                auth_info = HD_KNOWLEDGE_BASE.get(KnowledgeBaseKeys.AUTHORITIES.value, {}).get(hd_authority, {})
                auth_def = auth_info.get("definition", "No definition available for Authority.")
                formatted_output["summary"]["human_design_authority_definition"] = auth_def
            
            if motivation_type:
                formatted_output["summary"]["human_design_motivation"] = motivation_type
                motivation_info = HD_KNOWLEDGE_BASE.get(KnowledgeBaseKeys.VARIABLES.value, {}).get(KnowledgeBaseKeys.MOTIVATION.value, {}).get(motivation_type, {})
                motivation_def = motivation_info.get("definition", "No definition available for Motivation.")
                formatted_output["summary"]["human_design_motivation_definition"] = motivation_def

            if perspective_type:
                formatted_output["summary"]["human_design_perspective"] = perspective_type
                perspective_info = HD_KNOWLEDGE_BASE.get(KnowledgeBaseKeys.VARIABLES.value, {}).get(KnowledgeBaseKeys.PERSPECTIVE.value, {}).get(perspective_type, {})
                perspective_def = perspective_info.get("definition", "No definition available for Perspective.")
                formatted_output["summary"]["human_design_perspective_definition"] = perspective_def

        # Add Sun Sign and definition if available
        if astro_factors and astro_factors.get('Sun') and astro_factors['Sun'].get('sign'):
            sun_sign = astro_factors['Sun']['sign']
            formatted_output["summary"]["sun_sign"] = sun_sign
            astro_def = get_astro_definition(sun_sign)
            sign_def = "No definition available."
            if astro_def:
                sign_def = astro_def.get("definition", sign_def)
            formatted_output["summary"]["sun_sign_definition"] = sign_def


    # --- Populate Details (Example: Assessment Spectrums) ---
    if assessment_results and assessment_results.get('spectrumPlacements'):
         formatted_output["details"]["assessment_spectrums"] = assessment_results['spectrumPlacements']
         # TODO: Add descriptive text for each placement from TYPOLOGY_DESCRIPTIONS

    # Add Astrology Details if available
    if astro_factors:
        formatted_output["details"]["astrology_factors"] = astro_factors # Add the whole dict

    # Add Human Design Details if available
    if hd_interpreted:
        formatted_output["details"]["human_design_interpreted"] = hd_interpreted # Add the whole dict

    # --- Populate Strategies (Example: Ideal Approaches & Misalignments) ---
    if assessment_results and assessment_results.get('typologyPair'):
        pair_key = assessment_results['typologyPair'].get('key')
        ideal_data = IDEAL_APPROACHES.get(pair_key, {})
        misalignment_data = COMMON_MISALIGNMENTS.get(pair_key, [])
        formatted_output["strategies"]["ideal_approaches"] = {
            "strengths_summary": ideal_data.get("strengths", ""),
            "list": ideal_data.get("approaches", [])
        }
        formatted_output["strategies"]["common_misalignments"] = misalignment_data

    # --- Format Synthesized Insights (Example: Add Markdown) ---
    formatted_insights = []
    for insight in formatted_output["synthesized_insights"]:
        # Example: Add markdown emphasis based on category
        category = insight.get("category", "General")
        text = insight.get("text", "")
        formatted_text = f"**{category}:** {text}" # Simple markdown bolding
        formatted_insights.append({
            "id": insight.get("id"),
            "category": category,
            "formatted_text": formatted_text,
            "derived_from": insight.get("derived_from", [])
        })
    formatted_output["synthesized_insights"] = formatted_insights # Replace raw with formatted

    # TODO: Add more sections and formatting based on available data
    # - Detailed Astrology factors (Sun, Moon, Asc, Ruler) with interpretations
    #   Example Usage:
    #   moon_sign = astro_factors.get('Moon', {}).get('sign')
    #   if moon_sign:
    #       moon_def = get_astro_definition(moon_sign)
    #       if moon_def:
    #           # Add moon_def['definition'] or other fields to the narrative section
    #           pass
    #
    # - Detailed HD factors (Authority, Profile, Centers, Channels) with interpretations
    #   Example Usage:
    #   authority_name = hd_interpreted.get('authority')
    #   if authority_name:
    #       # Handle potential variations like "Emotional - Solar Plexus" vs "Emotional" if needed,
    #       # although the interpreter already maps common ones.
    #       auth_key = authority_name.split('-')[0].strip() if '-' in authority_name else authority_name
    #       auth_info = HD_KNOWLEDGE_BASE.get('authorities', {}).get(auth_key, {})
    #       if auth_info:
    #           # Add auth_info['definition'] or other fields to the narrative section
    #           pass
    #   profile_name = hd_interpreted.get('profile')
    #   if profile_name:
    #       profile_info = HD_KNOWLEDGE_BASE.get('profiles', {}).get(profile_name, {})
    #       # Add profile_info['theme'] etc.
    #
    # - Mastery Assessment results (dominant values) with interpretations
    # - Energy Focus description

    logger.info(f"Output formatting complete for profile ID: {profile_id}")
    return formatted_output


# Example Usage (for testing purposes)
if __name__ == '__main__':
    # Sample data (replace with actual outputs from previous steps)
    test_profile_id = "user_test_123"
    test_synthesis_results = {
        "synthesized_insights": [
            {"id": "SYN001", "text": "As a structured-fluid with a Human Design Type of Projector, your approach likely involves grounding intuitive insights.", "category": "Core Approach", "derived_from": ["Typology: structured-fluid", "HD Type: Projector"]},
            {"id": "SYN_DEFAULT", "text": "Further insights pending.", "category": "General", "derived_from": []}
        ]
    }
    test_assessment_results = assessment_scorer.generate_complete_results(
         typology_responses={ # Sample data
            'cognitive-q1': 'left', 'cognitive-q2': 'balanced', 'perceptual-q1': 'right', 'perceptual-q2': 'right',
            'kinetic-q1': 'left', 'kinetic-q2': 'left', 'choice-q1': 'balanced', 'choice-q2': 'left',
            'resonance-q1': 'right', 'resonance-q2': 'balanced', 'rhythm-q1': 'balanced', 'rhythm-q2': 'right'
         },
         mastery_responses={} # No mastery for this example
    )
    test_astro_factors = {"Sun": {"sign": "Taurus", "house": 10}, "Moon": {"sign": "Cancer", "house": 12}}
    test_hd_interpreted = {"type": "Projector", "authority": "Self-Projected", "profile": "5/1"}

    formatted = format_synthesized_output(
        test_profile_id,
        test_synthesis_results,
        test_assessment_results,
        test_astro_factors,
        test_hd_interpreted
    )

    print("\n--- Formatted Output (Sample) ---")
    import json
    print(json.dumps(formatted, indent=2))