import logging
from typing import List, Dict, Any, Optional

from ..crud.base_dao import execute_read_query, DAOException
from ..graph_schema.schema_setup import (
    PERSON_LABEL,
    ASTROFEATURE_LABEL,
    HDFEATURE_LABEL,
    HAS_FEATURE_REL,
    INFLUENCES_REL
)

logger = logging.getLogger(__name__)

async def find_persons_with_features(
    astro_feature_name: Optional[str] = None,
    hd_feature_name: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Finds all Person nodes that have a specific AstroFeature and/or HDFeature.
    If only one feature name is provided, it searches for persons with that feature.
    If both are provided, it finds persons with both features.
    """
    if not astro_feature_name and not hd_feature_name:
        logger.warning("find_persons_with_features called without any feature names.")
        return []

    match_clauses = []
    where_clauses = [] # Not strictly needed if matching on properties directly
    params = {}
    
    return_vars = ["p"] # Always return the person

    if astro_feature_name:
        match_clauses.append(f"(p:{PERSON_LABEL})-[:{HAS_FEATURE_REL}]->(af:{ASTROFEATURE_LABEL} {{name: $astro_name}})")
        params["astro_name"] = astro_feature_name
        return_vars.append("af")

    if hd_feature_name:
        match_clauses.append(f"(p:{PERSON_LABEL})-[:{HAS_FEATURE_REL}]->(hf:{HDFEATURE_LABEL} {{name: $hd_name}})")
        params["hd_name"] = hd_feature_name
        return_vars.append("hf")
        
    query = f"""
    MATCH {', '.join(match_clauses)}
    RETURN p, {', '.join(return_vars[1:])}
    """
    # The RETURN clause needs to be smarter if some vars are not present.
    # For simplicity, we'll adjust the query based on inputs.
    
    if astro_feature_name and hd_feature_name:
        query = f"""
        MATCH (p:{PERSON_LABEL})-[:{HAS_FEATURE_REL}]->(af:{ASTROFEATURE_LABEL} {{name: $astro_name}}),
              (p)-[:{HAS_FEATURE_REL}]->(hf:{HDFEATURE_LABEL} {{name: $hd_name}})
        RETURN p, af, hf
        """
    elif astro_feature_name:
        query = f"""
        MATCH (p:{PERSON_LABEL})-[:{HAS_FEATURE_REL}]->(af:{ASTROFEATURE_LABEL} {{name: $astro_name}})
        RETURN p, af
        """
    elif hd_feature_name:
        query = f"""
        MATCH (p:{PERSON_LABEL})-[:{HAS_FEATURE_REL}]->(hf:{HDFEATURE_LABEL} {{name: $hd_name}})
        RETURN p, hf
        """
    else: # Should not happen due to initial check
        return []

    try:
        logger.debug(f"Executing find_persons_with_features query: {query} with params: {params}")
        results = await execute_read_query(query, params)
        # Process results to extract Person data, potentially along with the matched features
        # Each result in the list will be a dict like {"p": {...}, "af": {...}, "hf": {...}}
        # We might want to return a list of PersonNode objects or just their properties.
        # For now, returning the raw records.
        return results
    except DAOException as e:
        logger.error(f"DAOException in find_persons_with_features: {e}", exc_info=True)
        return []


async def count_persons_influenced_by_astrofeature(astro_feature_name: str) -> int:
    """
    Counts how many distinct Person nodes are influenced by a specific AstroFeature.
    This assumes an INFLUENCES relationship can go from AstroFeature to Person,
    or more likely, AstroFeature -> (some intermediate node) -> Person, or
    Person -> HAS_FEATURE -> AstroFeature, and then other AstroFeatures INFLUENCE this one.
    
    Let's assume for now: (InfluencingFeature:AstroFeature)-[:INFLUENCES]->(TargetFeature:AstroFeature)<-[:HAS_FEATURE]-(p:Person)
    This query finds persons who HAVE a feature that IS INFLUENCED BY the given astro_feature_name.
    
    A more direct interpretation: (GivenFeature:AstroFeature)<-[:HAS_FEATURE]-(p:Person)
    And we want to count how many OTHER features this GivenFeature influences, and then how many people have THOSE.
    This is complex. Let's simplify: Count persons who HAVE an AstroFeature that IS INFLUENCED BY `astro_feature_name`.
    """
    query = f"""
    MATCH (influencer_af:{ASTROFEATURE_LABEL} {{name: $astro_name}})
    MATCH (influencer_af)-[:{INFLUENCES_REL}]->(influenced_target_af:{ASTROFEATURE_LABEL})
    MATCH (p:{PERSON_LABEL})-[:{HAS_FEATURE_REL}]->(influenced_target_af)
    RETURN count(DISTINCT p) as person_count
    """
    params = {"astro_name": astro_feature_name}
    try:
        results = await execute_read_query(query, params)
        if results and results[0] and "person_count" in results[0]:
            return results[0]["person_count"]
        return 0
    except DAOException as e:
        logger.error(f"DAOException in count_persons_influenced_by_astrofeature for '{astro_feature_name}': {e}", exc_info=True)
        return 0

async def traverse_relationships(
    start_node_label: str,
    start_node_identifier_property: str, # e.g., "user_id" or "name"
    start_node_identifier_value: Any,
    min_hops: int = 1,
    max_hops: int = 1,
    relationship_types: Optional[List[str]] = None, # e.g., ["HAS_FEATURE", "INFLUENCES"]
    direction: str = "OUTGOING" # OUTGOING, INCOMING, BOTH
) -> List[Dict[str, Any]]:
    """
    Traverses relationships from a starting node up to N degrees of separation.

    Args:
        start_node_label: Label of the starting node.
        start_node_identifier_property: Property name to identify the start node (e.g., 'user_id', 'name').
        start_node_identifier_value: Value of the identifier property.
        min_hops: Minimum number of hops (relationships) in the path.
        max_hops: Maximum number of hops in the path.
        relationship_types: Optional list of relationship types to traverse. If None, all types.
        direction: Direction of traversal ('OUTGOING', 'INCOMING', 'BOTH').

    Returns:
        A list of paths, where each path is represented as a list of nodes and relationships.
        Or a list of end nodes, depending on what's most useful. For now, returning paths.
    """
    if min_hops < 0 or max_hops < min_hops:
        raise ValueError("Invalid min_hops or max_hops values.")

    rel_type_cypher = ""
    if relationship_types:
        rel_type_cypher = ":" + "|".join(relationship_types) # e.g., ":HAS_FEATURE|INFLUENCES"

    path_pattern = ""
    if direction.upper() == "OUTGOING":
        path_pattern = f"-[r{rel_type_cypher}*{min_hops}..{max_hops}]->"
    elif direction.upper() == "INCOMING":
        path_pattern = f"<-[r{rel_type_cypher}*{min_hops}..{max_hops}]-"
    elif direction.upper() == "BOTH":
        path_pattern = f"-[r{rel_type_cypher}*{min_hops}..{max_hops}]-"
    else:
        raise ValueError(f"Invalid direction: {direction}. Must be OUTGOING, INCOMING, or BOTH.")

    query = f"""
    MATCH path = (start_node:{start_node_label} {{{start_node_identifier_property}: $start_val}}){path_pattern}(end_node)
    WHERE start_node <> end_node  // Avoid self-loops if not desired, though path length handles simple cases
    RETURN path
    """
    # Note: Returning 'path' gives a Path object. To make it serializable or easier to work with,
    # you might want to return nodes(path) and relationships(path) and process them.
    # For now, let's assume the driver handles Path objects in results in a usable way,
    # or we can process it: RETURN [n IN nodes(path) | properties(n)] AS path_nodes, [rel IN relationships(path) | {{type: type(rel), props: properties(rel)}}] AS path_rels

    params = {"start_val": start_node_identifier_value}
    try:
        logger.debug(f"Executing traverse_relationships query: {query} with params: {params}")
        results = await execute_read_query(query, params)
        # Results will be a list of records, each containing a 'path' object.
        # Processing these Path objects into a more standard list/dict structure might be needed.
        # For now, returning raw results.
        return results
    except DAOException as e:
        logger.error(f"DAOException in traverse_relationships: {e}", exc_info=True)
        return []
    except ValueError as e: # Catch our own validation errors
        logger.error(f"ValueError in traverse_relationships: {e}", exc_info=True)
        raise


async def export_person_subgraph_cypher(user_id: str) -> str:
    """
    Exports all data related to a specific Person as a series of Cypher CREATE statements.
    This includes the Person node, their features, typology results, and relationships.
    """
    # This is a complex operation. It involves fetching all related data and then formatting it.
    # Step 1: Fetch the Person node.
    # Step 2: Fetch all direct relationships and connected nodes (AstroFeatures, HDFeatures, TypologyResults).
    # Step 3: Fetch relationships between these connected nodes (e.g., INFLUENCES between features).
    # Step 4: Format all these into Cypher CREATE statements.

    # Simplified example: Just get the person and their direct features for now.
    # A full export would require more comprehensive traversal.

    cypher_statements = []

    # Get Person
    person_query = f"MATCH (p:{PERSON_LABEL} {{user_id: $user_id}}) RETURN p"
    person_result = await execute_read_query(person_query, {"user_id": user_id})
    if not person_result or not person_result[0].get("p"):
        return f"// Person with user_id '{user_id}' not found."

    person_props = person_result[0]["p"]
    # Escape strings in properties for Cypher
    # Corrected string formatting and escaping
    person_props_list = []
    for k, v in person_props.items():
        if v is None: continue
        if isinstance(v, str):
            # Escape backslashes first, then single quotes
            escaped_v = str(v).replace("\\", "\\\\").replace("'", "\\'")
            person_props_list.append(f"{k}: '{escaped_v}'")
        elif isinstance(v, (int, float, bool)):
             person_props_list.append(f"{k}: {v}")
        else: # Handle other types like lists or dicts if necessary, or convert to string
             escaped_v = str(v).replace("\\", "\\\\").replace("'", "\\'")
             person_props_list.append(f"{k}: '{escaped_v}'") # Default to string representation
    person_props_str = ", ".join(person_props_list)
    person_var_name = f"p_{user_id.replace('-', '_')}" # Consistent variable name
    cypher_statements.append(f"CREATE ({person_var_name}:{PERSON_LABEL} {{{person_props_str}}});")

    # Get connected features (example for HAS_FEATURE)
    features_query = f"""
    MATCH (p:{PERSON_LABEL} {{user_id: $user_id}})-[r:{HAS_FEATURE_REL}]->(f)
    RETURN f, type(r) as rel_type, properties(r) as rel_props, labels(f) as feature_labels
    """
    related_data = await execute_read_query(features_query, {"user_id": user_id})

    feature_vars = {} # To map feature original id/name to cypher var name

    for record in related_data:
        feature_node = record.get("f")
        rel_type = record.get("rel_type")
        rel_props = record.get("rel_props")
        feature_labels = record.get("feature_labels")

        if feature_node and feature_labels:
            label = feature_labels[0] # Assuming single primary label
            # Use a more robust way to get a unique identifier for the feature node
            feature_id_prop = feature_node.get("name") or feature_node.get("assessment_id")
            if not feature_id_prop: # Fallback if no standard unique prop
                 feature_id_prop = feature_node.get("elementId", f"unknown_{label}_{len(feature_vars)}") # Use elementId if available, else generate temp ID

            # Sanitize identifier for use in Cypher variable name
            sanitized_id = str(feature_id_prop).replace('-', '_').replace(' ', '_').replace('.', '_').replace('/', '_').replace('\\', '_')
            feature_var_name = f"{label.lower()}_{sanitized_id}"
            
            if feature_id_prop not in feature_vars: # Only create node if not already processed
                feature_vars[feature_id_prop] = feature_var_name

                # Corrected string formatting and escaping for feature properties
                f_props_list = []
                for k, v in feature_node.items():
                    if v is None: continue
                    if isinstance(v, str):
                        escaped_v = str(v).replace("\\", "\\\\").replace("'", "\\'")
                        f_props_list.append(f"{k}: '{escaped_v}'")
                    elif isinstance(v, (int, float, bool)):
                        f_props_list.append(f"{k}: {v}")
                    else:
                        escaped_v = str(v).replace("\\", "\\\\").replace("'", "\\'")
                        f_props_list.append(f"{k}: '{escaped_v}'")
                f_props_str = ", ".join(f_props_list)
                cypher_statements.append(f"CREATE ({feature_var_name}:{label} {{{f_props_str}}});")

            # Create relationship
            if rel_type:
                rel_props_list = []
                if rel_props: # Check if rel_props is not None and not empty
                    for k, v in rel_props.items():
                        if v is None: continue
                        if isinstance(v, str):
                            escaped_v = str(v).replace("\\", "\\\\").replace("'", "\\'")
                            rel_props_list.append(f"{k}: '{escaped_v}'")
                        elif isinstance(v, (int, float, bool)):
                            rel_props_list.append(f"{k}: {v}")
                        else:
                            escaped_v = str(v).replace("\\", "\\\\").replace("'", "\\'")
                            rel_props_list.append(f"{k}: '{escaped_v}'")
                
                rel_props_str = ""
                if rel_props_list:
                    rel_props_str = f" {{{', '.join(rel_props_list)}}}"
                    
                cypher_statements.append(f"CREATE ({person_var_name})-[:{rel_type}{rel_props_str}]->({feature_var_name});")


    # This is still a basic export. See previous comments.
    return "\n".join(cypher_statements)


if __name__ == "__main__":
    import asyncio
    from ..core.db import Neo4jDatabase
    from ..core.logging_config import setup_logging
    # For testing, you'd need to populate Neo4j with some data first.
    # The CRUD operation scripts have example __main__ blocks that create data.
    # You could adapt those to create a small test graph.

    setup_logging("DEBUG")

    async def test_query_utils():
        logger.info("Testing query_utils...")
        
        # --- Test find_persons_with_features ---
        logger.info("Testing find_persons_with_features (requires populated DB or mocks for real data)...")
        persons_with_sun_in_aries = await find_persons_with_features(astro_feature_name="Sun in Aries_TestQuery")
        logger.info(f"Persons with 'Sun in Aries_TestQuery': {persons_with_sun_in_aries} (expected empty if DB is empty/mocked)")

        # --- Test count_persons_influenced_by_astrofeature ---
        logger.info("Testing count_persons_influenced_by_astrofeature (requires populated DB or mocks)...")
        count = await count_persons_influenced_by_astrofeature(astro_feature_name="Mars_Trine_Venus_TestQuery")
        logger.info(f"Count of persons influenced by 'Mars_Trine_Venus_TestQuery': {count} (expected 0 if DB is empty/mocked)")

        # --- Test traverse_relationships ---
        logger.info("Testing traverse_relationships (requires populated DB or mocks)...")
        paths = await traverse_relationships(
            start_node_label=PERSON_LABEL,
            start_node_identifier_property="user_id",
            start_node_identifier_value="user_traverse_test",
            max_hops=2,
            relationship_types=[HAS_FEATURE_REL, INFLUENCES_REL]
        )
        logger.info(f"Paths from 'user_traverse_test': {paths} (expected empty if DB is empty/mocked)")

        # --- Test export_person_subgraph_cypher ---
        logger.info("Testing export_person_subgraph_cypher (requires populated DB or mocks)...")
        cypher_script = await export_person_subgraph_cypher(user_id="user_export_test")
        logger.info(f"Cypher script for 'user_export_test':\n{cypher_script}")


        await Neo4jDatabase.close_async_driver()
        logger.info("Neo4j driver closed after query_utils tests.")

    logger.info("query_utils.py loaded. Run test_query_utils() manually with a populated DB or mocks for meaningful results.")
    # To run:
    # import asyncio
    # from services.kg.app.utils.query_utils import test_query_utils # Corrected import path if running from project root
    # asyncio.run(test_query_utils()) # Correctly call the async function