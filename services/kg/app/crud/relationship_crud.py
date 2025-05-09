import logging
from typing import Optional, Dict, Any, List, Type

from ..graph_schema.nodes import PersonNode, AstroFeatureNode, HDFeatureNode, TypologyResultNode
from ..graph_schema.relationships import (
    HasFeatureProperties,
    InfluencesProperties,
    ConflictsWithProperties,
    HasTypologyResultProperties,
    RelationshipBase
)
from ..graph_schema.constants import ( # Import from constants
    PERSON_LABEL, ASTROFEATURE_LABEL, HDFEATURE_LABEL, TYPOLOGYRESULT_LABEL,
    HAS_FEATURE_REL, INFLUENCES_REL, CONFLICTS_WITH_REL, HAS_TYPOLOGY_RESULT_REL
)
from .base_dao import (
    execute_read_query,
    execute_write_query, # Kept for specific deletes or non-MERGE writes
    execute_merge_query, # Added for MERGE operations
    DAOException,
    RelationshipCreationError,
    NodeNotFoundError # To indicate if start/end nodes for relationship don't exist
)
# Import specific node CRUDs to verify node existence if needed, or rely on MATCH in Cypher
# These imports are for the test code, ensure they use the new upsert/get_by_composite_key methods
from .person_crud import upsert_person as person_upsert, delete_person
from .astrofeature_crud import upsert_astrofeature as astrofeature_upsert, delete_astrofeature as astrofeature_delete, get_astrofeature_by_name_and_type
from .hdfeature_crud import upsert_hdfeature as hdfeature_upsert, delete_hdfeature as hdfeature_delete, get_hdfeature_by_name_and_type
from .typology_crud import upsert_typology_result as typology_upsert, delete_typology_result


logger = logging.getLogger(__name__)

# --- Generic Relationship Creation (Idempotent) ---
async def merge_relationship(
    start_node_label: str,
    start_node_props: Dict[str, Any], # e.g., {"user_id": "id1"}
    end_node_label: str,
    end_node_props: Dict[str, Any],   # e.g., {"name": "feature_name", "feature_type": "type"}
    relationship_type: str,
    rel_props_model: Optional[RelationshipBase] = None
) -> Optional[Dict[str, Any]]:
    """
    Creates or updates a relationship (MERGE) of a given type between two nodes.
    This operation is idempotent. If the relationship exists, its properties are updated.
    `start_node_props` and `end_node_props` must contain enough info to uniquely identify the nodes.
    """
    start_match_props_str = ", ".join([f"{k}: ${k}_start" for k in start_node_props.keys()])
    end_match_props_str = ", ".join([f"{k}: ${k}_end" for k in end_node_props.keys()])

    params = {}
    for k, v in start_node_props.items():
        params[f"{k}_start"] = v
    for k, v in end_node_props.items():
        params[f"{k}_end"] = v

    set_clause = ""
    if rel_props_model:
        rel_dict = rel_props_model.model_dump(exclude_none=True)
        if rel_dict:
            params["rel_props"] = rel_dict
            # Set properties on create and on match to ensure idempotency with the given properties.
            set_clause = "ON CREATE SET r = $rel_props ON MATCH SET r = $rel_props"

    query = f"""
    MATCH (start_node:{start_node_label} {{{start_match_props_str}}})
    MATCH (end_node:{end_node_label} {{{end_match_props_str}}})
    MERGE (start_node)-[r:{relationship_type}]->(end_node)
    {set_clause}
    RETURN type(r) as type, properties(r) as properties, elementId(start_node) as start_id, elementId(end_node) as end_id
    """

    try:
        result = await execute_merge_query(query, params)
        if result:
            return result
        else:
            # If MERGE involves nodes that don't exist (due to MATCH failure), it won't create the relationship
            # and might not return anything, or execute_merge_query might return None if no record.
            logger.warning(
                f"Relationship '{relationship_type}' MERGE did not return result or failed. "
                f"Ensure start/end nodes exist with properties: Start: {start_node_props}, End: {end_node_props}. Query: {query}"
            )
            raise RelationshipCreationError(
                f"Failed to merge relationship '{relationship_type}'. "
                f"Start or end node likely not found, or MERGE query did not return expected data."
            )
    except DAOException as e:
        logger.error(f"DAOException merging relationship '{relationship_type}' from {start_node_props} to {end_node_props}: {e}", exc_info=True)
        raise RelationshipCreationError(f"Database error merging relationship '{relationship_type}': {e}") from e


# --- Specific Relationship CRUD Functions (using merge_relationship) ---

# 1. Person -> HAS_FEATURE -> AstroFeature
async def link_person_to_astrofeature(user_id: str, astrofeature_name: str, astrofeature_type: str, props: Optional[HasFeatureProperties] = None) -> Optional[Dict[str, Any]]:
    return await merge_relationship(
        start_node_label=PERSON_LABEL,
        start_node_props={"user_id": user_id},
        end_node_label=ASTROFEATURE_LABEL,
        end_node_props={"name": astrofeature_name, "feature_type": astrofeature_type}, # Composite key
        relationship_type=HAS_FEATURE_REL,
        rel_props_model=props
    )

# 2. Person -> HAS_FEATURE -> HDFeature
async def link_person_to_hdfeature(user_id: str, hdfeature_name: str, hdfeature_type: str, props: Optional[HasFeatureProperties] = None) -> Optional[Dict[str, Any]]:
    return await merge_relationship(
        start_node_label=PERSON_LABEL,
        start_node_props={"user_id": user_id},
        end_node_label=HDFEATURE_LABEL,
        end_node_props={"name": hdfeature_name, "feature_type": hdfeature_type}, # Composite key
        relationship_type=HAS_FEATURE_REL,
        rel_props_model=props
    )

# 3. Person -> HAS_TYPOLOGY_RESULT -> TypologyResult
async def link_person_to_typologyresult(user_id: str, assessment_id: str, props: Optional[HasTypologyResultProperties] = None) -> Optional[Dict[str, Any]]:
    return await merge_relationship(
        start_node_label=PERSON_LABEL,
        start_node_props={"user_id": user_id},
        end_node_label=TYPOLOGYRESULT_LABEL,
        end_node_props={"assessment_id": assessment_id}, # Unique key
        relationship_type=HAS_TYPOLOGY_RESULT_REL,
        rel_props_model=props
    )

# 4. AstroFeature -> INFLUENCES -> AstroFeature
async def link_astrofeature_influences_astrofeature(
    from_feature_name: str, from_feature_type: str,
    to_feature_name: str, to_feature_type: str,
    props: Optional[InfluencesProperties] = None
) -> Optional[Dict[str, Any]]:
    return await merge_relationship(
        start_node_label=ASTROFEATURE_LABEL,
        start_node_props={"name": from_feature_name, "feature_type": from_feature_type},
        end_node_label=ASTROFEATURE_LABEL,
        end_node_props={"name": to_feature_name, "feature_type": to_feature_type},
        relationship_type=INFLUENCES_REL,
        rel_props_model=props
    )

# TODO: Add similar specific linkers for HDFeature INFLUENCES HDFeature, CONFLICTS_WITH, etc.

async def get_relationships_from_node(
    node_label: str,
    node_props: Dict[str, Any],
    relationship_type: Optional[str] = None,
    direction: str = "out" # "out", "in", "both"
) -> List[Dict[str, Any]]:
    """
    Retrieves relationships connected to a specific node.
    Direction: "out" for outgoing, "in" for incoming, "both" for any.
    """
    match_props = ", ".join([f"{k}: ${k}_node" for k in node_props.keys()])
    params = {f"{k}_node": v for k, v in node_props.items()}

    rel_type_match = f":{relationship_type}" if relationship_type else ""

    if direction == "out":
        query = f"MATCH (start_node:{node_label} {{{match_props}}})-[r{rel_type_match}]->(end_node) RETURN type(r) as type, properties(r) as properties, elementId(start_node) as start_id, elementId(end_node) as end_id, properties(end_node) as end_node_props"
    elif direction == "in":
        query = f"MATCH (start_node)-[r{rel_type_match}]->(end_node:{node_label} {{{match_props}}}) RETURN type(r) as type, properties(r) as properties, elementId(start_node) as start_id, properties(start_node) as start_node_props, elementId(end_node) as end_id"
    else: # both
        query = f"MATCH (node1:{node_label} {{{match_props}}})-[r{rel_type_match}]-(node2) RETURN type(r) as type, properties(r) as properties, elementId(node1) as node1_id, properties(node1) as node1_props, elementId(node2) as node2_id, properties(node2) as node2_props"

    try:
        results = await execute_read_query(query, params)
        return results
    except DAOException as e:
        logger.error(f"DAOException fetching relationships for node {node_props} with type {relationship_type}: {e}", exc_info=True)
        return []

async def delete_relationship(
    start_node_label: str,
    start_node_props: Dict[str, Any],
    end_node_label: str,
    end_node_props: Dict[str, Any],
    relationship_type: str
) -> bool:
    """
    Deletes a specific relationship between two nodes.
    This version deletes ALL relationships of that type between those two nodes.
    To delete a specific relationship instance (if multiple can exist), elementId of the relationship would be needed.
    """
    start_match_props = ", ".join([f"{k}: ${k}_start" for k in start_node_props.keys()])
    end_match_props = ", ".join([f"{k}: ${k}_end" for k in end_node_props.keys()])

    params = {}
    for k, v in start_node_props.items():
        params[f"{k}_start"] = v
    for k, v in end_node_props.items():
        params[f"{k}_end"] = v

    query = f"""
    MATCH (start_node:{start_node_label} {{{start_match_props}}})-[r:{relationship_type}]->(end_node:{end_node_label} {{{end_match_props}}})
    DELETE r
    RETURN count(r) as deleted_count 
    """ # Note: count(r) after delete will be 0. Neo4j delete is idempotent.
    try:
        await execute_write_query(query, params)
        # If no error, assume success. A more robust check might query if the relationship still exists.
        return True
    except DAOException as e:
        logger.error(f"DAOException deleting relationship '{relationship_type}' from {start_node_props} to {end_node_props}: {e}", exc_info=True)
        return False


# Example Usage
if __name__ == "__main__":
    import asyncio
    from ..core.db import Neo4jDatabase
    from ..core.logging_config import setup_logging
    # Node models for test data creation
    from ..graph_schema.nodes import PersonNode, AstroFeatureNode


    setup_logging("INFO")

    async def test_relationship_crud():
        logger.info("Testing Relationship CRUD operations (using MERGE)...")
        user_id_rel_test = "rel_user_merge_001"
        astro_feat_name_rel_test = "Sun_in_Aries_RelMerge_Test"
        astro_feat_type_rel_test = "planet_in_sign_reltest"

        # 0. Create test nodes using their respective upsert functions
        person_details = PersonNode(user_id=user_id_rel_test, name="Rel Merge Test User")
        astro_details = AstroFeatureNode(name=astro_feat_name_rel_test, feature_type=astro_feat_type_rel_test, details={"orb": 1.0})
        
        person_node = None
        astro_node = None

        # Constraints are managed by schema_setup.py; no need to create/drop them here for tests
        # if the main schema setup is run. For isolated tests, one might.

        try:
            person_node = await person_upsert(person_details) # Uses new upsert
            astro_node = await astrofeature_upsert(astro_details) # Uses new upsert
            assert person_node and astro_node
            logger.info(f"Test nodes upserted: Person ID {person_node.user_id}, AstroFeature Name {astro_node.name}")

            # 1. Merge HAS_FEATURE relationship
            logger.info(f"Merging HAS_FEATURE between {user_id_rel_test} and {astro_feat_name_rel_test} ({astro_feat_type_rel_test})")
            has_feature_props = HasFeatureProperties(source_calculation_id="calc_rel_merge_001", relevance=0.8)
            
            rel_merged = await link_person_to_astrofeature(
                user_id_rel_test,
                astro_feat_name_rel_test,
                astro_feat_type_rel_test, # Added feature_type
                has_feature_props
            )
            assert rel_merged and rel_merged.get("type") == HAS_FEATURE_REL
            assert rel_merged.get("properties", {}).get("relevance") == 0.8
            logger.info(f"HAS_FEATURE relationship merged (created/updated): {rel_merged}")

            # Attempt to merge again with different properties to test update
            has_feature_props_updated = HasFeatureProperties(source_calculation_id="calc_rel_merge_001_v2", relevance=0.9)
            rel_merged_updated = await link_person_to_astrofeature(
                user_id_rel_test, astro_feat_name_rel_test, astro_feat_type_rel_test, has_feature_props_updated
            )
            assert rel_merged_updated and rel_merged_updated.get("properties", {}).get("relevance") == 0.9
            assert rel_merged_updated.get("properties", {}).get("source_calculation_id") == "calc_rel_merge_001_v2"
            logger.info(f"HAS_FEATURE relationship merged again (updated): {rel_merged_updated}")


            # 2. Get relationships from Person
            logger.info(f"Getting outgoing relationships for person {user_id_rel_test}")
            person_rels = await get_relationships_from_node(
                PERSON_LABEL,
                {"user_id": user_id_rel_test},
                HAS_FEATURE_REL,
                "out"
            )
            assert len(person_rels) == 1 # Should only be one due to MERGE
            assert person_rels[0].get("type") == HAS_FEATURE_REL
            assert person_rels[0].get("end_node_props", {}).get("name") == astro_feat_name_rel_test
            assert person_rels[0].get("properties", {}).get("relevance") == 0.9 # Check updated prop
            logger.info(f"Found relationships for person: {person_rels}")

            # 3. Delete relationship
            logger.info(f"Deleting HAS_FEATURE between {user_id_rel_test} and {astro_feat_name_rel_test} ({astro_feat_type_rel_test})")
            # Note: delete_relationship needs start/end node identifying props and rel type.
            # For AstroFeature, this means name AND feature_type.
            deleted_rel = await delete_relationship(
                PERSON_LABEL, {"user_id": user_id_rel_test},
                ASTROFEATURE_LABEL, {"name": astro_feat_name_rel_test, "feature_type": astro_feat_type_rel_test}, # Composite key for end node
                HAS_FEATURE_REL
            )
            assert deleted_rel is True
            logger.info(f"Relationship deletion status: {deleted_rel}")

            # Verify deletion
            person_rels_after_delete = await get_relationships_from_node(
                PERSON_LABEL, {"user_id": user_id_rel_test}, HAS_FEATURE_REL, "out"
            )
            assert len(person_rels_after_delete) == 0
            logger.info(f"Relationships for person after delete: {person_rels_after_delete}")

        except AssertionError as e:
            logger.error(f"Assertion failed during Relationship CRUD test: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"An error occurred during Relationship CRUD testing: {e}", exc_info=True)
        finally:
            # Cleanup nodes using their respective delete functions
            if astro_node: await astrofeature_delete(astro_feat_name_rel_test, astro_feat_type_rel_test) # Uses new delete
            if person_node: await delete_person(user_id_rel_test)
            
            await Neo4jDatabase.close_async_driver()
            logger.info("Neo4j async driver closed after Relationship CRUD tests.")

    asyncio.run(test_relationship_crud())