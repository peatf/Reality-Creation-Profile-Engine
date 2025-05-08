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
    execute_write_query,
    DAOException,
    RelationshipCreationError,
    NodeNotFoundError # To indicate if start/end nodes for relationship don't exist
)
# Import specific node CRUDs to verify node existence if needed, or rely on MATCH in Cypher
from .person_crud import get_person_by_user_id
from .astrofeature_crud import get_astrofeature_by_name
from .hdfeature_crud import get_hdfeature_by_name
from .typology_crud import get_typology_result_by_assessment_id


logger = logging.getLogger(__name__)

# --- Generic Relationship Creation ---
async def create_relationship(
    start_node_label: str,
    start_node_props: Dict[str, Any], # e.g., {"user_id": "id1"}
    end_node_label: str,
    end_node_props: Dict[str, Any],   # e.g., {"name": "feature_name"}
    relationship_type: str,
    rel_props_model: Optional[RelationshipBase] = None
) -> Optional[Dict[str, Any]]:
    """
    Creates a relationship of a given type between two nodes identified by their labels and properties.
    `start_node_props` and `end_node_props` must contain enough info to uniquely identify the nodes.
    Example: start_node_props={"user_id": "some_id"}, end_node_props={"name": "feature_name"}
    """
    # Build MATCH clauses for start and end nodes
    start_match_props = ", ".join([f"{k}: ${k}_start" for k in start_node_props.keys()])
    end_match_props = ", ".join([f"{k}: ${k}_end" for k in end_node_props.keys()])

    params = {}
    for k, v in start_node_props.items():
        params[f"{k}_start"] = v
    for k, v in end_node_props.items():
        params[f"{k}_end"] = v

    rel_props_cypher = ""
    if rel_props_model:
        rel_dict = rel_props_model.model_dump(exclude_none=True)
        if rel_dict:
            params["rel_props"] = rel_dict
            rel_props_cypher = "$rel_props"


    # MERGE relationship to avoid duplicates if properties are the same (or use CREATE if duplicates are allowed/handled)
    # Using CREATE as relationship properties might make them unique even if start/end nodes are same.
    # For simple existence, MERGE is better. For relationships with varying properties, CREATE is fine.
    # Let's use CREATE and assume higher-level logic handles preventing unwanted identical relationships if needed.
    query = f"""
    MATCH (start_node:{start_node_label} {{{start_match_props}}})
    MATCH (end_node:{end_node_label} {{{end_match_props}}})
    CREATE (start_node)-[r:{relationship_type} {rel_props_cypher}]->(end_node)
    RETURN type(r) as type, properties(r) as properties, elementId(start_node) as start_id, elementId(end_node) as end_id
    """

    try:
        # logger.debug(f"Executing create_relationship query: {query} with params: {params}")
        result = await execute_write_query(query, params)
        if result:
            return result # Contains type, properties, start_id, end_id
        else:
            # This could happen if either start_node or end_node was not found by the MATCH clause.
            # More specific checks for node existence can be done before calling this.
            logger.warning(f"Relationship '{relationship_type}' creation did not return result. Nodes might not exist or query failed. Start: {start_node_props}, End: {end_node_props}")
            # To provide better feedback, check if nodes exist:
            # This is simplified; a robust check would involve separate queries or more complex Cypher.
            raise RelationshipCreationError(f"Failed to create relationship '{relationship_type}'. Start or end node not found, or other error.")

    except DAOException as e:
        logger.error(f"DAOException creating relationship '{relationship_type}' from {start_node_props} to {end_node_props}: {e}", exc_info=True)
        raise RelationshipCreationError(f"Database error creating relationship '{relationship_type}': {e}") from e


# --- Specific Relationship CRUD Functions ---

# 1. Person -> HAS_FEATURE -> AstroFeature
async def link_person_to_astrofeature(user_id: str, astrofeature_name: str, props: Optional[HasFeatureProperties] = None) -> Optional[Dict[str, Any]]:
    return await create_relationship(
        start_node_label=PERSON_LABEL,
        start_node_props={"user_id": user_id},
        end_node_label=ASTROFEATURE_LABEL,
        end_node_props={"name": astrofeature_name},
        relationship_type=HAS_FEATURE_REL,
        rel_props_model=props
    )

# 2. Person -> HAS_FEATURE -> HDFeature
async def link_person_to_hdfeature(user_id: str, hdfeature_name: str, props: Optional[HasFeatureProperties] = None) -> Optional[Dict[str, Any]]:
    return await create_relationship(
        start_node_label=PERSON_LABEL,
        start_node_props={"user_id": user_id},
        end_node_label=HDFEATURE_LABEL,
        end_node_props={"name": hdfeature_name},
        relationship_type=HAS_FEATURE_REL,
        rel_props_model=props
    )

# 3. Person -> HAS_TYPOLOGY_RESULT -> TypologyResult
async def link_person_to_typologyresult(user_id: str, assessment_id: str, props: Optional[HasTypologyResultProperties] = None) -> Optional[Dict[str, Any]]:
    return await create_relationship(
        start_node_label=PERSON_LABEL,
        start_node_props={"user_id": user_id},
        end_node_label=TYPOLOGYRESULT_LABEL,
        end_node_props={"assessment_id": assessment_id},
        relationship_type=HAS_TYPOLOGY_RESULT_REL,
        rel_props_model=props
    )

# 4. AstroFeature -> INFLUENCES -> AstroFeature
async def link_astrofeature_influences_astrofeature(from_feature_name: str, to_feature_name: str, props: Optional[InfluencesProperties] = None) -> Optional[Dict[str, Any]]:
    return await create_relationship(
        start_node_label=ASTROFEATURE_LABEL,
        start_node_props={"name": from_feature_name},
        end_node_label=ASTROFEATURE_LABEL,
        end_node_props={"name": to_feature_name},
        relationship_type=INFLUENCES_REL,
        rel_props_model=props
    )

# Similar functions for HDFeature INFLUENCES HDFeature, AstroFeature CONFLICTS_WITH AstroFeature, etc.
# For brevity, only a few examples are fully fleshed out.

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
    # Use relative imports for internal modules when running as part of the package
    from ..core.db import Neo4jDatabase
    from .person_crud import create_person, delete_person, PersonNode # Already relative
    from .astrofeature_crud import create_astrofeature, delete_astrofeature, AstroFeatureNode # Already relative
    from ..core.logging_config import setup_logging

    # logging.basicConfig(level=logging.INFO) # Replaced by setup_logging
    setup_logging("INFO")

    async def test_relationship_crud():
        logger.info("Testing Relationship CRUD operations...")
        user_id_rel_test = "rel_user_test_001"
        astro_feat_name_rel_test = "Sun_in_Aries_Rel_Test"

        # 0. Create test nodes
        person_details = PersonNode(user_id=user_id_rel_test, name="Rel Test User")
        astro_details = AstroFeatureNode(name=astro_feat_name_rel_test, feature_type="planet_in_sign")
        
        person_node = None
        astro_node = None

        try:
            # Apply constraints (normally by schema_setup.py)
            await execute_write_query(f"CREATE CONSTRAINT IF NOT EXISTS person_user_id_unique_reltest FOR (p:{PERSON_LABEL}) REQUIRE p.user_id IS UNIQUE")
            await execute_write_query(f"CREATE CONSTRAINT IF NOT EXISTS astrofeature_name_unique_reltest FOR (af:{ASTROFEATURE_LABEL}) REQUIRE af.name IS UNIQUE")

            person_node = await create_person(person_details)
            astro_node = await create_astrofeature(astro_details)
            assert person_node and astro_node

            # 1. Create HAS_FEATURE relationship
            logger.info(f"Creating HAS_FEATURE between {user_id_rel_test} and {astro_feat_name_rel_test}")
            has_feature_props = HasFeatureProperties(source_calculation_id="calc_rel_test_001")
            rel_created = await link_person_to_astrofeature(user_id_rel_test, astro_feat_name_rel_test, has_feature_props)
            assert rel_created and rel_created.get("type") == HAS_FEATURE_REL
            logger.info(f"HAS_FEATURE relationship created: {rel_created}")

            # 2. Get relationships from Person
            logger.info(f"Getting outgoing relationships for person {user_id_rel_test}")
            person_rels = await get_relationships_from_node(PERSON_LABEL, {"user_id": user_id_rel_test}, HAS_FEATURE_REL, "out")
            assert len(person_rels) > 0
            assert person_rels[0].get("type") == HAS_FEATURE_REL
            assert person_rels[0].get("end_node_props", {}).get("name") == astro_feat_name_rel_test
            logger.info(f"Found relationships for person: {person_rels}")

            # 3. Delete relationship
            logger.info(f"Deleting HAS_FEATURE between {user_id_rel_test} and {astro_feat_name_rel_test}")
            deleted_rel = await delete_relationship(
                PERSON_LABEL, {"user_id": user_id_rel_test},
                ASTROFEATURE_LABEL, {"name": astro_feat_name_rel_test},
                HAS_FEATURE_REL
            )
            assert deleted_rel is True
            logger.info(f"Relationship deletion status: {deleted_rel}")

            # Verify deletion
            person_rels_after_delete = await get_relationships_from_node(PERSON_LABEL, {"user_id": user_id_rel_test}, HAS_FEATURE_REL, "out")
            assert len(person_rels_after_delete) == 0
            logger.info(f"Relationships for person after delete: {person_rels_after_delete}")


        except AssertionError as e:
            logger.error(f"Assertion failed during Relationship CRUD test: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"An error occurred during Relationship CRUD testing: {e}", exc_info=True)
        finally:
            # Cleanup nodes
            if astro_node: await delete_astrofeature(astro_feat_name_rel_test)
            if person_node: await delete_person(user_id_rel_test)
            
            # Cleanup test constraints
            await execute_write_query("DROP CONSTRAINT person_user_id_unique_reltest IF EXISTS")
            await execute_write_query("DROP CONSTRAINT astrofeature_name_unique_reltest IF EXISTS")

            await Neo4jDatabase.close_async_driver()
            logger.info("Neo4j async driver closed after Relationship CRUD tests.")

    asyncio.run(test_relationship_crud())