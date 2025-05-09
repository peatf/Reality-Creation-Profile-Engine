import logging
from typing import Optional, Dict, Any, List

from ..graph_schema.nodes import AstroFeatureNode
from ..graph_schema.constants import ASTROFEATURE_LABEL
from .base_dao import (
    execute_read_query,
    execute_merge_query, # Added
    execute_write_query, # Kept for specific updates/deletes if not using MERGE
    DAOException,
    NodeCreationError, # May be replaced by general DAOException for upserts
    NodeNotFoundError,
    UpdateError,
    DeletionError,
    UniqueConstraintViolationError # Should be rare with MERGE on composite key
)

logger = logging.getLogger(__name__)

async def upsert_astrofeature(feature_data: AstroFeatureNode) -> AstroFeatureNode:
    """
    Creates a new AstroFeature node or updates an existing one based on (name, feature_type) using MERGE.
    This operation is idempotent.
    """
    # Properties for MERGE. (name, feature_type) are used in the MERGE clause.
    # Other properties are set ON CREATE or ON MATCH.
    props_to_set = feature_data.model_dump(exclude_none=True, exclude={"name", "feature_type"})

    # Pydantic's model_dump should handle Json[Dict[str, Any]] for 'details' correctly.
    # It serializes it to a JSON string.

    query = f"""
    MERGE (af:{ASTROFEATURE_LABEL} {{name: $name, feature_type: $feature_type}})
    ON CREATE SET af = $create_props, af.name = $name, af.feature_type = $feature_type
    ON MATCH SET af += $match_props
    RETURN af
    """
    
    create_properties = feature_data.model_dump(exclude_none=True) # All props for creation
    match_properties = props_to_set # Props to update on match (already excludes name, feature_type)

    parameters = {
        "name": feature_data.name,
        "feature_type": feature_data.feature_type,
        "create_props": create_properties,
        "match_props": match_properties
    }

    try:
        result_node_data = await execute_merge_query(query, parameters)
        
        if result_node_data and result_node_data.get("af"):
            return AstroFeatureNode(**result_node_data["af"])
        else:
            logger.error(f"AstroFeature node upsert did not return node for name: {feature_data.name}, type: {feature_data.feature_type}. Params: {parameters}")
            raise DAOException(f"Failed to upsert or retrieve AstroFeature node for name: {feature_data.name}, type: {feature_data.feature_type}")
    except UniqueConstraintViolationError as e: # Should not happen if MERGE is on the logical composite key
        logger.error(f"Unexpected UniqueConstraintViolationError during MERGE for AstroFeature {feature_data.name}, type: {feature_data.feature_type}: {e}", exc_info=True)
        raise DAOException(f"Unique constraint issue during MERGE for AstroFeature: {e}") from e
    except DAOException as e:
        logger.error(f"DAOException during AstroFeature upsert for {feature_data.name}, type: {feature_data.feature_type}: {e}", exc_info=True)
        raise DAOException(f"Database error upserting AstroFeature {feature_data.name}, type: {feature_data.feature_type}: {e}") from e


async def get_astrofeature_by_name_and_type(name: str, feature_type: str) -> Optional[AstroFeatureNode]:
    """
    Retrieves an AstroFeature node by its composite key (name, feature_type).
    """
    query = f"""
    MATCH (af:{ASTROFEATURE_LABEL} {{name: $name, feature_type: $feature_type}})
    RETURN af
    """
    parameters = {"name": name, "feature_type": feature_type}
    try:
        results = await execute_read_query(query, parameters)
        if results and results[0].get("af"):
            node_properties = results[0]["af"]
            return AstroFeatureNode(**node_properties)
        return None
    except DAOException as e:
        logger.error(f"DAOException while fetching AstroFeature by name {name}, type {feature_type}: {e}", exc_info=True)
        return None

async def get_astrofeatures_by_type(feature_type: str) -> List[AstroFeatureNode]:
    """
    Retrieves all AstroFeature nodes of a specific type.
    """
    query = f"""
    MATCH (af:{ASTROFEATURE_LABEL} {{feature_type: $feature_type}})
    RETURN af
    """
    features = []
    try:
        results = await execute_read_query(query, {"feature_type": feature_type})
        for record in results:
            if record.get("af"):
                features.append(AstroFeatureNode(**record["af"]))
        return features
    except DAOException as e:
        logger.error(f"DAOException while fetching AstroFeatures by type {feature_type}: {e}", exc_info=True)
        return [] # Return empty list on error


async def update_astrofeature_properties(name: str, feature_type: str, update_props: Dict[str, Any]) -> Optional[AstroFeatureNode]:
    """
    Updates specific properties of an existing AstroFeature node, identified by (name, feature_type).
    This operation is idempotent for the properties being updated.
    It will NOT create an AstroFeature if one doesn't exist.
    Returns the updated AstroFeatureNode if found and updated, None otherwise.
    """
    if not update_props:
        logger.warning(f"update_astrofeature_properties called for '{name}' ({feature_type}) with no properties to update.")
        return await get_astrofeature_by_name_and_type(name, feature_type)

    # Ensure key fields are not in the properties to set
    if "name" in update_props:
        logger.warning(f"Attempted to update 'name' via update_astrofeature_properties for {name} ({feature_type}). Key fields cannot be changed.")
        del update_props["name"]
    if "feature_type" in update_props:
        logger.warning(f"Attempted to update 'feature_type' via update_astrofeature_properties for {name} ({feature_type}). Key fields cannot be changed.")
        del update_props["feature_type"]
    
    if not update_props: # If only key fields were passed and now it's empty
        return await get_astrofeature_by_name_and_type(name, feature_type)

    query = f"""
    MATCH (af:{ASTROFEATURE_LABEL} {{name: $name, feature_type: $feature_type}})
    SET af += $props_to_set
    RETURN af
    """
    parameters = {
        "name": name,
        "feature_type": feature_type,
        "props_to_set": update_props
    }

    try:
        result_node_data = await execute_write_query(query, parameters) # Using execute_write_query for MATCH SET

        if result_node_data and result_node_data.get("af"):
            return AstroFeatureNode(**result_node_data["af"])
        else:
            # If no result, the node was not found by MATCH
            logger.warning(f"AstroFeature with name '{name}' and type '{feature_type}' not found for update.")
            raise NodeNotFoundError(f"AstroFeature with name '{name}' and type '{feature_type}' not found for update.")
    except DAOException as e:
        if isinstance(e, NodeNotFoundError):
            raise
        logger.error(f"DAOException during AstroFeature properties update for {name} ({feature_type}): {e}", exc_info=True)
        raise UpdateError(f"Database error updating AstroFeature properties for {name} ({feature_type}): {e}") from e


async def delete_astrofeature(name: str, feature_type: str) -> bool:
    """
    Deletes an AstroFeature node by its composite key (name, feature_type).
    Also detaches and deletes any relationships.
    Returns True if deletion was successful (node existed and was deleted), False otherwise.
    """
    # First, check if the feature exists to provide accurate return value/logging
    existing_feature = await get_astrofeature_by_name_and_type(name, feature_type)
    if not existing_feature:
        logger.warning(f"AstroFeature with name '{name}' and type '{feature_type}' not found for deletion.")
        return False

    query = f"""
    MATCH (af:{ASTROFEATURE_LABEL} {{name: $name, feature_type: $feature_type}})
    DETACH DELETE af
    """
    parameters = {"name": name, "feature_type": feature_type}
    try:
        await execute_write_query(query, parameters) # execute_write_query returns None or a record, not a summary for count
        # If execute_write_query doesn't raise an error, assume success for DETACH DELETE.
        return True
    except DAOException as e:
        logger.error(f"DAOException during AstroFeature deletion for {name} ({feature_type}): {e}", exc_info=True)
        raise DeletionError(f"Database error deleting AstroFeature {name} ({feature_type}): {e}") from e

# Example usage
if __name__ == "__main__":
    import asyncio
    # Use relative imports for internal modules when running as part of the package
    from ..core.db import Neo4jDatabase
    from ..core.logging_config import setup_logging

    # logging.basicConfig(level=logging.INFO) # Replaced by setup_logging
    setup_logging("INFO")

    async def test_astrofeature_crud():
        logger.info("Testing AstroFeature CRUD operations...")
        test_feature_name = "Sun_in_Leo_Test_CRUD"
        test_feature_type = "planet_in_sign"
        
        feature_details_initial = AstroFeatureNode(
            name=test_feature_name,
            feature_type=test_feature_type,
            details={"element": "Fire", "modality": "Fixed", "orb": 1.0}
        )
        upserted_feature = None

        # Note: Constraints on (name, feature_type) are not directly supported by Neo4j Community for unique constraints.
        # This uniqueness is typically enforced at the application layer or via workarounds.
        # The schema_setup.py now creates individual indexes on name and feature_type.

        try:
            # Upsert (Create)
            logger.info(f"Attempting to upsert (create) AstroFeature: {test_feature_name} ({test_feature_type})")
            upserted_feature = await upsert_astrofeature(feature_details_initial)
            assert upserted_feature is not None
            assert upserted_feature.name == test_feature_name
            assert upserted_feature.feature_type == test_feature_type
            assert upserted_feature.details["modality"] == "Fixed"
            logger.info(f"AstroFeature upserted (created): {upserted_feature.model_dump_json(indent=2)}")

            # Get by name and type
            retrieved_feature = await get_astrofeature_by_name_and_type(test_feature_name, test_feature_type)
            assert retrieved_feature is not None
            assert retrieved_feature.name == test_feature_name
            logger.info(f"AstroFeature retrieved: {retrieved_feature.model_dump_json(indent=2)}")

            # Get by type (verify our test feature is among them)
            typed_features = await get_astrofeatures_by_type(test_feature_type)
            assert any(f.name == test_feature_name and f.feature_type == test_feature_type for f in typed_features)
            logger.info(f"Found {len(typed_features)} features of type '{test_feature_type}'.")

            # Upsert (Update existing)
            feature_details_updated = AstroFeatureNode(
                name=test_feature_name, # Key
                feature_type=test_feature_type, # Key
                details={"element": "Fire", "modality": "Fixed", "description": "Kingly and proud", "orb": 1.5} # New detail, updated orb
            )
            logger.info(f"Attempting to upsert (update) AstroFeature: {test_feature_name} ({test_feature_type})")
            upserted_feature = await upsert_astrofeature(feature_details_updated)
            assert upserted_feature is not None
            assert upserted_feature.details.get("description") == "Kingly and proud"
            assert upserted_feature.details.get("orb") == 1.5
            logger.info(f"AstroFeature upserted (updated): {upserted_feature.model_dump_json(indent=2)}")

            # Update specific properties
            partial_update_payload = {"details": {"new_info": "Added later", "orb": 2.0}} # This will overwrite the whole details dict
                                                                                       # To merge, logic in update_astrofeature_properties would need to be smarter
                                                                                       # For now, it replaces. Let's test replacing details.
            logger.info(f"Attempting to update specific properties for AstroFeature: {test_feature_name} ({test_feature_type})")
            
            # To properly test partial update of 'details', we should fetch existing, modify, then update.
            current_details = upserted_feature.details.copy() if upserted_feature.details else {}
            current_details["new_info"] = "Added later"
            current_details["orb"] = 2.0
            
            updated_again_feature = await update_astrofeature_properties(
                test_feature_name, test_feature_type, {"details": current_details}
            )
            assert updated_again_feature is not None
            assert updated_again_feature.details.get("new_info") == "Added later"
            assert updated_again_feature.details.get("orb") == 2.0
            assert updated_again_feature.details.get("description") == "Kingly and proud" # Should persist
            logger.info(f"AstroFeature partially updated: {updated_again_feature.model_dump_json(indent=2)}")

            # Test upsert again with original details (should update back)
            logger.info(f"Attempting upsert again with original details: {test_feature_name} ({test_feature_type})")
            final_upsert_feature = await upsert_astrofeature(feature_details_initial)
            assert final_upsert_feature.details.get("orb") == 1.0
            assert "description" not in final_upsert_feature.details # Description was not in initial
            assert "new_info" not in final_upsert_feature.details   # New info was not in initial
            logger.info(f"AstroFeature upserted to original: {final_upsert_feature.model_dump_json(indent=2)}")

        except AssertionError as e:
            logger.error(f"Assertion failed during AstroFeature CRUD test: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"An error occurred during AstroFeature CRUD testing: {e}", exc_info=True)
        finally:
            if upserted_feature: # If any upsert was successful
                deleted = await delete_astrofeature(test_feature_name, test_feature_type)
                assert deleted is True
                logger.info(f"AstroFeature '{test_feature_name} ({test_feature_type})' deletion status: {deleted}")
                
                not_found_feature = await get_astrofeature_by_name_and_type(test_feature_name, test_feature_type)
                assert not_found_feature is None
                logger.info("AstroFeature successfully verified as deleted.")
            
            await Neo4jDatabase.close_async_driver()
            logger.info("Neo4j async driver closed after AstroFeature CRUD tests.")

    asyncio.run(test_astrofeature_crud())