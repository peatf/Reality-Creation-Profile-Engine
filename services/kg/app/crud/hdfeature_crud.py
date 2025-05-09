import logging
from typing import Optional, Dict, Any, List

from ..graph_schema.nodes import HDFeatureNode
from ..graph_schema.constants import HDFEATURE_LABEL
from .base_dao import (
    execute_read_query,
    execute_merge_query, # Added
    execute_write_query, # Kept for specific updates/deletes
    DAOException,
    NodeCreationError,
    NodeNotFoundError,
    UpdateError,
    DeletionError,
    UniqueConstraintViolationError
)

logger = logging.getLogger(__name__)

async def upsert_hdfeature(feature_data: HDFeatureNode) -> HDFeatureNode:
    """
    Creates a new HDFeature node or updates an existing one based on (name, feature_type) using MERGE.
    This operation is idempotent.
    """
    props_to_set = feature_data.model_dump(exclude_none=True, exclude={"name", "feature_type"})

    query = f"""
    MERGE (hf:{HDFEATURE_LABEL} {{name: $name, feature_type: $feature_type}})
    ON CREATE SET hf = $create_props, hf.name = $name, hf.feature_type = $feature_type
    ON MATCH SET hf += $match_props
    RETURN hf
    """
    
    create_properties = feature_data.model_dump(exclude_none=True)
    match_properties = props_to_set

    parameters = {
        "name": feature_data.name,
        "feature_type": feature_data.feature_type,
        "create_props": create_properties,
        "match_props": match_properties
    }

    try:
        result_node_data = await execute_merge_query(query, parameters)
        
        if result_node_data and result_node_data.get("hf"):
            return HDFeatureNode(**result_node_data["hf"])
        else:
            logger.error(f"HDFeature node upsert did not return node for name: {feature_data.name}, type: {feature_data.feature_type}. Params: {parameters}")
            raise DAOException(f"Failed to upsert or retrieve HDFeature node for name: {feature_data.name}, type: {feature_data.feature_type}")
    except UniqueConstraintViolationError as e:
        logger.error(f"Unexpected UniqueConstraintViolationError during MERGE for HDFeature {feature_data.name}, type: {feature_data.feature_type}: {e}", exc_info=True)
        raise DAOException(f"Unique constraint issue during MERGE for HDFeature: {e}") from e
    except DAOException as e:
        logger.error(f"DAOException during HDFeature upsert for {feature_data.name}, type: {feature_data.feature_type}: {e}", exc_info=True)
        raise DAOException(f"Database error upserting HDFeature {feature_data.name}, type: {feature_data.feature_type}: {e}") from e


async def get_hdfeature_by_name_and_type(name: str, feature_type: str) -> Optional[HDFeatureNode]:
    """
    Retrieves an HDFeature node by its composite key (name, feature_type).
    """
    query = f"""
    MATCH (hf:{HDFEATURE_LABEL} {{name: $name, feature_type: $feature_type}})
    RETURN hf
    """
    parameters = {"name": name, "feature_type": feature_type}
    try:
        results = await execute_read_query(query, parameters)
        if results and results[0].get("hf"):
            node_properties = results[0]["hf"]
            return HDFeatureNode(**node_properties)
        return None
    except DAOException as e:
        logger.error(f"DAOException while fetching HDFeature by name {name}, type {feature_type}: {e}", exc_info=True)
        return None

async def get_hdfeatures_by_type(feature_type: str) -> List[HDFeatureNode]:
    """
    Retrieves all HDFeature nodes of a specific type.
    """
    query = f"""
    MATCH (hf:{HDFEATURE_LABEL} {{feature_type: $feature_type}})
    RETURN hf
    """
    features = []
    try:
        results = await execute_read_query(query, {"feature_type": feature_type})
        for record in results:
            if record.get("hf"):
                features.append(HDFeatureNode(**record["hf"]))
        return features
    except DAOException as e:
        logger.error(f"DAOException while fetching HDFeatures by type {feature_type}: {e}", exc_info=True)
        return []


async def update_hdfeature_properties(name: str, feature_type: str, update_props: Dict[str, Any]) -> Optional[HDFeatureNode]:
    """
    Updates specific properties of an existing HDFeature node, identified by (name, feature_type).
    This operation is idempotent for the properties being updated.
    It will NOT create an HDFeature if one doesn't exist.
    Returns the updated HDFeatureNode if found and updated, None otherwise.
    """
    if not update_props:
        logger.warning(f"update_hdfeature_properties called for '{name}' ({feature_type}) with no properties to update.")
        return await get_hdfeature_by_name_and_type(name, feature_type)

    if "name" in update_props:
        logger.warning(f"Attempted to update 'name' via update_hdfeature_properties for {name} ({feature_type}). Key fields cannot be changed.")
        del update_props["name"]
    if "feature_type" in update_props:
        logger.warning(f"Attempted to update 'feature_type' via update_hdfeature_properties for {name} ({feature_type}). Key fields cannot be changed.")
        del update_props["feature_type"]
    
    if not update_props:
        return await get_hdfeature_by_name_and_type(name, feature_type)

    query = f"""
    MATCH (hf:{HDFEATURE_LABEL} {{name: $name, feature_type: $feature_type}})
    SET hf += $props_to_set
    RETURN hf
    """
    parameters = {
        "name": name,
        "feature_type": feature_type,
        "props_to_set": update_props
    }

    try:
        result_node_data = await execute_write_query(query, parameters)

        if result_node_data and result_node_data.get("hf"):
            return HDFeatureNode(**result_node_data["hf"])
        else:
            logger.warning(f"HDFeature with name '{name}' and type '{feature_type}' not found for update.")
            raise NodeNotFoundError(f"HDFeature with name '{name}' and type '{feature_type}' not found for update.")
    except DAOException as e:
        if isinstance(e, NodeNotFoundError):
            raise
        logger.error(f"DAOException during HDFeature properties update for {name} ({feature_type}): {e}", exc_info=True)
        raise UpdateError(f"Database error updating HDFeature properties for {name} ({feature_type}): {e}") from e


async def delete_hdfeature(name: str, feature_type: str) -> bool:
    """
    Deletes an HDFeature node by its composite key (name, feature_type).
    Also detaches and deletes any relationships.
    Returns True if deletion was successful, False otherwise.
    """
    existing_feature = await get_hdfeature_by_name_and_type(name, feature_type)
    if not existing_feature:
        logger.warning(f"HDFeature with name '{name}' and type '{feature_type}' not found for deletion.")
        return False

    query = f"""
    MATCH (hf:{HDFEATURE_LABEL} {{name: $name, feature_type: $feature_type}})
    DETACH DELETE hf
    """
    parameters = {"name": name, "feature_type": feature_type}
    try:
        await execute_write_query(query, parameters)
        return True
    except DAOException as e:
        logger.error(f"DAOException during HDFeature deletion for {name} ({feature_type}): {e}", exc_info=True)
        raise DeletionError(f"Database error deleting HDFeature {name} ({feature_type}): {e}") from e

# Example usage
if __name__ == "__main__":
    import asyncio
    # Use relative imports for internal modules when running as part of the package
    from ..core.db import Neo4jDatabase
    from ..core.logging_config import setup_logging

    # logging.basicConfig(level=logging.INFO) # Replaced by setup_logging
    setup_logging("INFO")

    async def test_hdfeature_crud():
        logger.info("Testing HDFeature CRUD operations...")
        test_feature_name = "Defined_Sacral_Test_CRUD"
        test_feature_type = "center"

        feature_details_initial = HDFeatureNode(
            name=test_feature_name,
            feature_type=test_feature_type,
            details={"state": "Defined", "function": "Life force, vitality", "awareness": False}
        )
        upserted_feature = None
        
        # Uniqueness for (name, feature_type) is application-enforced.
        # schema_setup.py creates individual indexes on name and feature_type.

        try:
            # Upsert (Create)
            logger.info(f"Attempting to upsert (create) HDFeature: {test_feature_name} ({test_feature_type})")
            upserted_feature = await upsert_hdfeature(feature_details_initial)
            assert upserted_feature is not None
            assert upserted_feature.name == test_feature_name
            assert upserted_feature.feature_type == test_feature_type
            assert upserted_feature.details["function"] == "Life force, vitality"
            logger.info(f"HDFeature upserted (created): {upserted_feature.model_dump_json(indent=2)}")

            # Get by name and type
            retrieved_feature = await get_hdfeature_by_name_and_type(test_feature_name, test_feature_type)
            assert retrieved_feature is not None
            assert retrieved_feature.name == test_feature_name
            logger.info(f"HDFeature retrieved: {retrieved_feature.model_dump_json(indent=2)}")

            # Get by type
            typed_features = await get_hdfeatures_by_type(test_feature_type)
            assert any(f.name == test_feature_name and f.feature_type == test_feature_type for f in typed_features)
            logger.info(f"Found {len(typed_features)} features of type '{test_feature_type}'.")

            # Upsert (Update existing)
            feature_details_updated = HDFeatureNode(
                name=test_feature_name,
                feature_type=test_feature_type,
                details={"state": "Defined", "function": "Life force and work", "awareness": False, "pressure_center": True}
            )
            logger.info(f"Attempting to upsert (update) HDFeature: {test_feature_name} ({test_feature_type})")
            upserted_feature = await upsert_hdfeature(feature_details_updated)
            assert upserted_feature is not None
            assert upserted_feature.details.get("function") == "Life force and work"
            assert upserted_feature.details.get("pressure_center") is True
            logger.info(f"HDFeature upserted (updated): {upserted_feature.model_dump_json(indent=2)}")

            # Update specific properties
            current_details = upserted_feature.details.copy() if upserted_feature.details else {}
            current_details["extra_info"] = "Added for partial update test"
            current_details["awareness"] = True # Change existing value in details
            
            logger.info(f"Attempting to update specific properties for HDFeature: {test_feature_name} ({test_feature_type})")
            updated_again_feature = await update_hdfeature_properties(
                test_feature_name, test_feature_type, {"details": current_details}
            )
            assert updated_again_feature is not None
            assert updated_again_feature.details.get("extra_info") == "Added for partial update test"
            assert updated_again_feature.details.get("awareness") is True
            assert updated_again_feature.details.get("function") == "Life force and work" # Should persist
            logger.info(f"HDFeature partially updated: {updated_again_feature.model_dump_json(indent=2)}")

            # Test upsert again with original details
            logger.info(f"Attempting upsert again with original details: {test_feature_name} ({test_feature_type})")
            final_upsert_feature = await upsert_hdfeature(feature_details_initial)
            assert final_upsert_feature.details.get("function") == "Life force, vitality"
            assert "pressure_center" not in final_upsert_feature.details
            assert "extra_info" not in final_upsert_feature.details
            logger.info(f"HDFeature upserted to original: {final_upsert_feature.model_dump_json(indent=2)}")

        except AssertionError as e:
            logger.error(f"Assertion failed during HDFeature CRUD test: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"An error occurred during HDFeature CRUD testing: {e}", exc_info=True)
        finally:
            if upserted_feature:
                deleted = await delete_hdfeature(test_feature_name, test_feature_type)
                assert deleted is True
                logger.info(f"HDFeature '{test_feature_name} ({test_feature_type})' deletion status: {deleted}")
                
                not_found_feature = await get_hdfeature_by_name_and_type(test_feature_name, test_feature_type)
                assert not_found_feature is None
                logger.info("HDFeature successfully verified as deleted.")

            await Neo4jDatabase.close_async_driver()
            logger.info("Neo4j async driver closed after HDFeature CRUD tests.")

    asyncio.run(test_hdfeature_crud())