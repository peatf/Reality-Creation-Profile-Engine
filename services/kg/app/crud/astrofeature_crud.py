import logging
from typing import Optional, Dict, Any, List

from ..graph_schema.nodes import AstroFeatureNode
from ..graph_schema.constants import ASTROFEATURE_LABEL # Import from constants
from .base_dao import (
    execute_read_query,
    execute_write_query,
    DAOException,
    NodeCreationError,
    NodeNotFoundError,
    UpdateError,
    DeletionError,
    UniqueConstraintViolationError
)

logger = logging.getLogger(__name__)

async def create_astrofeature(feature_data: AstroFeatureNode) -> AstroFeatureNode:
    """
    Creates a new AstroFeature node in Neo4j.
    The 'name' property is expected to be unique due to constraints.
    """
    query = f"""
    CREATE (af:{ASTROFEATURE_LABEL} $props)
    RETURN af
    """
    props = feature_data.model_dump(exclude_none=True)
    # Convert details if it's a Pydantic Json type to a string for Neo4j,
    # or ensure your Neo4j version/driver handles it.
    # The Pydantic model should serialize `Json[Dict[str, Any]]` to a string.
    # If props['details'] is already a string, it's fine. If it's a dict, json.dumps might be needed
    # if the driver doesn't handle dict-to-JSON-string for a JSON property type automatically.
    # For now, assume Pydantic model_dump handles Json type correctly for the driver.

    try:
        result_node_props = await execute_write_query(query, {"props": props})
        if result_node_props and result_node_props.get("af"):
            created_node_data = result_node_props["af"]
            return AstroFeatureNode(**created_node_data)
        else:
            logger.error(f"AstroFeature node creation did not return node for name: {feature_data.name}. Props: {props}")
            raise NodeCreationError(f"Failed to create or retrieve AstroFeature node for name: {feature_data.name}")
    except UniqueConstraintViolationError as e:
        logger.warning(f"AstroFeature with name '{feature_data.name}' already exists.")
        # Depending on desired behavior, could fetch existing instead of raising error.
        # For now, strict creation: if it exists, it's an error for 'create' operation.
        raise NodeCreationError(f"AstroFeature with name '{feature_data.name}' already exists.") from e
    except DAOException as e:
        logger.error(f"DAOException during AstroFeature creation for name {feature_data.name}: {e}", exc_info=True)
        raise NodeCreationError(f"Database error creating AstroFeature: {e}") from e


async def get_astrofeature_by_name(name: str) -> Optional[AstroFeatureNode]:
    """
    Retrieves an AstroFeature node by its unique name.
    """
    query = f"""
    MATCH (af:{ASTROFEATURE_LABEL} {{name: $name}})
    RETURN af
    """
    try:
        results = await execute_read_query(query, {"name": name})
        if results and results[0].get("af"):
            node_properties = results[0]["af"]
            return AstroFeatureNode(**node_properties)
        return None
    except DAOException as e:
        logger.error(f"DAOException while fetching AstroFeature by name {name}: {e}", exc_info=True)
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


async def update_astrofeature(name: str, update_data: Dict[str, Any]) -> Optional[AstroFeatureNode]:
    """
    Updates properties of an existing AstroFeature node.
    'name' is the unique identifier and cannot be changed via this method.
    'feature_type' also typically shouldn't be changed.
    """
    if "name" in update_data:
        del update_data["name"]
    if "feature_type" in update_data: # Or raise error if trying to change type
        del update_data["feature_type"]

    if not update_data:
        logger.warning(f"Update_astrofeature called for '{name}' with no data to update.")
        return await get_astrofeature_by_name(name)

    set_clauses = [f"af.{key} = $props.{key}" for key in update_data.keys()]
    query = f"""
    MATCH (af:{ASTROFEATURE_LABEL} {{name: $name}})
    SET {', '.join(set_clauses)}
    RETURN af
    """
    parameters = {"name": name, "props": update_data}

    try:
        result_node_props = await execute_write_query(query, parameters)
        if result_node_props and result_node_props.get("af"):
            return AstroFeatureNode(**result_node_props["af"])
        else:
            existing_feature = await get_astrofeature_by_name(name)
            if not existing_feature:
                raise NodeNotFoundError(f"AstroFeature with name '{name}' not found for update.")
            raise UpdateError(f"Failed to update or retrieve updated AstroFeature node for name: {name}")
    except DAOException as e:
        logger.error(f"DAOException during AstroFeature update for name {name}: {e}", exc_info=True)
        raise UpdateError(f"Database error updating AstroFeature {name}: {e}") from e


async def delete_astrofeature(name: str) -> bool:
    """
    Deletes an AstroFeature node by its name.
    Also detaches and deletes any relationships.
    """
    existing_feature = await get_astrofeature_by_name(name)
    if not existing_feature:
        logger.warning(f"AstroFeature with name '{name}' not found for deletion.")
        return False

    query = f"""
    MATCH (af:{ASTROFEATURE_LABEL} {{name: $name}})
    DETACH DELETE af
    """
    try:
        await execute_write_query(query, {"name": name})
        return True
    except DAOException as e:
        logger.error(f"DAOException during AstroFeature deletion for name {name}: {e}", exc_info=True)
        raise DeletionError(f"Database error deleting AstroFeature {name}: {e}") from e

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
        feature_details = AstroFeatureNode(
            name=test_feature_name,
            feature_type="planet_in_sign",
            details={"element": "Fire", "modality": "Fixed"}
        )
        created_feature = None
        try:
            # Apply constraint first (normally done by schema_setup.py)
            try:
                await execute_write_query(f"CREATE CONSTRAINT IF NOT EXISTS astrofeature_name_unique_test FOR (af:{ASTROFEATURE_LABEL}) REQUIRE af.name IS UNIQUE")
            except Exception as e: # Catch if it already exists from a previous partial run
                 logger.info(f"Constraint astrofeature_name_unique_test might already exist or failed to create: {e}")


            # Create
            logger.info(f"Attempting to create AstroFeature: {test_feature_name}")
            created_feature = await create_astrofeature(feature_details)
            assert created_feature is not None and created_feature.name == test_feature_name
            logger.info(f"AstroFeature created: {created_feature.model_dump_json(indent=2)}")

            # Get
            retrieved_feature = await get_astrofeature_by_name(test_feature_name)
            assert retrieved_feature is not None and retrieved_feature.name == test_feature_name
            logger.info(f"AstroFeature retrieved: {retrieved_feature.model_dump_json(indent=2)}")

            # Get by type
            typed_features = await get_astrofeatures_by_type("planet_in_sign")
            assert any(f.name == test_feature_name for f in typed_features)
            logger.info(f"Found {len(typed_features)} features of type 'planet_in_sign'.")

            # Update
            update_payload = {"details": {"element": "Fire", "modality": "Fixed", "description": "Kingly and proud"}}
            updated_feature = await update_astrofeature(test_feature_name, update_payload)
            assert updated_feature is not None and updated_feature.details.get("description") == "Kingly and proud"
            logger.info(f"AstroFeature updated: {updated_feature.model_dump_json(indent=2)}")

            # Test duplicate creation
            logger.info(f"Attempting to create duplicate AstroFeature (should fail): {test_feature_name}")
            try:
                await create_astrofeature(feature_details)
            except NodeCreationError as e:
                logger.info(f"Successfully caught error for duplicate AstroFeature creation: {e}")
            else:
                logger.error("Duplicate AstroFeature creation did not fail as expected!")

        except AssertionError as e:
            logger.error(f"Assertion failed during AstroFeature CRUD test: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"An error occurred during AstroFeature CRUD testing: {e}", exc_info=True)
        finally:
            if created_feature:
                deleted = await delete_astrofeature(test_feature_name)
                assert deleted is True
                logger.info(f"AstroFeature '{test_feature_name}' deletion status: {deleted}")
                not_found_feature = await get_astrofeature_by_name(test_feature_name)
                assert not_found_feature is None
                logger.info("AstroFeature successfully verified as deleted.")
            
            # Clean up test constraint
            try:
                await execute_write_query("DROP CONSTRAINT astrofeature_name_unique_test IF EXISTS")
                logger.info("Dropped test constraint astrofeature_name_unique_test.")
            except Exception:
                pass

            await Neo4jDatabase.close_async_driver()
            logger.info("Neo4j async driver closed after AstroFeature CRUD tests.")

    asyncio.run(test_astrofeature_crud())