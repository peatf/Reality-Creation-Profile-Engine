import logging
from typing import Optional, Dict, Any, List

from ..graph_schema.nodes import HDFeatureNode
from ..graph_schema.constants import HDFEATURE_LABEL # Import from constants
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

async def create_hdfeature(feature_data: HDFeatureNode) -> HDFeatureNode:
    """
    Creates a new HDFeature node in Neo4j.
    The 'name' property is expected to be unique due to constraints.
    """
    query = f"""
    CREATE (hf:{HDFEATURE_LABEL} $props)
    RETURN hf
    """
    props = feature_data.model_dump(exclude_none=True)
    # Similar to AstroFeature, ensure 'details' (Json type) is handled correctly by Pydantic/driver.

    try:
        result_node_props = await execute_write_query(query, {"props": props})
        if result_node_props and result_node_props.get("hf"):
            created_node_data = result_node_props["hf"]
            return HDFeatureNode(**created_node_data)
        else:
            logger.error(f"HDFeature node creation did not return node for name: {feature_data.name}. Props: {props}")
            raise NodeCreationError(f"Failed to create or retrieve HDFeature node for name: {feature_data.name}")
    except UniqueConstraintViolationError as e:
        logger.warning(f"HDFeature with name '{feature_data.name}' already exists.")
        raise NodeCreationError(f"HDFeature with name '{feature_data.name}' already exists.") from e
    except DAOException as e:
        logger.error(f"DAOException during HDFeature creation for name {feature_data.name}: {e}", exc_info=True)
        raise NodeCreationError(f"Database error creating HDFeature: {e}") from e


async def get_hdfeature_by_name(name: str) -> Optional[HDFeatureNode]:
    """
    Retrieves an HDFeature node by its unique name.
    """
    query = f"""
    MATCH (hf:{HDFEATURE_LABEL} {{name: $name}})
    RETURN hf
    """
    try:
        results = await execute_read_query(query, {"name": name})
        if results and results[0].get("hf"):
            node_properties = results[0]["hf"]
            return HDFeatureNode(**node_properties)
        return None
    except DAOException as e:
        logger.error(f"DAOException while fetching HDFeature by name {name}: {e}", exc_info=True)
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


async def update_hdfeature(name: str, update_data: Dict[str, Any]) -> Optional[HDFeatureNode]:
    """
    Updates properties of an existing HDFeature node.
    'name' is the unique identifier and cannot be changed.
    'feature_type' also typically shouldn't be changed.
    """
    if "name" in update_data:
        del update_data["name"]
    if "feature_type" in update_data:
        del update_data["feature_type"]

    if not update_data:
        logger.warning(f"Update_hdfeature called for '{name}' with no data to update.")
        return await get_hdfeature_by_name(name)

    set_clauses = [f"hf.{key} = $props.{key}" for key in update_data.keys()]
    query = f"""
    MATCH (hf:{HDFEATURE_LABEL} {{name: $name}})
    SET {', '.join(set_clauses)}
    RETURN hf
    """
    parameters = {"name": name, "props": update_data}

    try:
        result_node_props = await execute_write_query(query, parameters)
        if result_node_props and result_node_props.get("hf"):
            return HDFeatureNode(**result_node_props["hf"])
        else:
            existing_feature = await get_hdfeature_by_name(name)
            if not existing_feature:
                raise NodeNotFoundError(f"HDFeature with name '{name}' not found for update.")
            raise UpdateError(f"Failed to update or retrieve updated HDFeature node for name: {name}")
    except DAOException as e:
        logger.error(f"DAOException during HDFeature update for name {name}: {e}", exc_info=True)
        raise UpdateError(f"Database error updating HDFeature {name}: {e}") from e


async def delete_hdfeature(name: str) -> bool:
    """
    Deletes an HDFeature node by its name.
    Also detaches and deletes any relationships.
    """
    existing_feature = await get_hdfeature_by_name(name)
    if not existing_feature:
        logger.warning(f"HDFeature with name '{name}' not found for deletion.")
        return False

    query = f"""
    MATCH (hf:{HDFEATURE_LABEL} {{name: $name}})
    DETACH DELETE hf
    """
    try:
        await execute_write_query(query, {"name": name})
        return True
    except DAOException as e:
        logger.error(f"DAOException during HDFeature deletion for name {name}: {e}", exc_info=True)
        raise DeletionError(f"Database error deleting HDFeature {name}: {e}") from e

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
        feature_details = HDFeatureNode(
            name=test_feature_name,
            feature_type="center",
            details={"state": "Defined", "function": "Life force, vitality"}
        )
        created_feature = None
        try:
            # Apply constraint first
            try:
                await execute_write_query(f"CREATE CONSTRAINT IF NOT EXISTS hdfeature_name_unique_test FOR (hf:{HDFEATURE_LABEL}) REQUIRE hf.name IS UNIQUE")
            except Exception as e:
                 logger.info(f"Constraint hdfeature_name_unique_test might already exist or failed to create: {e}")

            # Create
            logger.info(f"Attempting to create HDFeature: {test_feature_name}")
            created_feature = await create_hdfeature(feature_details)
            assert created_feature is not None and created_feature.name == test_feature_name
            logger.info(f"HDFeature created: {created_feature.model_dump_json(indent=2)}")

            # Get
            retrieved_feature = await get_hdfeature_by_name(test_feature_name)
            assert retrieved_feature is not None and retrieved_feature.name == test_feature_name
            logger.info(f"HDFeature retrieved: {retrieved_feature.model_dump_json(indent=2)}")

            # Get by type
            typed_features = await get_hdfeatures_by_type("center")
            assert any(f.name == test_feature_name for f in typed_features)
            logger.info(f"Found {len(typed_features)} features of type 'center'.")

            # Update
            update_payload = {"details": {"state": "Defined", "function": "Life force, vitality", "not_self_theme_if_open": "Not knowing when enough is enough"}}
            updated_feature = await update_hdfeature(test_feature_name, update_payload)
            assert updated_feature is not None and updated_feature.details.get("not_self_theme_if_open") is not None
            logger.info(f"HDFeature updated: {updated_feature.model_dump_json(indent=2)}")

            # Test duplicate creation
            logger.info(f"Attempting to create duplicate HDFeature (should fail): {test_feature_name}")
            try:
                await create_hdfeature(feature_details)
            except NodeCreationError as e:
                logger.info(f"Successfully caught error for duplicate HDFeature creation: {e}")
            else:
                logger.error("Duplicate HDFeature creation did not fail as expected!")

        except AssertionError as e:
            logger.error(f"Assertion failed during HDFeature CRUD test: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"An error occurred during HDFeature CRUD testing: {e}", exc_info=True)
        finally:
            if created_feature:
                deleted = await delete_hdfeature(test_feature_name)
                assert deleted is True
                logger.info(f"HDFeature '{test_feature_name}' deletion status: {deleted}")
                not_found_feature = await get_hdfeature_by_name(test_feature_name)
                assert not_found_feature is None
                logger.info("HDFeature successfully verified as deleted.")

            try:
                await execute_write_query("DROP CONSTRAINT hdfeature_name_unique_test IF EXISTS")
                logger.info("Dropped test constraint hdfeature_name_unique_test.")
            except Exception:
                pass

            await Neo4jDatabase.close_async_driver()
            logger.info("Neo4j async driver closed after HDFeature CRUD tests.")

    asyncio.run(test_hdfeature_crud())