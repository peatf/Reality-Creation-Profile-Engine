import logging
from typing import Optional, Dict, Any, List

from ..graph_schema.nodes import PersonNode
from ..graph_schema.constants import PERSON_LABEL # Import from constants
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

async def create_person(person_data: PersonNode) -> PersonNode:
    """
    Creates a new Person node in Neo4j.
    The user_id is expected to be unique due to constraints.
    """
    query = f"""
    CREATE (p:{PERSON_LABEL} $props)
    RETURN p
    """
    # Pydantic's model_dump is good for converting model to dict,
    # but Neo4j driver handles Pydantic models directly as properties in recent versions.
    # However, to be safe and explicit, especially with potential None values:
    props = person_data.model_dump(exclude_none=True)

    try:
        result_node_props = await execute_write_query(query, {"props": props})
        if result_node_props and result_node_props.get("p"):
            created_node_data = result_node_props["p"]
            # The driver might return an internal Node object or its properties.
            # We reconstruct our Pydantic model from the returned properties.
            return PersonNode(**created_node_data)
        else:
            # This case should ideally not be hit if CREATE was successful and RETURN p was used.
            logger.error(f"Person node creation did not return the node for user_id: {person_data.user_id}. Props: {props}")
            raise NodeCreationError(f"Failed to create or retrieve person node for user_id: {person_data.user_id}")
    except UniqueConstraintViolationError as e:
        logger.warning(f"Person with user_id '{person_data.user_id}' already exists.")
        raise NodeCreationError(f"Person with user_id '{person_data.user_id}' already exists.") from e
    except DAOException as e:
        logger.error(f"DAOException during person creation for user_id {person_data.user_id}: {e}", exc_info=True)
        raise NodeCreationError(f"Database error creating person: {e}") from e


async def get_person_by_user_id(user_id: str) -> Optional[PersonNode]:
    """
    Retrieves a Person node by its user_id.
    """
    query = f"""
    MATCH (p:{PERSON_LABEL} {{user_id: $user_id}})
    RETURN p
    """
    try:
        results = await execute_read_query(query, {"user_id": user_id})
        if results and results[0].get("p"):
            # Assuming the query returns a list of records, and each record has a 'p' key
            # whose value is a dictionary of the node's properties.
            node_properties = results[0]["p"]
            return PersonNode(**node_properties)
        return None
    except DAOException as e:
        logger.error(f"DAOException while fetching person by user_id {user_id}: {e}", exc_info=True)
        # Depending on desired behavior, could re-raise or return None.
        # For a 'get' operation, returning None on error might be acceptable.
        return None


async def update_person(user_id: str, update_data: Dict[str, Any]) -> Optional[PersonNode]:
    """
    Updates properties of an existing Person node.
    `update_data` should be a dictionary of properties to update.
    It does not allow updating the user_id.
    """
    if "user_id" in update_data:
        del update_data["user_id"] # Prevent changing the unique identifier

    if not update_data:
        logger.warning("Update_person called with no data to update.")
        return await get_person_by_user_id(user_id) # Return current state

    # Cypher SET clause for dynamic updates
    set_clauses = [f"p.{key} = $props.{key}" for key in update_data.keys()]
    query = f"""
    MATCH (p:{PERSON_LABEL} {{user_id: $user_id}})
    SET {', '.join(set_clauses)}
    RETURN p
    """
    parameters = {"user_id": user_id, "props": update_data}

    try:
        result_node_props = await execute_write_query(query, parameters)
        if result_node_props and result_node_props.get("p"):
            updated_node_data = result_node_props["p"]
            return PersonNode(**updated_node_data)
        else:
            # This could mean the node was not found or the update didn't return it as expected
            logger.warning(f"Person node with user_id '{user_id}' not found for update, or update query failed to return node.")
            # Check if node exists before raising NodeNotFoundError
            existing_person = await get_person_by_user_id(user_id)
            if not existing_person:
                raise NodeNotFoundError(f"Person with user_id '{user_id}' not found for update.")
            # If it exists but update failed to return, it's an UpdateError
            raise UpdateError(f"Failed to update or retrieve updated person node for user_id: {user_id}")

    except DAOException as e:
        logger.error(f"DAOException during person update for user_id {user_id}: {e}", exc_info=True)
        raise UpdateError(f"Database error updating person {user_id}: {e}") from e


async def delete_person(user_id: str) -> bool:
    """
    Deletes a Person node by its user_id.
    Also detaches and deletes any relationships connected to this node.
    Returns True if deletion was successful, False otherwise.
    """
    # DETACH DELETE removes the node and all its relationships.
    query = f"""
    MATCH (p:{PERSON_LABEL} {{user_id: $user_id}})
    DETACH DELETE p
    RETURN count(p) as deleted_count
    """
    # The RETURN count(p) after DELETE will be 0 if successful, as p no longer exists.
    # A better way to confirm deletion is to check the summary or if an error occurred.
    # For simplicity, we'll rely on execute_write_query not raising an error for success.
    # However, Neo4j's DELETE doesn't error if the node doesn't exist.
    # We should first check if the node exists to provide a more accurate return.

    existing_person = await get_person_by_user_id(user_id)
    if not existing_person:
        logger.warning(f"Person with user_id '{user_id}' not found for deletion.")
        return False # Or raise NodeNotFoundError based on desired API contract

    try:
        # We don't need the result of the delete query itself, just whether it succeeded.
        # execute_write_query will raise an exception on failure.
        await execute_write_query(query, {"user_id": user_id})
        # To be absolutely sure, one could try to fetch the node again and expect None.
        # For now, if no exception, assume success.
        return True
    except DAOException as e:
        logger.error(f"DAOException during person deletion for user_id {user_id}: {e}", exc_info=True)
        raise DeletionError(f"Database error deleting person {user_id}: {e}") from e


# Example usage (for testing this module directly)
if __name__ == "__main__":
    import asyncio
    # Use relative imports for internal modules when running as part of the package
    from ..core.db import Neo4jDatabase # For closing driver
    from ..core.logging_config import setup_logging

    # logging.basicConfig(level=logging.INFO) # Replaced by setup_logging
    setup_logging("INFO")

    async def test_person_crud():
        logger.info("Testing Person CRUD operations...")
        test_user_id = "crud_test_user_001"
        person_details = PersonNode(
            user_id=test_user_id,
            profile_id="prof_crud_abc",
            name="CRUD Test User",
            birth_datetime_utc=datetime.now(datetime.timezone.utc),
            birth_latitude=10.0,
            birth_longitude=20.0
        )

        created_person = None
        try:
            # Create
            logger.info(f"Attempting to create person: {test_user_id}")
            created_person = await create_person(person_details)
            assert created_person is not None
            assert created_person.user_id == test_user_id
            assert created_person.name == "CRUD Test User"
            logger.info(f"Person created successfully: {created_person.model_dump_json(indent=2)}")

            # Get
            logger.info(f"Attempting to get person: {test_user_id}")
            retrieved_person = await get_person_by_user_id(test_user_id)
            assert retrieved_person is not None
            assert retrieved_person.user_id == test_user_id
            logger.info(f"Person retrieved successfully: {retrieved_person.model_dump_json(indent=2)}")

            # Update
            logger.info(f"Attempting to update person: {test_user_id}")
            update_payload = {"name": "CRUD Test User Updated", "birth_latitude": 12.34}
            updated_person = await update_person(test_user_id, update_payload)
            assert updated_person is not None
            assert updated_person.name == "CRUD Test User Updated"
            assert updated_person.birth_latitude == 12.34
            logger.info(f"Person updated successfully: {updated_person.model_dump_json(indent=2)}")

            # Test creating duplicate (should fail due to constraint)
            logger.info(f"Attempting to create duplicate person (should fail): {test_user_id}")
            try:
                await create_person(person_details) # Same user_id
            except NodeCreationError as e:
                logger.info(f"Successfully caught error for duplicate creation: {e}")
            else:
                logger.error("Duplicate person creation did not fail as expected!")


        except AssertionError as e:
            logger.error(f"Assertion failed during CRUD test: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"An error occurred during Person CRUD testing: {e}", exc_info=True)
        finally:
            # Delete (cleanup)
            if created_person: # Only attempt delete if creation was likely successful
                logger.info(f"Attempting to delete person: {test_user_id}")
                deleted = await delete_person(test_user_id)
                assert deleted is True
                logger.info(f"Person deletion status: {deleted}")

                # Verify deletion
                logger.info(f"Verifying deletion of person: {test_user_id}")
                not_found_person = await get_person_by_user_id(test_user_id)
                assert not_found_person is None
                logger.info(f"Person successfully verified as deleted.")
            
            # Clean up any constraint if it was created by schema_setup.py for TestNode
            # This is more for the base_dao test, but good to be mindful of test states.
            try:
                await execute_write_query("DROP CONSTRAINT person_user_id_unique IF EXISTS")
                logger.info("Dropped person_user_id_unique constraint if it existed (for test cleanup).")
            except Exception:
                pass # Ignore if it fails (e.g., constraint didn't exist)

            await Neo4jDatabase.close_async_driver()
            logger.info("Neo4j async driver closed after Person CRUD tests.")

    asyncio.run(test_person_crud())