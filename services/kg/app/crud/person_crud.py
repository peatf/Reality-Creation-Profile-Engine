import logging
from typing import Optional, Dict, Any, List

from ..graph_schema.nodes import PersonNode
from ..graph_schema.constants import PERSON_LABEL
from .base_dao import (
    execute_read_query,
    execute_write_query, # Will be replaced by execute_merge_query for relevant ops
    execute_merge_query, # Added
    DAOException,
    NodeCreationError, # Can be refined or replaced by more general UpdateError/DAOException for MERGE
    NodeNotFoundError,
    UpdateError,
    DeletionError,
    UniqueConstraintViolationError # Should be less frequent with MERGE
)

logger = logging.getLogger(__name__)

async def upsert_person(person_data: PersonNode) -> PersonNode:
    """
    Creates a new Person node or updates an existing one based on user_id using MERGE.
    This operation is idempotent.
    """
    # Properties for MERGE. user_id is used in the MERGE clause.
    # Other properties are set ON CREATE or ON MATCH.
    # model_dump(exclude_none=True) is good to avoid setting properties to null if not provided.
    props_to_set = person_data.model_dump(exclude_none=True, exclude={"user_id"})

    # If birth_datetime_utc is present, ensure it's in a Neo4j-compatible format (e.g., ISO 8601 string or datetime object)
    # Pydantic models should handle datetime serialization if configured, but explicit conversion might be needed
    # if Neo4j driver has issues. For now, assume Pydantic model handles it.
    # If props_to_set has datetime, it should be fine.

    query = f"""
    MERGE (p:{PERSON_LABEL} {{user_id: $user_id}})
    ON CREATE SET p = $create_props, p.user_id = $user_id
    ON MATCH SET p += $match_props
    RETURN p
    """
    # For ON CREATE, we set all properties including user_id from the initial merge key.
    # $create_props includes all fields from person_data.
    # For ON MATCH, we update with $match_props.
    # It's often simpler to set all non-key props in both cases if the source data is complete.
    
    # Let's ensure create_props contains user_id for the initial set,
    # and match_props contains only fields to update.
    create_properties = person_data.model_dump(exclude_none=True)
    match_properties = props_to_set # Already excludes user_id

    parameters = {
        "user_id": person_data.user_id,
        "create_props": create_properties, # All props for creation
        "match_props": match_properties    # Props to update on match
    }

    try:
        # execute_merge_query returns a dict or None
        result_node_data = await execute_merge_query(query, parameters)
        
        if result_node_data and result_node_data.get("p"):
            # The result 'p' should be a dictionary of the node's properties
            return PersonNode(**result_node_data["p"])
        else:
            # This case implies the MERGE operation failed to return the node, which is unexpected.
            logger.error(f"Person node upsert did not return the node for user_id: {person_data.user_id}. Params: {parameters}")
            raise DAOException(f"Failed to upsert or retrieve person node for user_id: {person_data.user_id}")
    except UniqueConstraintViolationError as e: # Should not happen if MERGE is on the constrained property
        logger.error(f"Unexpected UniqueConstraintViolationError during MERGE for user_id {person_data.user_id}: {e}", exc_info=True)
        raise DAOException(f"Unique constraint issue during MERGE for person: {e}") from e
    except DAOException as e:
        logger.error(f"DAOException during person upsert for user_id {person_data.user_id}: {e}", exc_info=True)
        # Specific error types like NodeCreationError/UpdateError might be too granular for a MERGE.
        # DAOException or a more general UpsertError could be used.
        raise DAOException(f"Database error upserting person {person_data.user_id}: {e}") from e


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


async def update_person_properties(user_id: str, update_props: Dict[str, Any]) -> Optional[PersonNode]:
    """
    Updates specific properties of an existing Person node using MERGE ON MATCH.
    This operation is idempotent for the properties being updated.
    It will NOT create a person if one doesn't exist with the given user_id.
    Returns the updated PersonNode if found and updated, None otherwise.
    """
    if not update_props:
        logger.warning(f"update_person_properties called for user_id '{user_id}' with no properties to update.")
        return await get_person_by_user_id(user_id) # Return current state if no updates

    # Ensure user_id is not in the properties to set, as it's the merge key
    if "user_id" in update_props:
        logger.warning(f"Attempted to update 'user_id' via update_person_properties for {user_id}. 'user_id' cannot be changed.")
        del update_props["user_id"]
        if not update_props: # If only user_id was passed
            return await get_person_by_user_id(user_id)


    # We need to ensure the node exists to perform an update.
    # MERGE ... ON MATCH SET will not fail if the node doesn't exist, it just won't do anything.
    # To provide feedback (NodeNotFoundError), we can either do a preliminary check
    # or adjust the query to conditionally return something that indicates no match.

    # Approach 1: Preliminary check (safer for NodeNotFoundError)
    # existing_person = await get_person_by_user_id(user_id)
    # if not existing_person:
    #     raise NodeNotFoundError(f"Person with user_id '{user_id}' not found for update.")

    # Approach 2: Use OPTIONAL MATCH with MERGE or a more complex return
    # For simplicity and to align with MERGE's nature, we'll use MERGE and check the result.
    # If the node doesn't exist, MERGE won't match, and `p` won't be returned.

    query = f"""
    MATCH (p:{PERSON_LABEL} {{user_id: $user_id}})
    SET p += $props_to_set
    RETURN p
    """
    # This is not using MERGE, but a MATCH SET. If node not found, it returns nothing.
    # To use MERGE strictly for ON MATCH:
    # MERGE (p:Person {user_id: $user_id})
    # ON MATCH SET p += $props_to_set
    # RETURN p
    # This query, if p does not exist, will create p with only user_id and then try to apply props_to_set.
    # This is not the desired "update only if exists" behavior.
    # So, MATCH followed by SET is more appropriate for a strict update.
    # Or, a more complex MERGE that doesn't create:
    # MERGE (p:Person {user_id: $user_id}) WHERE EXISTS(p.user_id) <--- This doesn't quite work as MERGE itself finds or creates.
    #
    # The original MATCH + SET is fine for "update if exists".
    # Let's stick to MATCH + SET for this specific "update-only" behavior.
    # If we wanted MERGE for this, it would be:
    # MERGE (p:Person {user_id: $user_id})
    #   // No ON CREATE
    # ON MATCH SET p += $props_to_set
    # WITH p, CASE WHEN p IS NULL THEN false ELSE true END as matched
    # WHERE matched // Ensure we only proceed if matched
    # RETURN p
    # This is getting complex. The original MATCH/SET is clearer for "update only".
    # Let's refine the MATCH/SET to ensure it returns the node or indicates not found.

    parameters = {"user_id": user_id, "props_to_set": update_props}

    try:
        # Using execute_write_query as it's a targeted update, not a full merge/upsert.
        result_node_data = await execute_write_query(query, parameters)

        if result_node_data and result_node_data.get("p"):
            return PersonNode(**result_node_data["p"])
        else:
            # If no result, the node was not found by MATCH
            logger.warning(f"Person with user_id '{user_id}' not found for update using MATCH SET.")
            # To maintain consistency with previous error type:
            # We must check if it *really* doesn't exist, because an empty SET might also return null.
            # However, `update_props` is checked for emptiness.
            # If query returns nothing, it means MATCH failed.
            raise NodeNotFoundError(f"Person with user_id '{user_id}' not found for update.")

    except DAOException as e: # Includes NodeNotFoundError if execute_write_query raises it based on its logic
        if isinstance(e, NodeNotFoundError): # Re-raise if it's already the correct type
             raise
        logger.error(f"DAOException during person properties update for user_id {user_id}: {e}", exc_info=True)
        raise UpdateError(f"Database error updating person properties for {user_id}: {e}") from e


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
            # Upsert (Create)
            logger.info(f"Attempting to upsert (create) person: {test_user_id}")
            created_person = await upsert_person(person_details)
            assert created_person is not None
            assert created_person.user_id == test_user_id
            assert created_person.name == "CRUD Test User"
            logger.info(f"Person upserted (created) successfully: {created_person.model_dump_json(indent=2)}")

            # Get
            logger.info(f"Attempting to get person: {test_user_id}")
            retrieved_person = await get_person_by_user_id(test_user_id)
            assert retrieved_person is not None
            assert retrieved_person.user_id == test_user_id
            logger.info(f"Person retrieved successfully: {retrieved_person.model_dump_json(indent=2)}")

            # Upsert (Update existing)
            logger.info(f"Attempting to upsert (update) person: {test_user_id}")
            updated_person_details = PersonNode(
                user_id=test_user_id,
                name="CRUD Test User Upserted",
                profile_id="prof_crud_xyz", # Changed profile_id
                birth_latitude=12.34
                # birth_datetime_utc and birth_longitude will be set to None if not provided
                # and exclude_none=True is used in model_dump.
                # If we want to keep old values for unspecified fields in an upsert,
                # the logic in upsert_person for ON MATCH SET p += $match_props needs care.
                # Currently, it overwrites with all provided fields in person_data.
            )
            # To truly test partial update with upsert, person_details for ON MATCH should only contain changed fields.
            # The current upsert_person sets all fields from person_data on match.
            # This is fine for a "replace all properties" type of upsert.
            upserted_person = await upsert_person(updated_person_details)
            assert upserted_person is not None
            assert upserted_person.name == "CRUD Test User Upserted"
            assert upserted_person.profile_id == "prof_crud_xyz"
            assert upserted_person.birth_latitude == 12.34
            # Check if previously set longitude is still there or wiped, based on exclude_none behavior
            # If updated_person_details didn't include birth_longitude, it would be None if not handled.
            # The current `props_to_set = person_data.model_dump(exclude_none=True, exclude={"user_id"})`
            # means if `birth_longitude` was not in `updated_person_details`, it won't be in `match_properties`
            # and `p += match_properties` will not remove it. This is good.
            assert upserted_person.birth_longitude == 20.0 # Should remain from initial creation.

            logger.info(f"Person upserted (updated) successfully: {upserted_person.model_dump_json(indent=2)}")

            # Update specific properties (Partial Update)
            logger.info(f"Attempting to update specific properties for person: {test_user_id}")
            partial_update_payload = {"name": "CRUD Test User Partially Updated"}
            partially_updated_person = await update_person_properties(test_user_id, partial_update_payload)
            assert partially_updated_person is not None
            assert partially_updated_person.name == "CRUD Test User Partially Updated"
            assert partially_updated_person.profile_id == "prof_crud_xyz" # Should remain from previous upsert
            logger.info(f"Person partially updated successfully: {partially_updated_person.model_dump_json(indent=2)}")

            # Test creating duplicate with old create_person (should not happen with upsert)
            # upsert_person will just update if called again with same user_id.
            logger.info(f"Attempting upsert again with original details (should just update): {test_user_id}")
            final_upsert_person = await upsert_person(person_details) # original details
            assert final_upsert_person.name == "CRUD Test User" # Back to original name
            logger.info(f"Person upserted again successfully: {final_upsert_person.model_dump_json(indent=2)}")


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