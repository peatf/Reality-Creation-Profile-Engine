import logging
from typing import Optional, Dict, Any, List

from ..graph_schema.nodes import TypologyResultNode
from ..graph_schema.constants import TYPOLOGYRESULT_LABEL
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

async def upsert_typology_result(result_data: TypologyResultNode) -> TypologyResultNode:
    """
    Creates a new TypologyResult node or updates an existing one based on assessment_id using MERGE.
    This operation is idempotent.
    """
    props_to_set = result_data.model_dump(exclude_none=True, exclude={"assessment_id"})

    query = f"""
    MERGE (tr:{TYPOLOGYRESULT_LABEL} {{assessment_id: $assessment_id}})
    ON CREATE SET tr = $create_props, tr.assessment_id = $assessment_id
    ON MATCH SET tr += $match_props
    RETURN tr
    """
    
    create_properties = result_data.model_dump(exclude_none=True)
    match_properties = props_to_set

    parameters = {
        "assessment_id": result_data.assessment_id,
        "create_props": create_properties,
        "match_props": match_properties
    }

    try:
        result_node_data = await execute_merge_query(query, parameters)
        
        if result_node_data and result_node_data.get("tr"):
            return TypologyResultNode(**result_node_data["tr"])
        else:
            logger.error(f"TypologyResult node upsert did not return node for assessment_id: {result_data.assessment_id}. Params: {parameters}")
            raise DAOException(f"Failed to upsert or retrieve TypologyResult node for assessment_id: {result_data.assessment_id}")
    except UniqueConstraintViolationError as e: # Should not happen if MERGE is on the constrained property
        logger.error(f"Unexpected UniqueConstraintViolationError during MERGE for assessment_id {result_data.assessment_id}: {e}", exc_info=True)
        raise DAOException(f"Unique constraint issue during MERGE for TypologyResult: {e}") from e
    except DAOException as e:
        logger.error(f"DAOException during TypologyResult upsert for assessment_id {result_data.assessment_id}: {e}", exc_info=True)
        raise DAOException(f"Database error upserting TypologyResult {result_data.assessment_id}: {e}") from e


async def get_typology_result_by_assessment_id(assessment_id: str) -> Optional[TypologyResultNode]:
    """
    Retrieves a TypologyResult node by its unique assessment_id.
    """
    query = f"""
    MATCH (tr:{TYPOLOGYRESULT_LABEL} {{assessment_id: $assessment_id}})
    RETURN tr
    """
    try:
        results = await execute_read_query(query, {"assessment_id": assessment_id})
        if results and results[0].get("tr"):
            node_properties = results[0]["tr"]
            return TypologyResultNode(**node_properties)
        return None
    except DAOException as e:
        logger.error(f"DAOException while fetching TypologyResult by assessment_id {assessment_id}: {e}", exc_info=True)
        return None

async def get_typology_results_by_name(typology_name: str) -> List[TypologyResultNode]:
    """
    Retrieves all TypologyResult nodes for a specific typology system name.
    """
    query = f"""
    MATCH (tr:{TYPOLOGYRESULT_LABEL} {{typology_name: $typology_name}})
    RETURN tr
    """
    results_list = []
    try:
        records = await execute_read_query(query, {"typology_name": typology_name})
        for record in records:
            if record.get("tr"):
                results_list.append(TypologyResultNode(**record["tr"]))
        return results_list
    except DAOException as e:
        logger.error(f"DAOException while fetching TypologyResults by name {typology_name}: {e}", exc_info=True)
        return []


async def update_typology_result_properties(assessment_id: str, update_props: Dict[str, Any]) -> Optional[TypologyResultNode]:
    """
    Updates specific properties of an existing TypologyResult node, identified by assessment_id.
    This operation is idempotent for the properties being updated.
    It will NOT create a TypologyResult if one doesn't exist.
    Returns the updated TypologyResultNode if found and updated, None otherwise.
    """
    if not update_props:
        logger.warning(f"update_typology_result_properties called for assessment_id '{assessment_id}' with no properties to update.")
        return await get_typology_result_by_assessment_id(assessment_id)

    if "assessment_id" in update_props:
        logger.warning(f"Attempted to update 'assessment_id' via update_typology_result_properties for {assessment_id}. Key field cannot be changed.")
        del update_props["assessment_id"]
    
    if not update_props: # If only assessment_id was passed
        return await get_typology_result_by_assessment_id(assessment_id)

    query = f"""
    MATCH (tr:{TYPOLOGYRESULT_LABEL} {{assessment_id: $assessment_id}})
    SET tr += $props_to_set
    RETURN tr
    """
    parameters = {
        "assessment_id": assessment_id,
        "props_to_set": update_props
    }

    try:
        result_node_data = await execute_write_query(query, parameters) # Using execute_write_query for MATCH SET

        if result_node_data and result_node_data.get("tr"):
            return TypologyResultNode(**result_node_data["tr"])
        else:
            logger.warning(f"TypologyResult with assessment_id '{assessment_id}' not found for update.")
            raise NodeNotFoundError(f"TypologyResult with assessment_id '{assessment_id}' not found for update.")
    except DAOException as e:
        if isinstance(e, NodeNotFoundError):
            raise
        logger.error(f"DAOException during TypologyResult properties update for {assessment_id}: {e}", exc_info=True)
        raise UpdateError(f"Database error updating TypologyResult properties for {assessment_id}: {e}") from e


async def delete_typology_result(assessment_id: str) -> bool:
    """
    Deletes a TypologyResult node by its assessment_id.
    Also detaches and deletes any relationships.
    """
    existing_result = await get_typology_result_by_assessment_id(assessment_id)
    if not existing_result:
        logger.warning(f"TypologyResult with assessment_id '{assessment_id}' not found for deletion.")
        return False

    query = f"""
    MATCH (tr:{TYPOLOGYRESULT_LABEL} {{assessment_id: $assessment_id}})
    DETACH DELETE tr
    """
    try:
        await execute_write_query(query, {"assessment_id": assessment_id})
        return True
    except DAOException as e:
        logger.error(f"DAOException during TypologyResult deletion for assessment_id {assessment_id}: {e}", exc_info=True)
        raise DeletionError(f"Database error deleting TypologyResult {assessment_id}: {e}") from e

# Example usage
if __name__ == "__main__":
    import asyncio
    # Use relative imports for internal modules when running as part of the package
    from ..core.db import Neo4jDatabase
    from ..core.logging_config import setup_logging

    # logging.basicConfig(level=logging.INFO) # Replaced by setup_logging
    setup_logging("INFO")

    async def test_typology_result_crud():
        logger.info("Testing TypologyResult CRUD operations...")
        test_assessment_id = "typology_assess_crud_001"
        result_details = TypologyResultNode(
            assessment_id=test_assessment_id,
            typology_name="TestTypology",
            score="Result X",
            confidence=0.9,
            details={"notes": "Initial test assessment"}
        )
        created_result = None
        try:
            # Apply constraint first
            try:
                await execute_write_query(f"CREATE CONSTRAINT IF NOT EXISTS typologyresult_assessment_id_unique_test FOR (tr:{TYPOLOGYRESULT_LABEL}) REQUIRE tr.assessment_id IS UNIQUE")
            except Exception as e:
                 logger.info(f"Constraint typologyresult_assessment_id_unique_test might already exist or failed to create: {e}")

            # Upsert (Create)
            logger.info(f"Attempting to upsert (create) TypologyResult: {test_assessment_id}")
            created_result = await upsert_typology_result(result_details)
            assert created_result is not None
            assert created_result.assessment_id == test_assessment_id
            assert created_result.score == "Result X"
            logger.info(f"TypologyResult upserted (created): {created_result.model_dump_json(indent=2)}")

            # Get
            retrieved_result = await get_typology_result_by_assessment_id(test_assessment_id)
            assert retrieved_result is not None
            assert retrieved_result.assessment_id == test_assessment_id
            logger.info(f"TypologyResult retrieved: {retrieved_result.model_dump_json(indent=2)}")

            # Get by typology name
            typed_results = await get_typology_results_by_name("TestTypology")
            assert any(r.assessment_id == test_assessment_id for r in typed_results)
            logger.info(f"Found {len(typed_results)} results for typology 'TestTypology'.")

            # Upsert (Update existing)
            updated_result_details = TypologyResultNode(
                assessment_id=test_assessment_id,
                typology_name="TestTypology", # Keep same or update
                score="Result Y", # Changed
                confidence=0.95,  # Changed
                details={"notes": "Updated test assessment", "version": 2} # Changed
            )
            logger.info(f"Attempting to upsert (update) TypologyResult: {test_assessment_id}")
            upserted_result = await upsert_typology_result(updated_result_details)
            assert upserted_result is not None
            assert upserted_result.score == "Result Y"
            assert upserted_result.confidence == 0.95
            assert upserted_result.details["version"] == 2
            logger.info(f"TypologyResult upserted (updated): {upserted_result.model_dump_json(indent=2)}")

            # Update specific properties
            partial_update_payload = {"score": "Result Z", "details": {"notes": "Partially updated", "version": 3, "extra": "Added"}}
            logger.info(f"Attempting to update specific properties for TypologyResult: {test_assessment_id}")
            partially_updated_result = await update_typology_result_properties(test_assessment_id, partial_update_payload)
            assert partially_updated_result is not None
            assert partially_updated_result.score == "Result Z"
            assert partially_updated_result.details["extra"] == "Added"
            assert partially_updated_result.confidence == 0.95 # Should remain from previous upsert
            logger.info(f"TypologyResult partially updated: {partially_updated_result.model_dump_json(indent=2)}")
            
            # Test upsert again with original details (should update back)
            logger.info(f"Attempting upsert again with original details: {test_assessment_id}")
            final_upsert_result = await upsert_typology_result(result_details) # original details
            assert final_upsert_result.score == "Result X"
            assert final_upsert_result.details["notes"] == "Initial test assessment"
            assert "version" not in final_upsert_result.details # version was not in initial
            logger.info(f"TypologyResult upserted to original: {final_upsert_result.model_dump_json(indent=2)}")

        except AssertionError as e:
            logger.error(f"Assertion failed during TypologyResult CRUD test: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"An error occurred during TypologyResult CRUD testing: {e}", exc_info=True)
        finally:
            if created_result:
                deleted = await delete_typology_result(test_assessment_id)
                assert deleted is True
                logger.info(f"TypologyResult '{test_assessment_id}' deletion status: {deleted}")
                not_found_result = await get_typology_result_by_assessment_id(test_assessment_id)
                assert not_found_result is None
                logger.info("TypologyResult successfully verified as deleted.")

            try:
                await execute_write_query("DROP CONSTRAINT typologyresult_assessment_id_unique_test IF EXISTS")
                logger.info("Dropped test constraint typologyresult_assessment_id_unique_test.")
            except Exception:
                pass

            await Neo4jDatabase.close_async_driver()
            logger.info("Neo4j async driver closed after TypologyResult CRUD tests.")

    asyncio.run(test_typology_result_crud())