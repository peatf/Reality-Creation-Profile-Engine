import logging
from typing import Optional, Dict, Any, List

from ..graph_schema.nodes import TypologyResultNode
from ..graph_schema.constants import TYPOLOGYRESULT_LABEL # Import from constants
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

async def create_typology_result(result_data: TypologyResultNode) -> TypologyResultNode:
    """
    Creates a new TypologyResult node in Neo4j.
    The 'assessment_id' is expected to be unique.
    """
    query = f"""
    CREATE (tr:{TYPOLOGYRESULT_LABEL} $props)
    RETURN tr
    """
    props = result_data.model_dump(exclude_none=True)

    try:
        result_node_props = await execute_write_query(query, {"props": props})
        if result_node_props and result_node_props.get("tr"):
            created_node_data = result_node_props["tr"]
            return TypologyResultNode(**created_node_data)
        else:
            logger.error(f"TypologyResult node creation did not return node for assessment_id: {result_data.assessment_id}. Props: {props}")
            raise NodeCreationError(f"Failed to create or retrieve TypologyResult node for assessment_id: {result_data.assessment_id}")
    except UniqueConstraintViolationError as e:
        logger.warning(f"TypologyResult with assessment_id '{result_data.assessment_id}' already exists.")
        raise NodeCreationError(f"TypologyResult with assessment_id '{result_data.assessment_id}' already exists.") from e
    except DAOException as e:
        logger.error(f"DAOException during TypologyResult creation for assessment_id {result_data.assessment_id}: {e}", exc_info=True)
        raise NodeCreationError(f"Database error creating TypologyResult: {e}") from e


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


async def update_typology_result(assessment_id: str, update_data: Dict[str, Any]) -> Optional[TypologyResultNode]:
    """
    Updates properties of an existing TypologyResult node.
    'assessment_id' is the unique identifier and cannot be changed.
    """
    if "assessment_id" in update_data:
        del update_data["assessment_id"]

    if not update_data:
        logger.warning(f"Update_typology_result called for '{assessment_id}' with no data to update.")
        return await get_typology_result_by_assessment_id(assessment_id)

    set_clauses = [f"tr.{key} = $props.{key}" for key in update_data.keys()]
    query = f"""
    MATCH (tr:{TYPOLOGYRESULT_LABEL} {{assessment_id: $assessment_id}})
    SET {', '.join(set_clauses)}
    RETURN tr
    """
    parameters = {"assessment_id": assessment_id, "props": update_data}

    try:
        result_node_props = await execute_write_query(query, parameters)
        if result_node_props and result_node_props.get("tr"):
            return TypologyResultNode(**result_node_props["tr"])
        else:
            existing_result = await get_typology_result_by_assessment_id(assessment_id)
            if not existing_result:
                raise NodeNotFoundError(f"TypologyResult with assessment_id '{assessment_id}' not found for update.")
            raise UpdateError(f"Failed to update or retrieve updated TypologyResult node for assessment_id: {assessment_id}")
    except DAOException as e:
        logger.error(f"DAOException during TypologyResult update for assessment_id {assessment_id}: {e}", exc_info=True)
        raise UpdateError(f"Database error updating TypologyResult {assessment_id}: {e}") from e


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

            # Create
            logger.info(f"Attempting to create TypologyResult: {test_assessment_id}")
            created_result = await create_typology_result(result_details)
            assert created_result is not None and created_result.assessment_id == test_assessment_id
            logger.info(f"TypologyResult created: {created_result.model_dump_json(indent=2)}")

            # Get
            retrieved_result = await get_typology_result_by_assessment_id(test_assessment_id)
            assert retrieved_result is not None and retrieved_result.assessment_id == test_assessment_id
            logger.info(f"TypologyResult retrieved: {retrieved_result.model_dump_json(indent=2)}")

            # Get by typology name
            typed_results = await get_typology_results_by_name("TestTypology")
            assert any(r.assessment_id == test_assessment_id for r in typed_results)
            logger.info(f"Found {len(typed_results)} results for typology 'TestTypology'.")

            # Update
            update_payload = {"score": "Result Y", "confidence": 0.95, "details": {"notes": "Updated test assessment"}}
            updated_result = await update_typology_result(test_assessment_id, update_payload)
            assert updated_result is not None and updated_result.score == "Result Y"
            logger.info(f"TypologyResult updated: {updated_result.model_dump_json(indent=2)}")

            # Test duplicate creation
            logger.info(f"Attempting to create duplicate TypologyResult (should fail): {test_assessment_id}")
            try:
                await create_typology_result(result_details)
            except NodeCreationError as e:
                logger.info(f"Successfully caught error for duplicate TypologyResult creation: {e}")
            else:
                logger.error("Duplicate TypologyResult creation did not fail as expected!")

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