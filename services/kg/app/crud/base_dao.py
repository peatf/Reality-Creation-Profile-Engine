import logging
from typing import Any, Dict, List, Optional, Callable, Awaitable

from neo4j import AsyncSession, AsyncTransaction # Removed Neo4jError from here
from neo4j.exceptions import ServiceUnavailable, TransientError, ConstraintError, Neo4jError # Added Neo4jError here

from ..core.db import Neo4jDatabase, get_async_db_session # Use relative import

logger = logging.getLogger(__name__)

# --- Custom Exceptions for DAO Layer ---
class DAOException(Exception):
    """Base exception for DAO operations."""
    pass

class NodeCreationError(DAOException):
    """Error during node creation."""
    pass

class NodeNotFoundError(DAOException):
    """Node not found."""
    pass

class RelationshipCreationError(DAOException):
    """Error during relationship creation."""
    pass

class UpdateError(DAOException):
    """Error during update operation."""
    pass

class DeletionError(DAOException):
    """Error during deletion operation."""
    pass

class UniqueConstraintViolationError(DAOException):
    """Raised when a unique constraint is violated."""
    def __init__(self, message="Unique constraint violated.", original_exception=None):
        super().__init__(message)
        self.original_exception = original_exception


async def execute_read_query(query: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Executes a read-only Cypher query.
    """
    driver = await Neo4jDatabase.get_async_driver()
    async with await get_async_db_session(driver) as session:
        try:
            async def _query_tx(tx: AsyncTransaction):
                result_cursor = await tx.run(query, parameters)
                return await result_cursor.data()

            results = await session.execute_read(_query_tx)
            return results
        except (ServiceUnavailable, TransientError) as e:
            logger.error(f"Neo4j transient error during read query: {query} | params: {parameters} | error: {e}", exc_info=True)
            raise DAOException(f"Database service unavailable or transient error: {e}") from e
        except Neo4jError as e:
            logger.error(f"Neo4j error during read query: {query} | params: {parameters} | error: {e}", exc_info=True)
            raise DAOException(f"A Neo4j error occurred: {e}") from e
        except Exception as e:
            logger.error(f"Unexpected error during read query: {query} | params: {parameters} | error: {e}", exc_info=True)
            raise DAOException(f"An unexpected error occurred: {e}") from e

async def execute_write_query(query: str, parameters: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    """
    Executes a write Cypher query within a transaction.
    Returns the summary of the transaction or the first record if available.
    """
    driver = await Neo4jDatabase.get_async_driver()
    async with await get_async_db_session(driver) as session:
        try:
            # Using execute_write to handle transactions automatically
            async def _query_tx(tx: AsyncTransaction):
                result_cursor = await tx.run(query, parameters)
                # For write queries, often we want to ensure it ran, .single() is good if one record is expected
                # or .consume() if we only need the summary (e.g., for CREATE, DELETE without RETURN)
                # If the schema setup queries are expected to return nothing, .consume() might be more appropriate
                # or checking result_cursor.summary()
                # For now, sticking to .single() as per original intent for some write ops
                return await result_cursor.single()

            result = await session.execute_write(_query_tx)
            # result here is a Record object or None if the query yields no records or only summary
            return dict(result) if result else None
        except ConstraintError as e:
            logger.warning(f"Neo4j constraint violation: {query} | params: {parameters} | error: {e}")
            if "already exists with label" in str(e) or "already exists with node" in str(e): # More specific checks can be added
                raise UniqueConstraintViolationError(original_exception=e) from e
            raise DAOException(f"Constraint violation: {e}") from e
        except (ServiceUnavailable, TransientError) as e:
            logger.error(f"Neo4j transient error during write query: {query} | params: {parameters} | error: {e}", exc_info=True)
            raise DAOException(f"Database service unavailable or transient error: {e}") from e
        except Neo4jError as e:
            logger.error(f"Neo4j error during write query: {query} | params: {parameters} | error: {e}", exc_info=True)
            raise DAOException(f"A Neo4j error occurred: {e}") from e
        except Exception as e:
            logger.error(f"Unexpected error during write query: {query} | params: {parameters} | error: {e}", exc_info=True)
            raise DAOException(f"An unexpected error occurred: {e}") from e


async def execute_merge_query(
    query: str,
    parameters: Optional[Dict[str, Any]] = None,
    *,
    fetch_one: bool = True # If true, expects and returns a single record, else returns None (summary only)
) -> Optional[Dict[str, Any]]:
    """
    Executes a MERGE Cypher query within a transaction, designed for idempotent operations.
    Returns the first record if fetch_one is True and a record is returned by the query, otherwise None.
    """
    driver = await Neo4jDatabase.get_async_driver()
    async with await get_async_db_session(driver) as session:
        try:
            async def _query_tx(tx: AsyncTransaction):
                result_cursor = await tx.run(query, parameters)
                if fetch_one:
                    return await result_cursor.single()
                else:
                    await result_cursor.consume() # Consume if we only care about the summary/success
                    return None

            record = await session.execute_write(_query_tx)
            return dict(record) if record else None
        except ConstraintError as e: # Should be less common with MERGE if used correctly, but possible
            logger.warning(f"Neo4j constraint violation during MERGE: {query} | params: {parameters} | error: {e}")
            if "already exists with label" in str(e) or "already exists with node" in str(e):
                raise UniqueConstraintViolationError(original_exception=e) from e
            raise DAOException(f"Constraint violation: {e}") from e
        except (ServiceUnavailable, TransientError) as e:
            logger.error(f"Neo4j transient error during MERGE: {query} | params: {parameters} | error: {e}", exc_info=True)
            raise DAOException(f"Database service unavailable or transient error: {e}") from e
        except Neo4jError as e:
            logger.error(f"Neo4j error during MERGE: {query} | params: {parameters} | error: {e}", exc_info=True)
            raise DAOException(f"A Neo4j error occurred: {e}") from e
        except Exception as e:
            logger.error(f"Unexpected error during MERGE: {query} | params: {parameters} | error: {e}", exc_info=True)
            raise DAOException(f"An unexpected error occurred: {e}") from e


# Example of a more complex transaction function if needed
async def execute_transaction_fn(
    transaction_work: Callable[[AsyncTransaction], Awaitable[Any]],
    is_write: bool = True
) -> Any:
    """
    Executes a custom function within a Neo4j transaction.

    Args:
        transaction_work: An async function that takes an AsyncTransaction and performs operations.
        is_write: True if the transaction involves write operations, False for read-only.

    Returns:
        The result of the transaction_work function.
    """
    driver = await Neo4jDatabase.get_async_driver()
    async with await get_async_db_session(driver) as session:
        try:
            if is_write:
                result = await session.execute_write(transaction_work)
            else:
                result = await session.execute_read(transaction_work)
            return result
        except ConstraintError as e:
            logger.warning(f"Neo4j constraint violation during transaction_fn | error: {e}")
            if "already exists with label" in str(e) or "already exists with node" in str(e):
                raise UniqueConstraintViolationError(original_exception=e) from e
            raise DAOException(f"Constraint violation: {e}") from e
        except (ServiceUnavailable, TransientError) as e:
            logger.error(f"Neo4j transient error during transaction_fn | error: {e}", exc_info=True)
            raise DAOException(f"Database service unavailable or transient error: {e}") from e
        except Neo4jError as e:
            logger.error(f"Neo4j error during transaction_fn | error: {e}", exc_info=True)
            raise DAOException(f"A Neo4j error occurred: {e}") from e
        except Exception as e:
            logger.error(f"Unexpected error during transaction_fn | error: {e}", exc_info=True)
            raise DAOException(f"An unexpected error occurred: {e}") from e

if __name__ == "__main__":
    # This section is for illustrative purposes and basic testing of this module.
    # It requires a running Neo4j instance configured in .env or via environment variables.
    logging.basicConfig(level=logging.INFO)

    async def test_dao_operations():
        logger.info("Testing DAO operations...")
        try:
            # Test read query
            logger.info("Testing read query: RETURN 1 AS number")
            read_result = await execute_read_query("RETURN 1 AS number")
            if read_result and read_result[0].get("number") == 1:
                logger.info(f"Read query successful: {read_result}")
            else:
                logger.error(f"Read query failed or returned unexpected data: {read_result}")

            # Test write query (simple node creation and deletion for test)
            test_node_id = "test_dao_node_123"
            logger.info(f"Testing write query: CREATE (n:TestNode {{id: $id, name: 'Test DAO Node'}}) RETURN n.id AS id")
            # Create
            create_result = await execute_write_query(
                "CREATE (n:TestNode {id: $id, name: 'Test DAO Node'}) RETURN n.id AS created_id, n.name AS name",
                {"id": test_node_id}
            )
            if create_result and create_result.get("created_id") == test_node_id:
                logger.info(f"Write query (CREATE) successful: {create_result}")
            else:
                logger.error(f"Write query (CREATE) failed or returned unexpected data: {create_result}")

            # Test constraint violation (if a unique constraint exists on TestNode.id)
            # This requires setting up a constraint: CREATE CONSTRAINT ON (n:TestNode) ASSERT n.id IS UNIQUE
            # For now, we'll just log that this part would test it.
            logger.info("Skipping unique constraint violation test as it requires schema setup.")
            # try:
            #     await execute_write_query(
            #         "CREATE (n:TestNode {id: $id, name: 'Duplicate Test Node'}) RETURN n.id AS id",
            #         {"id": test_node_id} # Attempt to create with same ID
            #     )
            # except UniqueConstraintViolationError as ucve:
            #     logger.info(f"Successfully caught UniqueConstraintViolationError: {ucve}")
            # except Exception as e:
            #     logger.error(f"Unexpected error during constraint violation test: {e}")


            # Clean up test node
            logger.info(f"Cleaning up: MATCH (n:TestNode {{id: $id}}) DELETE n")
            await execute_write_query("MATCH (n:TestNode {id: $id}) DELETE n", {"id": test_node_id})
            logger.info("Test node cleanup successful.")

        except DAOException as e:
            logger.error(f"A DAO Exception occurred during testing: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"An unexpected error occurred during testing: {e}", exc_info=True)
        finally:
            await Neo4jDatabase.close_async_driver() # Ensure driver is closed after test

    import asyncio
    asyncio.run(test_dao_operations())