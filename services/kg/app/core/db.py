import logging
from neo4j import GraphDatabase, AsyncGraphDatabase, Neo4jDriver, AsyncNeo4jDriver
from typing import Optional

from .config import neo4j_settings # Import the renamed settings object

logger = logging.getLogger(__name__)

class Neo4jDatabase:
    _driver: Optional[Neo4jDriver] = None
    _async_driver: Optional[AsyncNeo4jDriver] = None

    @classmethod
    def get_driver(cls) -> Neo4jDriver:
        if cls._driver is None:
            try:
                cls._driver = GraphDatabase.driver(
                    neo4j_settings.uri,
                    auth=(neo4j_settings.user, neo4j_settings.password),
                    # Configure connection pool options if needed, e.g.:
                    # max_connection_lifetime=3600,
                    # max_connection_pool_size=50,
                    # connection_acquisition_timeout=60
                )
                # Verify connectivity
                cls._driver.verify_connectivity()
                logger.info(f"Successfully connected to Neo4j (sync driver) at {neo4j_settings.uri} for database '{neo4j_settings.database}'")
            except Exception as e:
                logger.error(f"Failed to connect to Neo4j (sync driver): {e}", exc_info=True)
                # Depending on application requirements, you might want to raise the exception
                # or handle it by returning None or a dummy driver.
                # For now, let it raise to make connection issues apparent.
                raise
        return cls._driver

    @classmethod
    async def get_async_driver(cls) -> AsyncNeo4jDriver:
        if cls._async_driver is None:
            try:
                cls._async_driver = AsyncGraphDatabase.driver(
                    neo4j_settings.uri,
                    auth=(neo4j_settings.user, neo4j_settings.password)
                    # Async driver might have different or fewer pool options directly in constructor
                )
                # Verify connectivity for async driver
                await cls._async_driver.verify_connectivity()
                logger.info(f"Successfully connected to Neo4j (async driver) at {neo4j_settings.uri} for database '{neo4j_settings.database}'")
            except Exception as e:
                logger.error(f"Failed to connect to Neo4j (async driver): {e}", exc_info=True)
                raise
        return cls._async_driver

    @classmethod
    def close_driver(cls):
        if cls._driver:
            cls._driver.close()
            cls._driver = None
            logger.info("Neo4j synchronous driver closed.")

    @classmethod
    async def close_async_driver(cls):
        if cls._async_driver:
            await cls._async_driver.close()
            cls._async_driver = None
            logger.info("Neo4j asynchronous driver closed.")

# Helper functions to get a session, managing database selection
def get_db_session(driver: Neo4jDriver):
    return driver.session(database=neo4j_settings.database)

async def get_async_db_session(async_driver: AsyncNeo4jDriver):
    return async_driver.session(database=neo4j_settings.database)


# Example usage and connectivity test
async def check_neo4j_connection():
    """Checks both sync and async Neo4j driver connectivity."""
    sync_connected = False
    async_connected = False
    try:
        sync_driver = Neo4jDatabase.get_driver()
        with get_db_session(sync_driver) as session:
            result = session.run("RETURN 1 AS one")
            record = result.single()
            if record and record["one"] == 1:
                logger.info("Sync Neo4j connection test successful: RETURN 1 worked.")
                sync_connected = True
            else:
                logger.error("Sync Neo4j connection test failed: RETURN 1 did not yield expected result.")
    except Exception as e:
        logger.error(f"Sync Neo4j connection test failed: {e}", exc_info=True)
    finally:
        Neo4jDatabase.close_driver()

    try:
        async_driver = await Neo4jDatabase.get_async_driver()
        async with await get_async_db_session(async_driver) as session:
            result = await session.run("RETURN 1 AS one")
            record = await result.single()
            if record and record["one"] == 1:
                logger.info("Async Neo4j connection test successful: RETURN 1 worked.")
                async_connected = True
            else:
                logger.error("Async Neo4j connection test failed: RETURN 1 did not yield expected result.")
    except Exception as e:
        logger.error(f"Async Neo4j connection test failed: {e}", exc_info=True)
    finally:
        await Neo4jDatabase.close_async_driver()
    
    return sync_connected, async_connected

if __name__ == "__main__":
    import asyncio
    logging.basicConfig(level=logging.INFO)
    logger.info("Running Neo4j connection check...")
    
    # This is just for standalone testing of this module.
    # In a real app, driver initialization and closing would be tied to app lifecycle.
    sync_ok, async_ok = asyncio.run(check_neo4j_connection())
    
    if sync_ok:
        print("Synchronous Neo4j driver connected and tested successfully.")
    else:
        print("Synchronous Neo4j driver connection FAILED.")
        
    if async_ok:
        print("Asynchronous Neo4j driver connected and tested successfully.")
    else:
        print("Asynchronous Neo4j driver connection FAILED.")