import asyncio
import logging
from aiokafka import AIOKafkaClient
from aiokafka.errors import KafkaConnectionError
from neo4j.exceptions import ServiceUnavailable, AuthError

from .db import Neo4jDatabase
from .config import kafka_settings

logger = logging.getLogger(__name__)

NEO4J_HEALTH_CHECK_TIMEOUT = 5  # seconds
KAFKA_HEALTH_CHECK_TIMEOUT = 5  # seconds

async def check_neo4j_connection() -> bool:
    """
    Checks the connectivity to the Neo4j database by running a simple query.
    """
    logger.debug("Checking Neo4j connection...")
    try:
        driver = Neo4jDatabase.get_async_driver()
        # verify_connectivity attempts to connect and potentially authenticate
        await asyncio.wait_for(
            driver.verify_connectivity(),
            timeout=NEO4J_HEALTH_CHECK_TIMEOUT
        )
        # Optionally run a simple query
        async with driver.session() as session:
             await asyncio.wait_for(
                 session.run("RETURN 1"),
                 timeout=NEO4J_HEALTH_CHECK_TIMEOUT
             )
        logger.debug("Neo4j connection check successful.")
        return True
    except (ServiceUnavailable, AuthError, asyncio.TimeoutError) as e:
        logger.error(f"Neo4j connection check failed: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during Neo4j health check: {e}", exc_info=True)
        return False

async def check_kafka_connection() -> bool:
    """
    Checks the connectivity to Kafka brokers by attempting to list topics or get metadata.
    """
    logger.debug("Checking Kafka connection...")
    client = None
    try:
        # Use AIOKafkaClient for a lightweight connection check
        client = AIOKafkaClient(
            bootstrap_servers=kafka_settings.bootstrap_servers,
            request_timeout_ms=(KAFKA_HEALTH_CHECK_TIMEOUT * 1000) # Convert seconds to ms
        )
        await asyncio.wait_for(
            client.bootstrap(),
            timeout=KAFKA_HEALTH_CHECK_TIMEOUT
        )
        # Optionally, try fetching metadata (more comprehensive check)
        # await asyncio.wait_for(
        #     client.cluster.request_update(),
        #     timeout=KAFKA_HEALTH_CHECK_TIMEOUT
        # )
        # metadata = client.cluster.brokers()
        # if not metadata:
        #     raise KafkaConnectionError("No broker metadata found.")

        logger.debug("Kafka connection check successful.")
        return True
    except (KafkaConnectionError, asyncio.TimeoutError) as e:
        logger.error(f"Kafka connection check failed: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during Kafka health check: {e}", exc_info=True)
        return False
    finally:
        if client:
            try:
                await client.close()
            except Exception:
                logger.warning("Error closing AIOKafkaClient during health check cleanup.", exc_info=True)