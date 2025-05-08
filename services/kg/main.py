import asyncio
import logging
import signal
import os

# Setup logging first
from app.core.logging_config import setup_logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
setup_logging(LOG_LEVEL)

from app.core.db import Neo4jDatabase
from app.graph_schema.schema_setup import apply_schema
from app.consumers.chart_consumer import ChartCalculatedConsumer
from app.consumers.typology_consumer import TypologyAssessedConsumer

logger = logging.getLogger(__name__)

# List to keep track of running consumer tasks
consumer_tasks = []
stop_event = asyncio.Event()

def handle_signal(sig, frame):
    logger.info(f"Received signal {sig}, initiating graceful shutdown...")
    stop_event.set()

async def run_service():
    """Initializes and runs the KG service components."""
    logger.info("Starting KG Service...")

    # Apply Neo4j schema (constraints/indexes) on startup
    try:
        await apply_schema()
    except Exception as e:
        logger.error(f"Failed to apply Neo4j schema on startup: {e}", exc_info=True)
        # Depending on policy, might want to exit if schema setup fails
        # raise SystemExit("Neo4j schema setup failed.") from e

    # Initialize consumers
    chart_consumer = ChartCalculatedConsumer()
    typology_consumer = TypologyAssessedConsumer()

    # Start consumers in background tasks
    logger.info("Starting Kafka consumers...")
    chart_task = asyncio.create_task(chart_consumer.consume())
    typology_task = asyncio.create_task(typology_consumer.consume())
    consumer_tasks.extend([chart_task, typology_task])

    # Keep running until stop signal is received
    await stop_event.wait()

    # Initiate shutdown
    logger.info("Stopping Kafka consumers...")
    await chart_consumer.stop()
    await typology_consumer.stop()

    # Wait for tasks to finish
    try:
        await asyncio.wait_for(asyncio.gather(*consumer_tasks, return_exceptions=True), timeout=10.0)
        logger.info("Consumer tasks finished.")
    except asyncio.TimeoutError:
        logger.warning("Consumer tasks did not finish within timeout during shutdown.")

    # Close Neo4j driver
    logger.info("Closing Neo4j driver...")
    await Neo4jDatabase.close_async_driver()

    logger.info("KG Service stopped gracefully.")


if __name__ == "__main__":
    # Setup signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        asyncio.run(run_service())
    except Exception as e:
        logger.critical(f"KG Service encountered critical error: {e}", exc_info=True)
        # Perform any final cleanup if possible
        asyncio.run(Neo4jDatabase.close_async_driver()) # Attempt to close driver on critical error too
        raise SystemExit(1) from e