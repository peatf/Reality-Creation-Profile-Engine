import asyncio
import logging
import signal
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, status
from fastapi.responses import JSONResponse

# Setup logging first
from app.core.logging_config import setup_logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
setup_logging(LOG_LEVEL)

from app.core.db import Neo4jDatabase
from app.graph_schema.schema_setup import apply_schema
from app.consumers.chart_consumer import ChartCalculatedConsumer
from app.consumers.typology_consumer import TypologyAssessedConsumer
from app.core.health_checks import check_neo4j_connection, check_kafka_connection # To be created

logger = logging.getLogger(__name__)

# Global state to hold consumers and tasks
consumer_state = {
    "chart_consumer": None,
    "typology_consumer": None,
    "chart_task": None,
    "typology_task": None,
}

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup logic
    logger.info("KG Service starting up...")

    # Apply Neo4j schema
    try:
        await apply_schema()
    except Exception as e:
        logger.error(f"Failed to apply Neo4j schema on startup: {e}", exc_info=True)
        # Decide if this is fatal. For now, we continue.

    # Initialize and start consumers
    consumer_state["chart_consumer"] = ChartCalculatedConsumer()
    consumer_state["typology_consumer"] = TypologyAssessedConsumer()

    logger.info("Starting Kafka consumers...")
    consumer_state["chart_task"] = asyncio.create_task(consumer_state["chart_consumer"].consume())
    consumer_state["typology_task"] = asyncio.create_task(consumer_state["typology_consumer"].consume())

    yield # Service runs here

    # Shutdown logic
    logger.info("KG Service shutting down...")
    
    # Stop consumers
    if consumer_state["chart_consumer"]:
        logger.info("Stopping chart consumer...")
        await consumer_state["chart_consumer"].stop()
    if consumer_state["typology_consumer"]:
        logger.info("Stopping typology consumer...")
        await consumer_state["typology_consumer"].stop()

    # Wait for consumer tasks to finish
    tasks = [t for t in [consumer_state["chart_task"], consumer_state["typology_task"]] if t]
    if tasks:
        try:
            await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=10.0)
            logger.info("Consumer tasks finished.")
        except asyncio.TimeoutError:
            logger.warning("Consumer tasks did not finish within timeout during shutdown.")

    # Close Neo4j driver
    logger.info("Closing Neo4j driver...")
    await Neo4jDatabase.close_async_driver()

    logger.info("KG Service stopped gracefully.")

# Create FastAPI app instance with lifespan manager
app = FastAPI(title="Knowledge Graph Service", lifespan=lifespan)

@app.get("/health", tags=["Health"])
async def health_check():
    """
    Performs health checks on the service and its dependencies (Neo4j, Kafka).
    Returns HTTP 200 if healthy, HTTP 503 if unhealthy.
    """
    neo4j_healthy = False
    kafka_healthy = False
    errors = []

    try:
        neo4j_healthy = await check_neo4j_connection()
    except Exception as e:
        logger.error(f"Health check: Neo4j connection failed: {e}", exc_info=True)
        errors.append(f"Neo4j connection error: {e}")

    try:
        kafka_healthy = await check_kafka_connection()
    except Exception as e:
        logger.error(f"Health check: Kafka connection failed: {e}", exc_info=True)
        errors.append(f"Kafka connection error: {e}")

    if neo4j_healthy and kafka_healthy:
        return {"status": "ok", "dependencies": {"neo4j": "ok", "kafka": "ok"}}
    else:
        status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        details = {
            "status": "unhealthy",
            "dependencies": {
                "neo4j": "ok" if neo4j_healthy else "unhealthy",
                "kafka": "ok" if kafka_healthy else "unhealthy",
            },
            "errors": errors
        }
        return JSONResponse(content=details, status_code=status_code)

# Remove old __main__ block if it exists
# The application will be run using Uvicorn, e.g., `uvicorn main:app --reload`