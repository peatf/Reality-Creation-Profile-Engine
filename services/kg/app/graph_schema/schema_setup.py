import logging
from ..crud.base_dao import execute_write_query, DAOException, UniqueConstraintViolationError

from .constants import (
    PERSON_LABEL, ASTROFEATURE_LABEL, HDFEATURE_LABEL, TYPOLOGYRESULT_LABEL,
    HAS_FEATURE_REL, INFLUENCES_REL, CONFLICTS_WITH_REL, HAS_TYPOLOGY_RESULT_REL
)

logger = logging.getLogger(__name__)

# --- Node Labels --- REMOVED
# --- Relationship Types --- REMOVED

# Schema definition: Constraints and Indexes
# Constraints imply indexes, so a separate index on the same property is not needed.

CONSTRAINTS = [
    # Person Node Constraints
    f"CREATE CONSTRAINT IF NOT EXISTS FOR (p:{PERSON_LABEL}) REQUIRE p.user_id IS UNIQUE", # Removed name

    # AstroFeature Node Constraints (name should be unique within its feature_type)
    # Neo4j doesn't directly support uniqueness constraints on a combination of properties out-of-the-box for all versions/setups without workarounds.
    # A common approach is to create a composite key or ensure uniqueness at the application layer.
    # For simplicity, we'll create a unique constraint on 'name' if it's globally unique,
    # or an index if uniqueness is per type (application-enforced).
    # Assuming 'name' for AstroFeature should be globally unique for now. If not, this should be an INDEX.
    f"CREATE CONSTRAINT IF NOT EXISTS FOR (af:{ASTROFEATURE_LABEL}) REQUIRE af.name IS UNIQUE", # Removed name

    # HDFeature Node Constraints (similar to AstroFeature)
    f"CREATE CONSTRAINT IF NOT EXISTS FOR (hf:{HDFEATURE_LABEL}) REQUIRE hf.name IS UNIQUE", # Removed name

    # TypologyResult Node Constraints
    # An assessment_id should be unique.
    f"CREATE CONSTRAINT IF NOT EXISTS FOR (tr:{TYPOLOGYRESULT_LABEL}) REQUIRE tr.assessment_id IS UNIQUE", # Removed name
]

INDEXES = [
    # Person Node Indexes (user_id is already indexed by unique constraint)
    f"CREATE INDEX IF NOT EXISTS FOR (p:{PERSON_LABEL}) ON (p.profile_id)", # Removed name
    f"CREATE INDEX IF NOT EXISTS FOR (p:{PERSON_LABEL}) ON (p.name)", # Removed name, If searching by name is common

    # AstroFeature Node Indexes (name is indexed by unique constraint if globally unique)
    f"CREATE INDEX IF NOT EXISTS FOR (af:{ASTROFEATURE_LABEL}) ON (af.feature_type)", # Removed name
    # If 'name' is not globally unique, but unique per 'feature_type', then remove constraint and add:
    # f"CREATE INDEX IF NOT EXISTS FOR (af:{ASTROFEATURE_LABEL}) ON (af.name)", # Removed name

    # HDFeature Node Indexes (name is indexed by unique constraint if globally unique)
    f"CREATE INDEX IF NOT EXISTS FOR (hf:{HDFEATURE_LABEL}) ON (hf.feature_type)", # Removed name
    # If 'name' is not globally unique:
    # f"CREATE INDEX IF NOT EXISTS FOR (hf:{HDFEATURE_LABEL}) ON (hf.name)", # Removed name

    # TypologyResult Node Indexes (assessment_id is indexed by unique constraint)
    f"CREATE INDEX IF NOT EXISTS FOR (tr:{TYPOLOGYRESULT_LABEL}) ON (tr.typology_name)", # Removed name
    # Add index on user_id if direct lookups on TypologyResult by user_id are needed,
    # though typically this would be found via Person-(HAS_TYPOLOGY_RESULT)->TypologyResult
    # f"CREATE INDEX IF NOT EXISTS typologyresult_user_id_idx FOR (tr:{TYPOLOGYRESULT_LABEL}) ON (tr.user_id)",
]

async def apply_schema():
    """
    Applies all defined constraints and indexes to the Neo4j database.
    This function should be called once during application startup.
    """
    logger.info("Applying Neo4j schema: Constraints and Indexes...")
    applied_count = 0
    failed_count = 0

    for constraint_query in CONSTRAINTS:
        try:
            logger.info(f"Applying constraint: {constraint_query}")
            await execute_write_query(constraint_query)
            logger.info(f"Successfully applied constraint.")
            applied_count += 1
        except UniqueConstraintViolationError: # Should not happen with "IF NOT EXISTS" but good to be aware
            logger.warning(f"Constraint likely already exists (UniqueConstraintViolationError): {constraint_query}")
            # This is not an error if "IF NOT EXISTS" is used.
            applied_count +=1 # Count as applied if it exists
        except DAOException as e:
            logger.error(f"Failed to apply constraint: {constraint_query}. Error: {e}", exc_info=True)
            failed_count += 1
        except Exception as e: # Catch any other unexpected errors
            logger.error(f"Unexpected error applying constraint: {constraint_query}. Error: {e}", exc_info=True)
            failed_count += 1


    for index_query in INDEXES:
        try:
            logger.info(f"Applying index: {index_query}")
            await execute_write_query(index_query) # Indexes are idempotent with "IF NOT EXISTS"
            logger.info(f"Successfully applied index.")
            applied_count += 1
        except DAOException as e: # execute_write_query wraps Neo4j errors in DAOException
            logger.error(f"Failed to apply index: {index_query}. Error: {e}", exc_info=True)
            failed_count += 1
        except Exception as e:
            logger.error(f"Unexpected error applying index: {index_query}. Error: {e}", exc_info=True)
            failed_count += 1

    if failed_count > 0:
        logger.error(f"Schema application completed with {failed_count} failures and {applied_count} successes/already-exists.")
    else:
        logger.info(f"Neo4j schema application completed successfully ({applied_count} constraints/indexes processed).")

    return applied_count, failed_count

if __name__ == "__main__":
    import asyncio
    # Use relative imports for internal modules when running as part of the package
    from ..core.db import Neo4jDatabase # For closing driver
    from ..core.logging_config import setup_logging # Assuming setup_logging is needed here too

    # logging.basicConfig(level=logging.INFO) # Replaced by setup_logging
    setup_logging("INFO") # Setup structured logging for standalone run
    # This requires Neo4j to be running and configured.
    async def main():
        logger.info("Starting schema setup script...")
        # Ensure .env is loaded if running standalone and it exists
        from ..core.config import neo4j_settings # To print config being used
        logger.info(f"Using Neo4j URI: {neo4j_settings.uri}, User: {neo4j_settings.user}, Database: {neo4j_settings.database}")

        applied, failed = await apply_schema()
        logger.info(f"Schema setup finished. Applied/Existing: {applied}, Failed: {failed}")
        
        # Important: Close the driver when the application or script finishes.
        # In a real app, this would be part of the app's lifecycle management.
        await Neo4jDatabase.close_async_driver()
        logger.info("Neo4j async driver closed after schema setup.")

    asyncio.run(main())