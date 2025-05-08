import os
import sys
from logging.config import fileConfig
import asyncio # Added for async online migrations

from dotenv import load_dotenv # Added to load .env file

load_dotenv() # Load environment variables from .env file
from sqlalchemy import pool
from sqlalchemy.engine import URL # Added for URL construction if needed
from sqlalchemy.ext.asyncio import create_async_engine # Added for async engine

from alembic import context

# Add project root to sys.path for model imports
# Adjust the path depth as necessary based on your project structure
project_root = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)


# Import Base and metadata from your models module
# Ensure src directory is in python path or adjust import accordingly
# --- Fix for PYTHONPATH issue: Add src/ to sys.path ---
from pathlib import Path
# Calculate the path to the 'src' directory relative to this env.py file
# env.py -> alembic/ -> project_root -> src/
src_path = Path(__file__).resolve().parents[1] / "src"
if str(src_path) not in sys.path:
    sys.path.append(str(src_path))
# --- End PYTHONPATH fix ---
try:
    from src.db.models import Base, convention as naming_convention # Import convention too
    target_metadata = Base.metadata
    # Ensure the schema is set if defined in models.py
    # target_metadata.schema = "public" # Already set in models.py via MetaData instance
except ImportError as e:
    print(f"Error importing models: {e}")
    print("Ensure 'src' directory is in PYTHONPATH or adjust import path in alembic/env.py")
    sys.exit(1)


# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Get DATABASE_URL from environment variable
# This URL is primarily for the *async* engine used in online mode.
# Alembic's offline generation often uses a sync driver implicitly.
db_url_env = os.getenv("DATABASE_URL")
if not db_url_env:
    raise ValueError("DATABASE_URL environment variable not set.")

# Set the sqlalchemy.url in the config object specifically for Alembic's internal use,
# potentially overriding the .ini file's setting if needed, especially for offline mode.
# We ensure it uses a synchronous driver like psycopg2 for compatibility with
# some Alembic operations that might not be fully async-aware.
sync_db_url = db_url_env.replace("postgresql+asyncpg", "postgresql+psycopg2")
config.set_main_option("sqlalchemy.url", sync_db_url)


# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    context.configure(
        url=config.get_main_option("sqlalchemy.url"), # Use the sync URL set above
        target_metadata=target_metadata,
        literal_binds=True, # Good for offline SQL generation
        dialect_opts={"paramstyle": "named"},
        compare_type=True, # Enable type comparison
        compare_server_default=True, # Enable server default comparison
        include_schemas=True, # Include schema name in diffs (since we use public schema explicitly)
        render_as_batch=True, # Recommended for SQLite, generally safe for PG too
        naming_convention=naming_convention, # Apply naming convention from models.py
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection):
    """Helper function to configure and run migrations within a transaction."""
    # Pass the naming convention here as well for online mode
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
        compare_server_default=True,
        include_schemas=True, # Ensure schema awareness online
        render_as_batch=True, # Use batch mode online too
        naming_convention=naming_convention, # Apply naming convention
    )

    # Transaction is handled by the caller (run_migrations_online)
    # context.begin_transaction() is called before invoking this in run_sync
    context.run_migrations()


async def run_migrations_online() -> None:
    """Run migrations in 'online' mode using an async engine."""

    # Create an async engine using the DATABASE_URL from env
    # Ensure the URL uses the asyncpg driver
    async_db_url = os.getenv("DATABASE_URL") # Re-fetch to ensure it's the original async one
    if not async_db_url:
        raise ValueError("DATABASE_URL environment variable not set for async engine.")
    if not async_db_url.startswith("postgresql+asyncpg"):
         # Attempt to fix if it's missing the asyncpg part
         if async_db_url.startswith("postgresql"):
             async_db_url = async_db_url.replace("postgresql", "postgresql+asyncpg", 1)
         else:
             raise ValueError(f"DATABASE_URL does not seem to be a valid PostgreSQL URL for asyncpg: {async_db_url}")


    connectable = create_async_engine(
        async_db_url,
        poolclass=pool.NullPool, # Use NullPool for migrations to avoid pool issues
    )

    async with connectable.connect() as connection:
        # Start a transaction explicitly and pass it to run_sync
        await connection.begin()
        await connection.run_sync(do_run_migrations)
        await connection.commit() # Commit the transaction

    # Dispose the engine after use
    await connectable.dispose()


if context.is_offline_mode():
    print("Running migrations offline...")
    run_migrations_offline()
else:
    print("Running migrations online...")
    # Use asyncio.run to execute the async online migration function
    try:
        asyncio.run(run_migrations_online())
    except Exception as e:
        print(f"Error during online migration: {e}")
        sys.exit(1)
