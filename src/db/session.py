import os
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import Engine

# Recommended to load from environment variables or a config file
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://user:password@host:port/dbname") # Provide a default or raise error if not set

def get_engine(db_url: str = DATABASE_URL) -> Engine:
    """
    Creates a synchronous SQLAlchemy engine instance.
    Note: For async operations, use get_async_engine.
    This function might be useful for Alembic or synchronous scripts.
    """
    # Using synchronous engine creation for potential sync tasks (like Alembic offline)
    # If only async is ever needed, this function could be removed or adapted.
    from sqlalchemy import create_engine
    return create_engine(
        db_url.replace("+asyncpg", "+psycopg"), # Alembic often uses psycopg
        pool_size=10,
        max_overflow=20,
        pool_pre_ping=True,
        pool_recycle=1800, # 30 minutes
    )

def get_async_engine(db_url: str = DATABASE_URL) -> Engine:
    """Creates an asynchronous SQLAlchemy engine instance."""
    return create_async_engine(
        db_url,
        pool_size=10,
        max_overflow=20,
        pool_pre_ping=True,
        pool_recycle=1800, # 30 minutes
        echo=False, # Set to True for debugging SQL
    )

def get_session_factory(engine: Engine) -> sessionmaker:
    """Creates an asynchronous session factory."""
    return async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False, # Important for async usage, especially with FastAPI
    )

# Create engine and session factory instances
async_engine = get_async_engine()
SessionFactory = get_session_factory(async_engine)

async def db_session() -> AsyncGenerator[AsyncSession, None]:
    """
    FastAPI dependency to provide a database session.

    Handles session creation, commit, rollback, and closing.
    """
    async with SessionFactory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            # The context manager (`async with SessionFactory()`) handles closing
            pass

# Example usage (outside FastAPI context):
# async def main():
#     async with SessionFactory() as session:
#         # Perform database operations
#         result = await session.execute(select(User).where(User.email == "test@example.com"))
#         user = result.scalars().first()
#         print(user)

# if __name__ == "__main__":
#     import asyncio
#     # Make sure to define User model or import it
#     # from .models import User
#     # from sqlalchemy import select
#     # asyncio.run(main())