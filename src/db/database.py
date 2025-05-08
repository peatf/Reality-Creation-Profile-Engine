import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import declarative_base

# Read DATABASE_URL from environment variable
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./default.db") # Default to a local SQLite DB if not set

# Configure the database engine
# echo=True will log all SQL statements issued to the database
engine = create_engine(DATABASE_URL, echo=True)

# Create a configured "Session" class
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

# Create a Base class for declarative class definitions
Base = declarative_base()

# Base class is defined above
# The get_db dependency function is now defined in main.py