import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    MetaData,
    create_engine,
    Column,
    String,
    DateTime,
    ForeignKey,
    UniqueConstraint,
    Index,
    Numeric,
    BigInteger,
    func,
    PrimaryKeyConstraint, # Added missing import
    JSON, # Import generic JSON type
)
from sqlalchemy.dialects.postgresql import UUID # Keep UUID for PostgreSQL
# Remove JSONB import: from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.dialects.postgresql.types import TIMESTAMP # Correct import for TIMESTAMP(timezone=True)
from sqlalchemy.orm import declarative_base, relationship

# Define naming conventions for constraints and indexes
# https://alembic.sqlalchemy.org/en/latest/naming.html
convention = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

metadata = MetaData(naming_convention=convention)
Base = declarative_base(metadata=metadata)


class User(Base):
    __tablename__ = "users"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email = Column(String(255), unique=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    tier = Column(String(20), nullable=False, default="free")
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )

    profiles = relationship("Profile", back_populates="user", cascade="all, delete-orphan")
    typology_answers = relationship("TypologyAnswer", back_populates="user", cascade="all, delete-orphan")
    log_entries = relationship("LogEntry", back_populates="user", cascade="all, delete-orphan")


class Profile(Base):
    __tablename__ = "profiles"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    birth_datetime = Column(TIMESTAMP(timezone=True), nullable=False)
    birth_lat = Column(Numeric(9, 6), nullable=False)
    birth_lon = Column(Numeric(9, 6), nullable=False)
    birth_tz = Column(String(64), nullable=False)
    hd_data = Column(JSON, nullable=False) # Use generic JSON
    astro_data = Column(JSON, nullable=False) # Use generic JSON
    synthesis_results = Column(JSON, nullable=True) # Use generic JSON
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())
    updated_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now())

    user = relationship("User", back_populates="profiles")
    typology_result = relationship("TypologyResult", back_populates="profile", uselist=False, cascade="all, delete-orphan")


class TypologyAnswer(Base):
    __tablename__ = "typology_answers"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    question_id = Column(String(64), nullable=False)
    answer_value = Column(String(64), nullable=False)
    answered_at = Column(TIMESTAMP(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))

    user = relationship("User", back_populates="typology_answers")

    __table_args__ = (UniqueConstraint("user_id", "question_id", name="uq_typology_answers_user_question"),)


class TypologyResult(Base):
    __tablename__ = "typology_results"

    # 1-1 relationship with profiles using profile.id as PK and FK
    id = Column(UUID(as_uuid=True), ForeignKey("profiles.id", ondelete="CASCADE"), primary_key=True)
    typology_name = Column(String(64), nullable=False)
    confidence = Column(Numeric(3, 2), nullable=False)
    raw_vector = Column(JSON, nullable=False) # Use generic JSON
    generated_at = Column(TIMESTAMP(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
 
    profile = relationship("Profile", back_populates="typology_result")
 
    def __init__(self, *args, profile_id: uuid.UUID = None, **kwargs):
        if profile_id is not None:
            if 'id' in kwargs and kwargs['id'] != profile_id:
                # Or handle as an error, e.g., raise ValueError
                # For now, let 'id' in kwargs take precedence if both are somehow passed and differ.
                # However, the typical use case for adding 'profile_id' is when 'id' isn't directly passed.
                pass # Allow explicit 'id' to override 'profile_id' if both are passed.
            elif 'id' not in kwargs: # Only set 'id' from 'profile_id' if 'id' isn't already specified.
                kwargs['id'] = profile_id
        super().__init__(*args, **kwargs)


class LogEntry(Base):
    __tablename__ = "log_entries"

    # TimescaleDB hypertable - primary key needs to include the time column
    id = Column(BigInteger, nullable=False) # Changed: Not primary key on its own
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    correlation_id = Column(UUID(as_uuid=True), nullable=True)
    event_type = Column(String(64), nullable=False)
    service_origin = Column(String(64), nullable=True)
    payload = Column(JSON, nullable=False) # Use generic JSON
    log_timestamp = Column(TIMESTAMP(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc)) # Part of composite PK

    user = relationship("User", back_populates="log_entries")

    __table_args__ = (
        PrimaryKeyConstraint('id', 'log_timestamp', name='pk_log_entries'), # Added composite primary key
        Index("ix_log_entries_user_id_log_timestamp_desc", "user_id", log_timestamp.desc()),
        Index("ix_log_entries_event_type_log_timestamp_desc", "event_type", log_timestamp.desc()),
        # Note: Hypertable creation and chunking is done via raw SQL in Alembic migration
    )

# Example usage (optional, for testing or direct script execution)
# if __name__ == "__main__":
#     # Replace with your actual database URL
#     DATABASE_URL = "postgresql+psycopg://user:password@host:port/dbname"
#     engine = create_engine(DATABASE_URL)

#     # Create tables (for development/testing only, use Alembic for production)
#     # Base.metadata.create_all(engine)

#     print("SQLAlchemy models defined.")
#     # You can add code here to test model interactions if needed