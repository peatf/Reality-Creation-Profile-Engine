import asyncio
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import sessionmaker

# Assuming models are in src.db.models relative to the project root
# Adjust the import path if your structure differs
from src.db.models import (
    Base,
    LogEntry,
    Profile,
    TypologyAnswer,
    TypologyResult,
    User,
)

# Use in-memory SQLite for testing
# Note: This won't test PG-specific features like JSONB, TIMESTAMPTZ, or TimescaleDB
TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"

# --- Test Fixtures ---

# The local event_loop fixture is removed to avoid conflict with pytest-asyncio.
# pytest-asyncio will provide the event_loop.

@pytest_asyncio.fixture(scope="session")
async def test_engine():
    """Creates an async engine for the test session."""
    engine = create_async_engine(TEST_DATABASE_URL, echo=False)
    yield engine
    await engine.dispose()

@pytest_asyncio.fixture(scope="session")
async def test_session_factory(test_engine):
    """Creates an async session factory for the test session."""
    async with test_engine.begin() as conn:
        # Ensure schema is created in the in-memory DB for the session
        await conn.run_sync(Base.metadata.create_all)

    TestSessionFactory = async_sessionmaker(
        bind=test_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    yield TestSessionFactory
    # Schema teardown isn't strictly necessary for :memory: but good practice
    # async with test_engine.begin() as conn:
    #     await conn.run_sync(Base.metadata.drop_all)


@pytest_asyncio.fixture(scope="function")
async def db_session(test_session_factory: sessionmaker[AsyncSession]):
    """Provides a clean database session for each test function."""
    async with test_session_factory() as session:
        yield session
        # Rollback any changes made during the test to ensure isolation
        # Since it's in-memory, this might not be strictly needed,
        # but good practice if tests modified data.
        await session.rollback()


# --- Model Tests ---

@pytest.mark.asyncio
async def test_user_roundtrip(db_session: AsyncSession):
    """Test creating and retrieving a User."""
    user_email = f"test_user_{uuid.uuid4()}@example.com"
    new_user = User(
        email=user_email,
        password_hash="hashed_password",
        tier="premium",
        # created_at/updated_at are handled by defaults/onupdate in real DB
    )
    db_session.add(new_user)
    await db_session.commit()
    await db_session.refresh(new_user) # Load defaults like ID, created_at

    # Retrieve the user
    stmt = select(User).where(User.email == user_email)
    result = await db_session.execute(stmt)
    retrieved_user = result.scalars().one_or_none()

    assert retrieved_user is not None
    assert retrieved_user.id == new_user.id
    assert retrieved_user.email == user_email
    assert retrieved_user.tier == "premium"
    assert isinstance(retrieved_user.created_at, datetime)
    assert isinstance(retrieved_user.updated_at, datetime)


@pytest.mark.asyncio
async def test_profile_roundtrip(db_session: AsyncSession):
    """Test creating and retrieving a Profile with its User relationship."""
    # 1. Create User
    user_email = f"profile_user_{uuid.uuid4()}@example.com"
    user = User(email=user_email, password_hash="pw")
    db_session.add(user)
    await db_session.commit()
    await db_session.refresh(user)

    # 2. Create Profile
    birth_dt = datetime(2000, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    new_profile = Profile(
        user_id=user.id,
        birth_datetime=birth_dt,
        birth_lat=Decimal("34.0522"),
        birth_lon=Decimal("-118.2437"),
        birth_tz="America/Los_Angeles",
        hd_data={"type": "Generator"},
        astro_data={"sun": "Capricorn"},
        # user=user # Can assign object directly too
    )
    db_session.add(new_profile)
    await db_session.commit()
    await db_session.refresh(new_profile)

    # 3. Retrieve Profile
    stmt = select(Profile).where(Profile.id == new_profile.id)
    result = await db_session.execute(stmt)
    retrieved_profile = result.scalars().one_or_none()

    assert retrieved_profile is not None
    assert retrieved_profile.id == new_profile.id
    assert retrieved_profile.user_id == user.id
    assert retrieved_profile.birth_datetime == birth_dt
    # SQLite might return strings for decimals, adjust assertion if needed
    assert retrieved_profile.birth_lat == Decimal("34.0522")
    assert retrieved_profile.birth_lon == Decimal("-118.2437")
    assert retrieved_profile.hd_data == {"type": "Generator"}

    # Test relationship loading (lazy load by default)
    retrieved_user = await db_session.get(User, user.id) # Re-fetch user in session
    assert retrieved_user is not None
    # Accessing profile.user triggers the load
    # Note: Need to ensure profile is associated with the current session
    profile_in_session = await db_session.get(Profile, new_profile.id)
    assert profile_in_session is not None
    assert profile_in_session.user.id == user.id
    assert profile_in_session.user.email == user_email


@pytest.mark.asyncio
async def test_typology_answer_roundtrip(db_session: AsyncSession):
    """Test creating and retrieving a TypologyAnswer."""
    user = User(email=f"answer_user_{uuid.uuid4()}@example.com", password_hash="pw")
    db_session.add(user)
    await db_session.commit()
    await db_session.refresh(user)

    answered_at_dt = datetime.now(timezone.utc)
    new_answer = TypologyAnswer(
        user_id=user.id,
        question_id="q1",
        answer_value="a1",
        answered_at=answered_at_dt,
    )
    db_session.add(new_answer)
    await db_session.commit()
    await db_session.refresh(new_answer)

    stmt = select(TypologyAnswer).where(TypologyAnswer.id == new_answer.id)
    result = await db_session.execute(stmt)
    retrieved_answer = result.scalars().one()

    assert retrieved_answer.id == new_answer.id
    assert retrieved_answer.user_id == user.id
    assert retrieved_answer.question_id == "q1"
    assert retrieved_answer.answer_value == "a1"
    assert retrieved_answer.answered_at == answered_at_dt


@pytest.mark.asyncio
async def test_typology_result_roundtrip(db_session: AsyncSession):
    """Test creating and retrieving a TypologyResult with Profile relationship."""
    user = User(email=f"result_user_{uuid.uuid4()}@example.com", password_hash="pw")
    profile = Profile(
        user=user, # Assign user object directly
        birth_datetime=datetime.now(timezone.utc),
        birth_lat=Decimal("10.0"), birth_lon=Decimal("10.0"), birth_tz="UTC",
        hd_data={}, astro_data={}
    )
    db_session.add_all([user, profile]) # Add both
    await db_session.commit()
    # Refresh to get IDs assigned by the database
    await db_session.refresh(user)
    await db_session.refresh(profile)


    generated_at_dt = datetime.now(timezone.utc)
    new_result = TypologyResult(
        # The __init__ of TypologyResult takes profile_id and sets the 'id' field.
        profile_id=profile.id,
        typology_name="TestType",
        confidence=Decimal("0.95"),
        raw_vector={"v1": 1.0},
        generated_at=generated_at_dt,
        # profile=profile # Can assign object
    )
    db_session.add(new_result)
    await db_session.commit()
    await db_session.refresh(new_result)

    # Retrieve result
    stmt = select(TypologyResult).where(TypologyResult.id == profile.id) # Query by 'id' column
    result = await db_session.execute(stmt)
    retrieved_result = result.scalars().one()
 
    assert retrieved_result.id == new_result.id # Check assigned UUID PK
    assert retrieved_result.id == profile.id # 'id' is the foreign key to profile.id
    assert retrieved_result.typology_name == "TestType"
    assert retrieved_result.confidence == Decimal("0.95")
    assert retrieved_result.raw_vector == {"v1": 1.0}
    assert retrieved_result.generated_at == generated_at_dt

    # Test relationship loading
    profile_in_session = await db_session.get(Profile, profile.id)
    assert profile_in_session is not None
    assert profile_in_session.typology_result is not None # Ensure relationship loaded
    assert profile_in_session.typology_result.id == retrieved_result.id


@pytest.mark.asyncio
async def test_log_entry_roundtrip(db_session: AsyncSession):
    """Test creating and retrieving a LogEntry."""
    user = User(email=f"log_user_{uuid.uuid4()}@example.com", password_hash="pw")
    db_session.add(user)
    await db_session.commit()
    await db_session.refresh(user)

    log_ts = datetime.now(timezone.utc)
    corr_id = uuid.uuid4()
    new_log = LogEntry(
        user_id=user.id,
        correlation_id=corr_id,
        event_type="TEST_EVENT",
        service_origin="test_service",
        payload={"data": "value"},
        log_timestamp=log_ts,
    )
    db_session.add(new_log)
    await db_session.commit()
    # Refresh to get the auto-incrementing ID (if applicable in SQLite, might be None)
    await db_session.refresh(new_log)

    # Retrieve log entry
    # Note: SQLite doesn't have bigserial, ID might behave differently. Query by corr_id.
    stmt = select(LogEntry).where(LogEntry.correlation_id == corr_id)
    result = await db_session.execute(stmt)
    retrieved_log = result.scalars().one()

    assert retrieved_log.id is not None # Should get an ID from SQLite's autoincrement
    assert retrieved_log.user_id == user.id
    assert retrieved_log.correlation_id == corr_id
    assert retrieved_log.event_type == "TEST_EVENT"
    assert retrieved_log.service_origin == "test_service"
    assert retrieved_log.payload == {"data": "value"}
    assert retrieved_log.log_timestamp == log_ts