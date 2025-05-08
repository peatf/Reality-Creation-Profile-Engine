# tests/auth/test_endpoints.py

import uuid
import time
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
import redis.asyncio as aioredis

from src.db.models import User
from src.auth.crypto import hash_password
from src.auth.jwt import decode_and_validate, JWT_REFRESH_TTL_SECONDS
from src.auth.refresh_token_store import (
    _get_redis_key as get_refresh_token_key,
    _get_password_reset_key,
    PASSWORD_RESET_TTL_SECONDS
)

# Assume test_keys fixture is available (e.g., from conftest.py or imported)
# Assume mock_jwt_env_vars and mock_crypto_env_vars are applied (e.g., via conftest.py autouse)
# Assume client: AsyncClient, db_session: AsyncSession, redis_conn: aioredis.Redis fixtures are available

pytestmark = pytest.mark.asyncio

# --- Helper Functions ---

async def create_test_user(
    session: AsyncSession,
    email: str = "test@example.com",
    password: str = "password123",
    tier: str = "free"
) -> User:
    """Helper to create a user directly in the DB for testing."""
    hashed_pw = hash_password(password)
    user = User(email=email.lower(), password_hash=hashed_pw, tier=tier)
    session.add(user)
    await session.flush()
    await session.refresh(user)
    return user

async def get_user_tokens(client: AsyncClient, email: str, password: str) -> dict:
    """Helper to log in and get tokens."""
    login_data = {"email": email, "password": password}
    response = await client.post("/auth/login", json=login_data)
    response.raise_for_status() # Raise exception for non-2xx status
    return response.json()

# --- Test Cases ---

# === Registration Tests ===

async def test_register_success(client: AsyncClient, db_session: AsyncSession):
    """Test successful user registration."""
    email = f"test_user_{uuid.uuid4()}@example.com"
    password = "strongPassword123!"
    register_data = {"email": email, "password": password}

    response = await client.post("/auth/register", json=register_data)

    assert response.status_code == 201
    assert response.json()["message"] == "User registered successfully."
    assert response.json()["code"] == "REG_OK"

    # Verify user exists in DB
    user_stmt = select(User).where(User.email == email.lower())
    result = await db_session.execute(user_stmt)
    user = result.scalars().first()
    assert user is not None
    assert user.email == email.lower()
    assert user.tier == "free" # Default tier

async def test_register_email_exists(client: AsyncClient, db_session: AsyncSession):
    """Test registration with an email that already exists."""
    user = await create_test_user(db_session, email="existing@example.com")
    register_data = {"email": user.email, "password": "newpassword"}

    response = await client.post("/auth/register", json=register_data)

    assert response.status_code == 409 # Conflict
    assert response.json()["detail"]["code"] == "REG_001"
    assert "already registered" in response.json()["detail"]["message"]

async def test_register_invalid_email(client: AsyncClient):
    """Test registration with an invalid email format."""
    register_data = {"email": "invalid-email", "password": "password123"}
    response = await client.post("/auth/register", json=register_data)
    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "REG_003"

async def test_register_empty_password(client: AsyncClient):
    """Test registration with an empty password."""
    register_data = {"email": "test@example.com", "password": ""}
    response = await client.post("/auth/register", json=register_data)
    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "REG_002" # Error from hash_password

# === Login Tests ===

async def test_login_success(client: AsyncClient, db_session: AsyncSession, redis_conn: aioredis.Redis):
    """Test successful login and token generation."""
    email = "login_success@example.com"
    password = "password123"
    user = await create_test_user(db_session, email=email, password=password)

    login_data = {"email": email, "password": password}
    response = await client.post("/auth/login", json=login_data)

    assert response.status_code == 200
    tokens = response.json()
    assert "access_token" in tokens
    assert "refresh_token" in tokens
    assert tokens["token_type"] == "bearer"

    # Verify access token
    access_payload = decode_and_validate(tokens["access_token"], expected_type="access")
    assert access_payload["sub"] == str(user.id)
    assert access_payload["tier"] == user.tier

    # Verify refresh token and check Redis store
    refresh_payload = decode_and_validate(tokens["refresh_token"], expected_type="refresh")
    assert refresh_payload["sub"] == str(user.id)
    assert refresh_payload["tier"] == user.tier
    assert "jti" in refresh_payload
    token_id = uuid.UUID(refresh_payload["jti"])

    redis_key = get_refresh_token_key(user.id, token_id)
    assert await redis_conn.exists(redis_key)

async def test_login_wrong_password(client: AsyncClient, db_session: AsyncSession):
    """Test login attempt with incorrect password."""
    email = "wrong_pass@example.com"
    password = "password123"
    await create_test_user(db_session, email=email, password=password)

    login_data = {"email": email, "password": "wrongpassword"}
    response = await client.post("/auth/login", json=login_data)

    assert response.status_code == 401
    assert response.json()["detail"]["code"] == "AUTH_004"
    assert "Incorrect email or password" in response.json()["detail"]["message"]

async def test_login_user_not_found(client: AsyncClient):
    """Test login attempt for a non-existent user."""
    login_data = {"email": "notfound@example.com", "password": "password123"}
    response = await client.post("/auth/login", json=login_data)

    assert response.status_code == 401 # Same error as wrong password
    assert response.json()["detail"]["code"] == "AUTH_004"

# Note: Testing rate limiting requires careful timing or mocking Redis INCR.
# Basic check: Ensure the endpoint exists and returns 429 eventually.
# More robust tests might mock redis_conn.incr directly.
async def test_login_rate_limit(client: AsyncClient, db_session: AsyncSession):
    """Approximate test for login rate limiting (may be flaky without mocks)."""
    email = "rate_limit_user@example.com"
    password = "password123"
    await create_test_user(db_session, email=email, password=password)
    login_data = {"email": email, "password": password}

    # Exceed limit (default 10 requests / 5 min)
    for _ in range(11):
        response = await client.post("/auth/login", json=login_data)
        # Allow successful logins until limit is hit
        if response.status_code == 429:
            break
        # Short delay to allow counter to potentially update if needed
        # time.sleep(0.01)
    else:
        pytest.fail("Rate limit (429) was not triggered after 11 requests.")

    assert response.status_code == 429
    assert response.json()["detail"]["code"] == "RATE_LIMIT_EXCEEDED"
    assert "Retry-After" in response.headers


# === Token Refresh Tests ===

async def test_refresh_token_success(client: AsyncClient, db_session: AsyncSession, redis_conn: aioredis.Redis):
    """Test successful token refresh."""
    email = "refresh_user@example.com"
    password = "password123"
    user = await create_test_user(db_session, email=email, password=password)
    initial_tokens = await get_user_tokens(client, email, password)
    initial_refresh_token = initial_tokens["refresh_token"]

    # Decode old refresh token to get its JTI
    old_refresh_payload = decode_and_validate(initial_refresh_token, expected_type="refresh")
    old_token_id = uuid.UUID(old_refresh_payload["jti"])

    # Wait a moment to ensure new tokens have different iat/exp
    time.sleep(1)

    refresh_data = {"refresh_token": initial_refresh_token}
    response = await client.post("/auth/token/refresh", json=refresh_data)

    assert response.status_code == 200
    new_tokens = response.json()
    assert "access_token" in new_tokens
    assert "refresh_token" in new_tokens
    assert new_tokens["token_type"] == "bearer"
    assert new_tokens["access_token"] != initial_tokens["access_token"]
    assert new_tokens["refresh_token"] != initial_refresh_token

    # Verify new access token
    new_access_payload = decode_and_validate(new_tokens["access_token"], expected_type="access")
    assert new_access_payload["sub"] == str(user.id)

    # Verify new refresh token and check Redis store
    new_refresh_payload = decode_and_validate(new_tokens["refresh_token"], expected_type="refresh")
    assert new_refresh_payload["sub"] == str(user.id)
    assert "jti" in new_refresh_payload
    new_token_id = uuid.UUID(new_refresh_payload["jti"])

    # Check old token JTI is revoked (deleted) from Redis
    old_redis_key = get_refresh_token_key(user.id, old_token_id)
    assert not await redis_conn.exists(old_redis_key)

    # Check new token JTI exists in Redis
    new_redis_key = get_refresh_token_key(user.id, new_token_id)
    assert await redis_conn.exists(new_redis_key)

async def test_refresh_token_invalid(client: AsyncClient):
    """Test refresh with an invalid token string."""
    refresh_data = {"refresh_token": "this.is.not.a.jwt"}
    response = await client.post("/auth/token/refresh", json=refresh_data)
    assert response.status_code == 401
    assert response.json()["detail"]["code"] == "TOKEN_DECODE_ERROR" # From jwt lib

async def test_refresh_token_expired(client: AsyncClient, db_session: AsyncSession):
    """Test refresh with an expired refresh token."""
    email = "refresh_expired@example.com"
    password = "password123"
    await create_test_user(db_session, email=email, password=password)
    tokens = await get_user_tokens(client, email, password)

    # Wait for refresh token to expire (JWT_REFRESH_TTL_SECONDS = 20 in mock)
    time.sleep(21)

    refresh_data = {"refresh_token": tokens["refresh_token"]}
    response = await client.post("/auth/token/refresh", json=refresh_data)
    assert response.status_code == 401
    assert response.json()["detail"]["code"] == "AUTH_006" # Specific code for expired refresh
    assert "Refresh token has expired" in response.json()["detail"]["message"]

async def test_refresh_token_revoked(client: AsyncClient, db_session: AsyncSession, redis_conn: aioredis.Redis):
    """Test refresh with a token whose JTI has been removed from Redis."""
    email = "refresh_revoked@example.com"
    password = "password123"
    user = await create_test_user(db_session, email=email, password=password)
    tokens = await get_user_tokens(client, email, password)
    refresh_token = tokens["refresh_token"]

    # Manually revoke the token by deleting its key from Redis
    payload = decode_and_validate(refresh_token, expected_type="refresh")
    token_id = uuid.UUID(payload["jti"])
    redis_key = get_refresh_token_key(user.id, token_id)
    await redis_conn.delete(redis_key)

    refresh_data = {"refresh_token": refresh_token}
    response = await client.post("/auth/token/refresh", json=refresh_data)
    assert response.status_code == 401
    assert response.json()["detail"]["code"] == "AUTH_005" # Specific code for revoked/invalid JTI
    assert "invalid or has been revoked" in response.json()["detail"]["message"]

async def test_refresh_token_used_twice(client: AsyncClient, db_session: AsyncSession):
    """Test attempting to use the same refresh token twice."""
    email = "refresh_reuse@example.com"
    password = "password123"
    await create_test_user(db_session, email=email, password=password)
    tokens = await get_user_tokens(client, email, password)
    refresh_token = tokens["refresh_token"]

    # First refresh (should succeed)
    refresh_data = {"refresh_token": refresh_token}
    response1 = await client.post("/auth/token/refresh", json=refresh_data)
    assert response1.status_code == 200

    # Second refresh with the *same* original token (should fail as it's revoked)
    response2 = await client.post("/auth/token/refresh", json=refresh_data)
    assert response2.status_code == 401
    assert response2.json()["detail"]["code"] == "AUTH_005" # Revoked

# === Password Forgot/Reset Tests ===

async def test_password_forgot_success(client: AsyncClient, db_session: AsyncSession, redis_conn: aioredis.Redis):
    """Test successful password forgot request."""
    email = "forgot_pass@example.com"
    user = await create_test_user(db_session, email=email)

    forgot_data = {"email": email}
    response = await client.post("/auth/password/forgot", json=forgot_data)

    assert response.status_code == 200
    assert "reset link has been sent" in response.json()["message"]
    assert response.json()["code"] == "PWD_FORGOT_OK"

    # Check if *any* reset token key exists for this user (we don't know the token value here)
    # This requires scanning keys - potentially slow, use carefully or find token via logs/mocking email
    # Alternative: Mock store_password_reset_token to capture the token
    # For now, we'll assume it worked based on the 200 OK and proceed to reset test

async def test_password_forgot_email_not_found(client: AsyncClient):
    """Test password forgot for a non-existent email (should still return 200 OK)."""
    forgot_data = {"email": "not_a_user@example.com"}
    response = await client.post("/auth/password/forgot", json=forgot_data)
    assert response.status_code == 200
    assert "reset link has been sent" in response.json()["message"]

# Rate limit test for forgot password (similar structure to login rate limit)
async def test_password_forgot_rate_limit(client: AsyncClient, db_session: AsyncSession):
    """Approximate test for password forgot rate limiting (3 req / hour)."""
    email = "forgot_rate_limit@example.com"
    await create_test_user(db_session, email=email)
    forgot_data = {"email": email}

    for i in range(4): # Limit is 3
        response = await client.post("/auth/password/forgot", json=forgot_data)
        if response.status_code == 429:
            break
        # time.sleep(0.01) # Small delay if needed
    else:
        pytest.fail("Rate limit (429) was not triggered after 4 requests.")

    assert response.status_code == 429
    assert response.json()["detail"]["code"] == "RATE_LIMIT_EXCEEDED"

async def test_password_reset_success(client: AsyncClient, db_session: AsyncSession, redis_conn: aioredis.Redis):
    """Test successful password reset flow."""
    email = "reset_pass_success@example.com"
    old_password = "oldPassword1"
    new_password = "newStrongPassword2!"
    user = await create_test_user(db_session, email=email, password=old_password)

    # 1. Trigger forgot password to get a token (simulate by directly storing in Redis)
    reset_token = uuid.uuid4()
    redis_key = _get_password_reset_key(reset_token)
    await redis_conn.set(redis_key, str(user.id), ex=PASSWORD_RESET_TTL_SECONDS)
    assert await redis_conn.exists(redis_key)

    # 2. Reset password using the token
    reset_data = {"reset_token": str(reset_token), "new_password": new_password}
    response = await client.post("/auth/password/reset", json=reset_data)

    assert response.status_code == 200
    assert response.json()["message"] == "Password has been reset successfully."
    assert response.json()["code"] == "PWD_RESET_OK"

    # 3. Verify token is consumed (deleted) from Redis
    assert not await redis_conn.exists(redis_key)

    # 4. Verify user password hash is updated in DB
    await db_session.refresh(user) # Refresh user state from DB
    from src.auth.crypto import verify_password # Reload potentially mocked module
    assert verify_password(new_password, user.password_hash) is True
    assert verify_password(old_password, user.password_hash) is False

    # 5. Verify login with new password works
    login_data = {"email": email, "password": new_password}
    login_response = await client.post("/auth/login", json=login_data)
    assert login_response.status_code == 200

async def test_password_reset_invalid_token(client: AsyncClient):
    """Test password reset with an invalid/non-existent token."""
    reset_data = {"reset_token": str(uuid.uuid4()), "new_password": "password123"}
    response = await client.post("/auth/password/reset", json=reset_data)
    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "PWD_RESET_002"
    assert "invalid or has expired" in response.json()["detail"]["message"]

async def test_password_reset_expired_token(client: AsyncClient, db_session: AsyncSession, redis_conn: aioredis.Redis):
    """Test password reset with an expired token."""
    email = "reset_pass_expired@example.com"
    user = await create_test_user(db_session, email=email)
    reset_token = uuid.uuid4()
    redis_key = _get_password_reset_key(reset_token)
    await redis_conn.set(redis_key, str(user.id), ex=1) # Set expiry to 1 second

    time.sleep(2) # Wait for token to expire

    reset_data = {"reset_token": str(reset_token), "new_password": "password123"}
    response = await client.post("/auth/password/reset", json=reset_data)
    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "PWD_RESET_002"

async def test_password_reset_token_reuse(client: AsyncClient, db_session: AsyncSession, redis_conn: aioredis.Redis):
    """Test attempting to reuse a password reset token."""
    email = "reset_pass_reuse@example.com"
    user = await create_test_user(db_session, email=email)
    reset_token = uuid.uuid4()
    redis_key = _get_password_reset_key(reset_token)
    await redis_conn.set(redis_key, str(user.id), ex=PASSWORD_RESET_TTL_SECONDS)

    # First reset attempt (should succeed)
    reset_data = {"reset_token": str(reset_token), "new_password": "newPassword1"}
    response1 = await client.post("/auth/password/reset", json=reset_data)
    assert response1.status_code == 200

    # Second reset attempt with the same token (should fail)
    reset_data2 = {"reset_token": str(reset_token), "new_password": "newPassword2"}
    response2 = await client.post("/auth/password/reset", json=reset_data2)
    assert response2.status_code == 400
    assert response2.json()["detail"]["code"] == "PWD_RESET_002" # Invalid/expired

# === Middleware and Tier Guard Tests ===
# These require a dummy protected endpoint in the test app setup

# Example: Assuming a route @app.get("/protected/free") requires auth
# and @app.get("/protected/premium") requires "premium" tier via Depends(require_tier("premium"))

async def test_protected_route_no_token(client: AsyncClient):
    """Test accessing protected route without token returns 401."""
    response = await client.get("/protected/free") # Assuming this endpoint exists in test app
    assert response.status_code == 401
    assert response.json()["detail"]["code"] == "AUTH_001" # Missing credentials
    assert response.headers["www-authenticate"] == "Bearer"

async def test_protected_route_invalid_token(client: AsyncClient):
    """Test accessing protected route with invalid token returns 401."""
    headers = {"Authorization": "Bearer invalid.token.string"}
    response = await client.get("/protected/free", headers=headers)
    assert response.status_code == 401
    # Error code might vary depending on why it's invalid (e.g., TOKEN_DECODE_ERROR)
    assert response.json()["detail"]["code"].startswith("TOKEN_")

async def test_protected_route_expired_token(client: AsyncClient, db_session: AsyncSession):
    """Test accessing protected route with expired access token returns 401."""
    email = "expired_access@example.com"
    password = "password123"
    await create_test_user(db_session, email=email, password=password)
    tokens = await get_user_tokens(client, email, password)
    access_token = tokens["access_token"]

    # Wait for access token to expire (JWT_ACCESS_TTL_SECONDS = 10 in mock)
    time.sleep(11)

    headers = {"Authorization": f"Bearer {access_token}"}
    response = await client.get("/protected/free", headers=headers)
    assert response.status_code == 401
    assert response.json()["detail"]["code"] == "AUTH_002" # Expired token
    assert "Token has expired" in response.json()["detail"]["message"]

async def test_protected_route_success(client: AsyncClient, db_session: AsyncSession):
    """Test successful access to a protected route with a valid token."""
    email = "valid_access@example.com"
    password = "password123"
    await create_test_user(db_session, email=email, password=password)
    tokens = await get_user_tokens(client, email, password)
    headers = {"Authorization": f"Bearer {tokens['access_token']}"}

    response = await client.get("/protected/free", headers=headers)
    assert response.status_code == 200 # Assuming /protected/free returns 200 OK
    # Add assertion for expected response body if applicable
    # assert response.json() == {"message": "Access granted for free tier"}

async def test_tier_guard_insufficient_tier(client: AsyncClient, db_session: AsyncSession):
    """Test accessing a premium route with a free user returns 403."""
    email = "free_user@example.com"
    password = "password123"
    # Ensure user is created with 'free' tier (default)
    await create_test_user(db_session, email=email, password=password, tier="free")
    tokens = await get_user_tokens(client, email, password)
    headers = {"Authorization": f"Bearer {tokens['access_token']}"}

    response = await client.get("/protected/premium", headers=headers) # Requires premium
    assert response.status_code == 403 # Forbidden
    assert response.json()["detail"]["code"] == "AUTH_003"
    assert "Insufficient permissions" in response.json()["detail"]["message"]

async def test_tier_guard_sufficient_tier(client: AsyncClient, db_session: AsyncSession):
    """Test accessing a premium route with a premium user returns 200."""
    email = "premium_user@example.com"
    password = "password123"
    # Create user with 'premium' tier
    await create_test_user(db_session, email=email, password=password, tier="premium")
    tokens = await get_user_tokens(client, email, password)
    headers = {"Authorization": f"Bearer {tokens['access_token']}"}

    response = await client.get("/protected/premium", headers=headers)
    assert response.status_code == 200 # Access granted
    # Add assertion for expected response body if applicable
    # assert response.json() == {"message": "Access granted for premium tier"}

async def test_tier_guard_admin_access_premium(client: AsyncClient, db_session: AsyncSession):
    """Test accessing a premium route with an admin user returns 200."""
    email = "admin_user@example.com"
    password = "password123"
    # Assuming 'admin' tier level is higher than 'premium'
    await create_test_user(db_session, email=email, password=password, tier="admin")
    tokens = await get_user_tokens(client, email, password)
    headers = {"Authorization": f"Bearer {tokens['access_token']}"}

    response = await client.get("/protected/premium", headers=headers)
    assert response.status_code == 200 # Access granted