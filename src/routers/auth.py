# src/routers/auth.py

import uuid
import logging
from datetime import datetime, timedelta, timezone

import redis.asyncio as aioredis
from fastapi import APIRouter, Depends, HTTPException, status, Request, Body
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from redis.exceptions import RedisError

# Database and Models
from src.db.session import db_session
from src.db.models import User

# Authentication Core
from src.auth.crypto import hash_password, verify_password
from src.auth.jwt import (
    create_access_token, create_refresh_token, decode_and_validate,
    TokenExpired, TokenInvalid, TokenError, JWT_REFRESH_TTL_SECONDS
)
from src.auth.refresh_token_store import (
    store_refresh_token, revoke_refresh_token, is_refresh_token_valid,
    store_password_reset_token, consume_password_reset_token,
    PASSWORD_RESET_TTL_SECONDS
)
from src.auth.schemas import (
    UserRegisterRequest, UserLoginRequest, TokenResponse, RefreshTokenRequest,
    PasswordForgotRequest, PasswordResetRequest, SuccessResponse,
    ErrorResponse, ErrorDetail
)

# Redis connection for rate limiting
from src.cache.connection import get_redis

_log = logging.getLogger(__name__)
router = APIRouter(prefix="/auth", tags=["Authentication"])

# --- Custom Rate Limiting Dependencies ---

async def _rate_limit_check(
    key_prefix: str, identifier: str, limit: int, window_seconds: int
) -> None:
    """Helper function for Redis-based rate limiting."""
    if not identifier: # Avoid issues with empty identifiers
        _log.warning(f"Rate limiting check skipped: Empty identifier provided for prefix {key_prefix}.")
        return

    redis: aioredis.Redis | None = await get_redis()
    if not redis:
        _log.warning(f"Rate limiting check skipped for {key_prefix}:{identifier}: Redis unavailable.")
        return # Fail open if Redis is down

    # Simple fixed window counter for IP/Email based limits
    # Normalize identifier (e.g., lowercase email)
    normalized_identifier = identifier.lower() if key_prefix == "email" else identifier
    key = f"rcpe:rl:{key_prefix}:{normalized_identifier}"

    try:
        # Use INCR and EXPIRE atomically if possible, or separate calls
        # Using pipeline for atomicity
        pipe = redis.pipeline()
        pipe.incr(key)
        pipe.expire(key, window_seconds, nx=True) # nx=True: Only set expiry if key doesn't have one
        results = await pipe.execute()
        current_count = results[0]

        if current_count is None: # Should not happen with INCR but handle defensively
             _log.error(f"Redis INCR command returned None for key {key}. Allowing request.")
             return

        current_count = int(current_count)

        if current_count > limit:
            _log.warning(f"Rate limit exceeded for {key_prefix}:{normalized_identifier}. Count: {current_count}, Limit: {limit}")
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=ErrorResponse(detail=ErrorDetail(
                    code="RATE_LIMIT_EXCEEDED",
                    message=f"Too many requests. Limit: {limit} per {window_seconds} seconds."
                )).model_dump(),
                headers={"Retry-After": str(window_seconds)} # Inform client when to retry
            )
        _log.debug(f"Rate limit check passed for {key_prefix}:{normalized_identifier}. Count: {current_count}/{limit}")

    except RedisError as e:
        _log.error(f"Redis error during rate limiting for {key}:{normalized_identifier}: {e}. Allowing request.")
        # Fail open on Redis errors
    except Exception as e:
         _log.error(f"Unexpected error during rate limiting for {key}:{normalized_identifier}: {e}. Allowing request.", exc_info=True)
         # Fail open on unexpected errors


async def rate_limit_ip(request: Request):
    """Dependency for IP-based rate limiting (e.g., for /login)."""
    limit = 10 # requests
    window = 300 # 5 minutes
    ip_address = request.client.host if request.client else "unknown_ip"
    await _rate_limit_check("ip", ip_address, limit, window)

# Dependency factory for email rate limiting to access the request body
def email_rate_limiter(limit: int, window_seconds: int):
    async def _limit_by_email(request_body: PasswordForgotRequest):
        await _rate_limit_check("email", request_body.email, limit, window_seconds)
    return _limit_by_email


# --- Endpoint Implementations ---

@router.post(
    "/register",
    response_model=SuccessResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Register a new user",
    responses={
        status.HTTP_400_BAD_REQUEST: {"model": ErrorResponse, "description": "Invalid input data (e.g., weak password)"},
        status.HTTP_409_CONFLICT: {"model": ErrorResponse, "description": "Email already registered"},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {"model": ErrorResponse, "description": "Internal server error"},
    }
)
async def register_user(
    user_data: UserRegisterRequest,
    session: AsyncSession = Depends(db_session)
):
    """
    Registers a new user with email and password.
    - Hashes the password using Argon2 + Pepper.
    - Stores the user in the database.
    - Default tier is 'free'.
    """
    # Basic email format validation (more robust validation might be needed)
    if "@" not in user_data.email or "." not in user_data.email.split('@')[-1]:
         raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ErrorResponse(detail=ErrorDetail(code="REG_003", message="Invalid email format.")).model_dump()
        )

    # Check if user already exists
    email_lower = user_data.email.lower()
    existing_user_stmt = select(User).where(User.email == email_lower)
    result = await session.execute(existing_user_stmt)
    existing_user = result.scalars().first()

    if existing_user:
        _log.warning(f"Registration attempt failed: Email '{email_lower}' already exists.")
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=ErrorResponse(detail=ErrorDetail(
                code="REG_001", message="Email address is already registered."
            )).model_dump()
        )

    # Hash password
    try:
        hashed_pw = hash_password(user_data.password)
    except ValueError as e: # Handle empty password case from crypto helper
         _log.error(f"Password hashing failed during registration for {email_lower}: {e}")
         raise HTTPException(
             status_code=status.HTTP_400_BAD_REQUEST,
             detail=ErrorResponse(detail=ErrorDetail(code="REG_002", message=str(e))).model_dump()
         )
    except Exception as e:
        _log.error(f"Unexpected error hashing password during registration for {email_lower}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorResponse(detail=ErrorDetail(code="AUTH_999", message="Internal server error during registration.")).model_dump()
        )


    # Create new user instance
    new_user = User(
        email=email_lower, # Store email in lowercase
        password_hash=hashed_pw,
        tier="free" # Default tier
        # created_at/updated_at are handled by server_default
    )

    # Add to session and implicitly commit via db_session dependency exit
    try:
        session.add(new_user)
        await session.flush() # Flush to get potential DB errors before commit
        _log.info(f"User registered successfully: {new_user.email} (ID: {new_user.id})")
    except Exception as e: # Catch potential IntegrityError etc.
        _log.error(f"Database error during user registration for {email_lower}: {e}", exc_info=True)
        # Rollback is handled by db_session dependency
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorResponse(detail=ErrorDetail(code="DB_ERROR", message="Could not register user due to a database error.")).model_dump()
        )

    return SuccessResponse(message="User registered successfully.", code="REG_OK")


@router.post(
    "/login",
    response_model=TokenResponse,
    summary="Authenticate user and return tokens",
    dependencies=[Depends(rate_limit_ip)], # Apply IP rate limiting
    responses={
        status.HTTP_401_UNAUTHORIZED: {"model": ErrorResponse, "description": "Invalid credentials"},
        status.HTTP_429_TOO_MANY_REQUESTS: {"model": ErrorResponse, "description": "Rate limit exceeded"},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {"model": ErrorResponse, "description": "Internal server error"},
    }
)
async def login_user(
    login_data: UserLoginRequest,
    session: AsyncSession = Depends(db_session)
):
    """
    Authenticates a user via email and password.
    - Verifies credentials against the database.
    - If successful, generates a new JWT access and refresh token pair.
    - Stores the refresh token's JTI in Redis for revocation purposes.
    """
    email_lower = login_data.email.lower()
    # Find user by email
    user_stmt = select(User).where(User.email == email_lower)
    result = await session.execute(user_stmt)
    user = result.scalars().first()

    # Verify password using constant-time comparison helper
    if not user or not verify_password(login_data.password, user.password_hash):
        _log.warning(f"Login attempt failed for email: {email_lower}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=ErrorResponse(detail=ErrorDetail(code="AUTH_004", message="Incorrect email or password.")).model_dump(),
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Generate tokens
    try:
        user_id = user.id
        tier = user.tier
        refresh_token_id = uuid.uuid4() # Unique ID for this refresh token instance

        access_token = create_access_token(user_id=user_id, tier=tier)
        refresh_token = create_refresh_token(user_id=user_id, tier=tier, token_id=refresh_token_id)

        # Store refresh token JTI in Redis
        refresh_expires_at = datetime.now(timezone.utc) + timedelta(seconds=JWT_REFRESH_TTL_SECONDS)
        stored = await store_refresh_token(
            token_id=refresh_token_id,
            user_id=user_id,
            expires_at=refresh_expires_at
        )
        if not stored:
            # Log error but proceed? Or fail login? Let's fail login for consistency.
            _log.error(f"Failed to store refresh token in Redis for user {user_id} during login.")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=ErrorResponse(detail=ErrorDetail(code="AUTH_999", message="Internal server error during login process (token store).")).model_dump()
            )

        _log.info(f"User logged in successfully: {user.email} (ID: {user_id})")
        return TokenResponse(
            access_token=access_token,
            refresh_token=refresh_token,
            token_type="bearer" # Explicitly return type
        )

    except Exception as e:
        _log.error(f"Error during token generation or storage for user {email_lower}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorResponse(detail=ErrorDetail(code="AUTH_999", message="Internal server error during login.")).model_dump()
        )


@router.post(
    "/token/refresh",
    response_model=TokenResponse,
    summary="Refresh access and refresh tokens",
    responses={
        status.HTTP_401_UNAUTHORIZED: {"model": ErrorResponse, "description": "Invalid, expired, or revoked refresh token"},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {"model": ErrorResponse, "description": "Internal server error"},
    }
)
async def refresh_token(
    refresh_request: RefreshTokenRequest,
    # session: AsyncSession = Depends(db_session) # Not strictly needed unless we re-verify user existence/status
):
    """
    Provides a new pair of access and refresh tokens using a valid refresh token.
    - Validates the provided refresh token (signature, expiry, type).
    - Checks if the refresh token (JTI) exists in the Redis store (not revoked).
    - Revokes the used refresh token from Redis.
    - Issues a new pair of tokens and stores the new refresh token JTI.
    """
    try:
        # Decode and validate the refresh token structure and basic claims
        payload = decode_and_validate(token=refresh_request.refresh_token, expected_type="refresh")

        user_id_str = payload.get("sub")
        tier = payload.get("tier") # Tier might be useful, e.g., if tiers change
        token_id_str = payload.get("jti")

        # Validate payload contents further (already done in decode_and_validate, but belt-and-suspenders)
        if not all([user_id_str, tier, token_id_str]):
             _log.error(f"Token refresh failed: Validated token missing claims. Payload: {payload}")
             raise TokenInvalid("Refresh token payload missing required claims (sub, tier, jti).", code="TOKEN_MISSING_CLAIM")

        try:
            user_id = uuid.UUID(user_id_str)
            token_id = uuid.UUID(token_id_str)
        except ValueError:
            _log.error(f"Token refresh failed: Invalid UUID format in claims. UserID: {user_id_str}, TokenID: {token_id_str}")
            raise TokenInvalid("Invalid UUID format in refresh token claims.", code="TOKEN_INVALID_UUID")

        # Check if the token is valid (exists) in the Redis store
        is_valid = await is_refresh_token_valid(token_id=token_id, user_id=user_id)
        if not is_valid:
            _log.warning(f"Refresh token rejected: Token ID {token_id} for user {user_id} not found or expired in Redis.")
            # Use a specific error code for revoked/invalid JTI
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=ErrorResponse(detail=ErrorDetail(code="AUTH_005", message="Refresh token is invalid or has been revoked.")).model_dump(),
                headers={"WWW-Authenticate": "Bearer error=\"invalid_token\", error_description=\"Refresh token revoked or invalid\""},
            )

        # Revoke the old refresh token *before* issuing new ones
        revoked = await revoke_refresh_token(token_id=token_id, user_id=user_id)
        if not revoked:
            # Log error but potentially continue? If revocation fails, the old token might still be usable.
            # For stricter security, maybe fail here. Let's log and continue for now.
             _log.error(f"Failed to revoke used refresh token {token_id} for user {user_id} in Redis. Continuing with refresh.")
        else:
             _log.info(f"Successfully revoked used refresh token {token_id} for user {user_id}.")


        # Issue new tokens
        new_refresh_token_id = uuid.uuid4()
        new_access_token = create_access_token(user_id=user_id, tier=tier)
        new_refresh_token = create_refresh_token(user_id=user_id, tier=tier, token_id=new_refresh_token_id)

        # Store the new refresh token JTI
        new_refresh_expires_at = datetime.now(timezone.utc) + timedelta(seconds=JWT_REFRESH_TTL_SECONDS)
        stored = await store_refresh_token(
            token_id=new_refresh_token_id,
            user_id=user_id,
            expires_at=new_refresh_expires_at
        )
        if not stored:
            _log.error(f"Failed to store NEW refresh token {new_refresh_token_id} in Redis for user {user_id} during refresh.")
            # If storing the new one fails, the user is effectively logged out.
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=ErrorResponse(detail=ErrorDetail(code="AUTH_999", message="Internal server error during token refresh process (new token store).")).model_dump()
            )

        _log.info(f"Token refreshed successfully for user {user_id}.")
        return TokenResponse(
            access_token=new_access_token,
            refresh_token=new_refresh_token,
            token_type="bearer"
        )

    except TokenExpired:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=ErrorResponse(detail=ErrorDetail(code="AUTH_006", message="Refresh token has expired.")).model_dump(),
            headers={"WWW-Authenticate": "Bearer error=\"invalid_token\", error_description=\"Refresh token expired\""},
        )
    except TokenInvalid as e:
         # Use the code/message from the specific TokenInvalid exception
         raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=ErrorResponse(detail=ErrorDetail(code=e.code, message=e.message)).model_dump(),
            headers={"WWW-Authenticate": f"Bearer error=\"invalid_token\", error_description=\"{e.message}\""},
        )
    except TokenError as e: # Catch base TokenError
         _log.error(f"Unexpected TokenError during token refresh: {e}", exc_info=True)
         raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, # Treat as invalid token
            detail=ErrorResponse(detail=ErrorDetail(code=e.code, message=e.message)).model_dump(),
            headers={"WWW-Authenticate": f"Bearer error=\"invalid_token\", error_description=\"{e.message}\""},
        )
    except HTTPException as e:
        # Re-raise HTTPExceptions raised within the try block (like the 401 for invalid JTI)
        raise e
    except Exception as e:
        _log.error(f"Unexpected error during token refresh for user {user_id if 'user_id' in locals() else 'unknown'}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorResponse(detail=ErrorDetail(code="AUTH_999", message="Internal server error during token refresh.")).model_dump()
        )


@router.post(
    "/password/forgot",
    response_model=SuccessResponse,
    summary="Request password reset token",
    # Apply Email rate limiting using the email from the body
    dependencies=[Depends(email_rate_limiter(limit=3, window_seconds=3600))], # 3 req / hour
    responses={
        # 200 OK even if email not found to prevent enumeration
        status.HTTP_429_TOO_MANY_REQUESTS: {"model": ErrorResponse, "description": "Rate limit exceeded"},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {"model": ErrorResponse, "description": "Internal server error"},
    }
)
async def forgot_password(
    forgot_request: PasswordForgotRequest, # Use Pydantic model for request body
    session: AsyncSession = Depends(db_session)
):
    """
    Initiates the password reset process for a given email.
    - Finds the user by email.
    - Generates a unique, time-limited password reset token (UUIDv4).
    - Stores the token in Redis, mapping it to the user ID.
    - **Note:** Actual email sending is assumed to happen elsewhere based on the generated token.
    """
    email = forgot_request.email.lower()

    # Find user by email
    user_stmt = select(User).where(User.email == email)
    result = await session.execute(user_stmt)
    user = result.scalars().first()

    # Return success even if email not found to prevent email enumeration attacks
    if not user:
        _log.info(f"Password forgot request for non-existent or unconfirmed email: {email}")
        # Still return 200 OK
        return SuccessResponse(message="If an account exists for this email, a password reset link has been sent.", code="PWD_FORGOT_OK")

    # Generate reset token
    reset_token = uuid.uuid4()

    # Store token in Redis
    stored = await store_password_reset_token(reset_token=reset_token, user_id=user.id)

    if not stored:
        _log.error(f"Failed to store password reset token in Redis for user {user.id}.")
        # Don't expose internal errors, return a generic success message but log the error
        # Or return 500 if this is critical? Let's return 500 for now.
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorResponse(detail=ErrorDetail(code="AUTH_999", message="Internal server error during password reset request (token store).")).model_dump()
        )

    # TODO: Trigger email sending mechanism here (e.g., publish event, call email service)
    # Pass the user's email and the reset_token (as string) to the email service.
    # For now, just log the token (REMOVE THIS IN PRODUCTION)
    _log.info(f"Password reset token generated for user {user.id} ({email}). Token: {reset_token}. Email sending should be triggered.")

    return SuccessResponse(message="If an account exists for this email, a password reset link has been sent.", code="PWD_FORGOT_OK")


@router.post(
    "/password/reset",
    response_model=SuccessResponse,
    summary="Reset password using a valid token",
    responses={
        status.HTTP_400_BAD_REQUEST: {"model": ErrorResponse, "description": "Invalid/expired token or weak password"},
        status.HTTP_404_NOT_FOUND: {"model": ErrorResponse, "description": "User associated with token not found"},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {"model": ErrorResponse, "description": "Internal server error"},
    }
)
async def reset_password(
    reset_request: PasswordResetRequest,
    session: AsyncSession = Depends(db_session)
):
    """
    Resets a user's password using a valid, single-use reset token.
    - Consumes the reset token from Redis, retrieving the associated user ID.
    - Finds the user in the database.
    - Hashes the new password.
    - Updates the user's password hash.
    """
    try:
        reset_token_uuid = uuid.UUID(reset_request.reset_token)
    except ValueError:
         raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ErrorResponse(detail=ErrorDetail(code="PWD_RESET_001", message="Invalid reset token format.")).model_dump()
        )

    # Consume the token (gets user ID and deletes token atomically)
    user_id = await consume_password_reset_token(reset_token=reset_token_uuid)

    if not user_id:
        _log.warning(f"Password reset attempt failed: Invalid, expired, or already used token {reset_token_uuid}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ErrorResponse(detail=ErrorDetail(code="PWD_RESET_002", message="Password reset token is invalid or has expired.")).model_dump()
        )

    # Find the user associated with the token
    user = await session.get(User, user_id)
    if not user:
        # This case should be rare if consume_password_reset_token worked, but handle defensively
        _log.error(f"Password reset failed: User {user_id} associated with token {reset_token_uuid} not found in DB.")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, # Or 400 Bad Request? 404 seems slightly better
            detail=ErrorResponse(detail=ErrorDetail(code="PWD_RESET_003", message="User associated with the reset token not found.")).model_dump()
        )

    # Hash the new password
    try:
        new_hashed_pw = hash_password(reset_request.new_password)
    except ValueError as e: # Handle empty password
         _log.error(f"Password hashing failed during password reset for user {user_id}: {e}")
         raise HTTPException(
             status_code=status.HTTP_400_BAD_REQUEST,
             detail=ErrorResponse(detail=ErrorDetail(code="PWD_RESET_004", message=str(e))).model_dump()
         )
    except Exception as e:
        _log.error(f"Unexpected error hashing password during reset for user {user_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorResponse(detail=ErrorDetail(code="AUTH_999", message="Internal server error during password reset (hashing).")).model_dump()
        )

    # Update user's password hash
    user.password_hash = new_hashed_pw
    # updated_at should be handled by onupdate=func.now() in the model definition

    # Add to session - commit happens automatically via dependency
    try:
        session.add(user) # Mark user object as dirty
        await session.flush() # Check for DB errors before commit
        _log.info(f"Password successfully reset for user {user_id} ({user.email}).")
    except Exception as e:
        _log.error(f"Database error during password reset update for user {user_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=ErrorResponse(detail=ErrorDetail(code="DB_ERROR", message="Could not reset password due to a database error.")).model_dump()
        )

    return SuccessResponse(message="Password has been reset successfully.", code="PWD_RESET_OK")