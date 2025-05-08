# src/auth/refresh_token_store.py
import uuid
import logging
from datetime import datetime, timezone

import redis.asyncio as aioredis
from redis.exceptions import RedisError

from src.cache.connection import get_redis # Use the existing connection manager

_log = logging.getLogger(__name__)

# Define a prefix for refresh token keys for better organization in Redis
REFRESH_TOKEN_KEY_PREFIX = "rcpe:rt"

def _get_redis_key(user_id: uuid.UUID, token_id: uuid.UUID) -> str:
    """Constructs the Redis key for a refresh token."""
    if not isinstance(user_id, uuid.UUID) or not isinstance(token_id, uuid.UUID):
        raise TypeError("user_id and token_id must be UUID objects.")
    return f"{REFRESH_TOKEN_KEY_PREFIX}:{user_id}:{token_id}"

async def store_refresh_token(token_id: uuid.UUID, user_id: uuid.UUID, expires_at: datetime) -> bool:
    """
    Stores a refresh token identifier in Redis with its expiration time.

    Args:
        token_id: The unique identifier (jti) of the refresh token.
        user_id: The ID of the user the token belongs to.
        expires_at: The absolute expiration datetime of the token (UTC).

    Returns:
        True if the token was successfully stored, False otherwise.
    """
    if not isinstance(token_id, uuid.UUID) or not isinstance(user_id, uuid.UUID):
        _log.error("Invalid input type for store_refresh_token: IDs must be UUIDs.")
        return False
    if not isinstance(expires_at, datetime):
         _log.error("Invalid input type for store_refresh_token: expires_at must be datetime.")
         return False

    redis: aioredis.Redis | None = await get_redis()
    if not redis:
        _log.error(f"Failed to store refresh token {token_id}: Redis connection unavailable.")
        return False

    key = _get_redis_key(user_id, token_id)
    now = datetime.now(timezone.utc)

    # Ensure expires_at is timezone-aware (UTC)
    if expires_at.tzinfo is None or expires_at.tzinfo.utcoffset(expires_at) != timezone.utc.utcoffset(now):
        _log.warning(f"store_refresh_token received non-UTC or naive datetime for token {token_id}. Converting/Assuming UTC.")
        expires_at = expires_at.astimezone(timezone.utc)

    # Calculate TTL in seconds relative to now
    ttl_seconds = int((expires_at - now).total_seconds())

    if ttl_seconds <= 0:
        _log.warning(f"Attempted to store already expired refresh token {token_id} for user {user_id}.")
        # Don't store expired tokens.
        return False

    try:
        # Store a simple value ("1" or the timestamp) with the calculated TTL
        # Storing the expiry timestamp as requested.
        await redis.set(key, int(expires_at.timestamp()), ex=ttl_seconds)
        _log.debug(f"Stored refresh token {token_id} for user {user_id} with TTL {ttl_seconds}s.")
        return True
    except RedisError as e:
        _log.error(f"Redis error storing refresh token {token_id} for user {user_id}: {e}")
        return False
    except Exception as e:
        _log.error(f"Unexpected error storing refresh token {token_id} for user {user_id}: {e}", exc_info=True)
        return False


async def revoke_refresh_token(token_id: uuid.UUID, user_id: uuid.UUID) -> bool:
    """
    Revokes (deletes) a refresh token identifier from Redis.

    Args:
        token_id: The unique identifier (jti) of the refresh token to revoke.
        user_id: The ID of the user the token belongs to.

    Returns:
        True if the token was successfully deleted or didn't exist, False on Redis error.
    """
    if not isinstance(token_id, uuid.UUID) or not isinstance(user_id, uuid.UUID):
        _log.error("Invalid input type for revoke_refresh_token: IDs must be UUIDs.")
        return False

    redis: aioredis.Redis | None = await get_redis()
    if not redis:
        _log.error(f"Failed to revoke refresh token {token_id}: Redis connection unavailable.")
        return False

    key = _get_redis_key(user_id, token_id)
    try:
        deleted_count = await redis.delete(key)
        if deleted_count > 0:
            _log.info(f"Revoked refresh token {token_id} for user {user_id}.")
        else:
             _log.debug(f"Attempted to revoke non-existent refresh token {token_id} for user {user_id}.")
        return True # Success includes deleting a non-existent key
    except RedisError as e:
        _log.error(f"Redis error revoking refresh token {token_id} for user {user_id}: {e}")
        return False
    except Exception as e:
        _log.error(f"Unexpected error revoking refresh token {token_id} for user {user_id}: {e}", exc_info=True)
        return False


async def is_refresh_token_valid(token_id: uuid.UUID, user_id: uuid.UUID) -> bool:
    """
    Checks if a refresh token identifier exists (has not expired or been revoked) in Redis.

    Args:
        token_id: The unique identifier (jti) of the refresh token.
        user_id: The ID of the user the token belongs to.

    Returns:
        True if the token exists in Redis, False otherwise (including Redis errors).
    """
    if not isinstance(token_id, uuid.UUID) or not isinstance(user_id, uuid.UUID):
        _log.error("Invalid input type for is_refresh_token_valid: IDs must be UUIDs.")
        return False

    redis: aioredis.Redis | None = await get_redis()
    if not redis:
        _log.error(f"Failed to check refresh token {token_id} validity: Redis connection unavailable.")
        # Fail closed: If we can't check, assume it's invalid for security.
        return False

    key = _get_redis_key(user_id, token_id)
    try:
        exists = await redis.exists(key)
        is_valid = exists > 0
        _log.debug(f"Refresh token {token_id} for user {user_id} validity check: {'Valid' if is_valid else 'Invalid/Expired/Revoked'}")
        return is_valid
    except RedisError as e:
        _log.error(f"Redis error checking validity for refresh token {token_id} for user {user_id}: {e}")
        return False # Fail closed
    except Exception as e:
        _log.error(f"Unexpected error checking validity for refresh token {token_id} for user {user_id}: {e}", exc_info=True)
        return False # Fail closed

# --- Password Reset Token Store ---
# Similar logic for password reset tokens

PASSWORD_RESET_KEY_PREFIX = "rcpe:pwreset"
PASSWORD_RESET_TTL_SECONDS = 900 # 15 minutes

def _get_password_reset_key(reset_token: uuid.UUID) -> str:
    """Constructs the Redis key for a password reset token."""
    if not isinstance(reset_token, uuid.UUID):
        raise TypeError("reset_token must be a UUID object.")
    return f"{PASSWORD_RESET_KEY_PREFIX}:{reset_token}"

async def store_password_reset_token(reset_token: uuid.UUID, user_id: uuid.UUID) -> bool:
    """Stores a password reset token mapped to a user ID."""
    if not isinstance(reset_token, uuid.UUID) or not isinstance(user_id, uuid.UUID):
        _log.error("Invalid input type for store_password_reset_token: IDs must be UUIDs.")
        return False

    redis: aioredis.Redis | None = await get_redis()
    if not redis:
        _log.error(f"Failed to store password reset token {reset_token}: Redis connection unavailable.")
        return False

    key = _get_password_reset_key(reset_token)
    try:
        # Store user_id as the value (bytes), with the reset TTL
        await redis.set(key, str(user_id).encode('utf-8'), ex=PASSWORD_RESET_TTL_SECONDS)
        _log.info(f"Stored password reset token {reset_token} for user {user_id}.")
        return True
    except RedisError as e:
        _log.error(f"Redis error storing password reset token {reset_token}: {e}")
        return False
    except Exception as e:
        _log.error(f"Unexpected error storing password reset token {reset_token}: {e}", exc_info=True)
        return False

async def consume_password_reset_token(reset_token: uuid.UUID) -> uuid.UUID | None:
    """
    Retrieves the user ID associated with a reset token and deletes the token.
    This makes the token single-use.

    Args:
        reset_token: The password reset token.

    Returns:
        The user ID (as UUID) if the token was valid and consumed, None otherwise.
    """
    if not isinstance(reset_token, uuid.UUID):
        _log.error("Invalid input type for consume_password_reset_token: reset_token must be UUID.")
        return None

    redis: aioredis.Redis | None = await get_redis()
    if not redis:
        _log.error(f"Failed to consume password reset token {reset_token}: Redis connection unavailable.")
        return None

    key = _get_password_reset_key(reset_token)
    try:
        # Use GETDEL to atomically get the value and delete the key
        user_id_bytes = await redis.getdel(key)

        if user_id_bytes:
            user_id_str = user_id_bytes.decode('utf-8')
            try:
                user_id = uuid.UUID(user_id_str)
                _log.info(f"Consumed password reset token {reset_token} for user {user_id}.")
                return user_id
            except ValueError:
                 _log.error(f"Invalid user ID format stored for password reset token {reset_token}: {user_id_str}")
                 return None # Data corruption in Redis?
        else:
            _log.warning(f"Attempted to consume non-existent or expired password reset token {reset_token}.")
            return None # Token not found or already used/expired
    except RedisError as e:
        _log.error(f"Redis error consuming password reset token {reset_token}: {e}")
        return None
    except Exception as e:
        _log.error(f"Unexpected error consuming password reset token {reset_token}: {e}", exc_info=True)
        return None