import redis
import logging
from typing import Optional, Any
import os # Import os module

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Redis client
try:
    # Get Redis host from environment variable, default to 'localhost'
    redis_host = os.environ.get('REDIS_HOST', 'localhost')
    logger.info(f"Attempting to connect to Redis at host: {redis_host}")
    # Use decode_responses=True to automatically decode responses from bytes to strings
    redis_client = redis.Redis(host=redis_host, port=6379, db=0, decode_responses=True)
    # Test connection
    redis_client.ping()
    logger.info("Successfully connected to Redis.")
except redis.exceptions.ConnectionError as e:
    logger.error(f"Could not connect to Redis: {e}")
    # Set client to None or handle appropriately if connection is critical at startup
    redis_client = None

def cache_set(key: str, value: Any, expire_seconds: Optional[int] = None):
    """
    Sets a key-value pair in the Redis cache.

    Args:
        key: The key to set.
        value: The value to store.
        expire_seconds: Optional time-to-live in seconds.
    """
    if redis_client:
        try:
            redis_client.set(key, value, ex=expire_seconds)
            logger.debug(f"Cache set: key='{key}', expiry={expire_seconds}s")
            return True # Indicate success
        except redis.exceptions.RedisError as e:
            logger.error(f"Error setting cache key '{key}': {e}")
            return False # Indicate failure
    else:
        logger.warning("Redis client not available. Cache set operation skipped.")
        return False # Indicate failure (client not available)

def cache_get(key: str) -> Optional[Any]:
    """
    Gets a value from the Redis cache by key.

    Args:
        key: The key to retrieve.

    Returns:
        The cached value, or None if the key doesn't exist or Redis is unavailable.
    """
    if redis_client:
        try:
            value = redis_client.get(key)
            if value is not None:
                logger.debug(f"Cache hit: key='{key}'")
                return value
            else:
                logger.debug(f"Cache miss: key='{key}'")
                return None
        except redis.exceptions.RedisError as e:
            logger.error(f"Error getting cache key '{key}': {e}")
            return None
    else:
        logger.warning("Redis client not available. Cache get operation skipped.")
        return None