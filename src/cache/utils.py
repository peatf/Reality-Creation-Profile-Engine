import logging
import asyncio # Import asyncio if needed for sleep
from typing import Optional

import redis.asyncio as redis
from redis.exceptions import RedisError

# Import the new connection helper and constants
from src.cache.connection import get_redis
# Import the helper function from decorators
from src.cache.decorators import NAMESPACE, _unlink_all

logger = logging.getLogger(__name__)

async def clear_cache(pattern: str) -> int:
    """
    Clears cache keys matching the given pattern using SCAN and UNLINK.

    Uses the lazy connection pool from src.cache.connection.

    Args:
        pattern: The pattern to match keys against (e.g., "module.Class.*").
                 The namespace prefix "rcpe:" will be added automatically.

    Returns:
        The number of keys deleted. Returns 0 if Redis is unavailable or
        an error occurs during the process.
    """
    # Get connection lazily
    redis_conn: Optional[redis.Redis] = await get_redis()
    if not redis_conn:
        logger.warning("Redis unavailable, cannot clear cache.")
        return 0

    # Construct the full pattern including namespace and wildcard
    # Ensure the user-provided pattern doesn't already include the namespace
    clean_pattern = pattern.removeprefix(NAMESPACE)
    full_pattern = f"{NAMESPACE}{clean_pattern}"
    if not full_pattern.endswith('*'):
        full_pattern += '*'

    logger.info(f"Starting cache clearing for pattern: {full_pattern}")

    # Call the shared helper function from decorators.py
    deleted_count = await _unlink_all(redis_conn, full_pattern)

    logger.info(f"Cache clearing complete for pattern '{full_pattern}'. Deleted {deleted_count} keys.")

    # Return the count as required by the function's contract and tests
    return deleted_count

# Example Usage (can be run separately or in tests)
# async def main():
#     # Assuming Redis is running and accessible via REDIS_URL
#     # Add some dummy data first
#     conn = await get_redis()
#     if conn:
#         await conn.set(f"{NAMESPACE_PREFIX}test.func:abc", "1")
#         await conn.set(f"{NAMESPACE_PREFIX}test.func:def", "2")
#         await conn.set(f"{NAMESPACE_PREFIX}other.module:xyz", "3")
#         print("Added dummy keys.")
#
#         deleted = await clear_cache("test.func:*")
#         print(f"Deleted {deleted} keys matching 'test.func:*'") # Should be 2
#
#         deleted_again = await clear_cache("other.*")
#         print(f"Deleted {deleted_again} keys matching 'other.*'") # Should be 1
#
#         deleted_nonexistent = await clear_cache("nonexistent.*")
#         print(f"Deleted {deleted_nonexistent} keys matching 'nonexistent.*'") # Should be 0
#
#         # Clean up connection pool if running standalone
#         from src.cache.connection import close_redis
#         await close_redis()
#     else:
#         print("Could not connect to Redis.")
#
# if __name__ == "__main__":
#     import asyncio
#     logging.basicConfig(level=logging.INFO)
#     asyncio.run(main())