import redis.asyncio as aioredis
from typing import Optional, Any
import json
import os

# --- Configuration ---
# Default Redis URL, can be overridden by environment variable
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
DEFAULT_CACHE_TTL_SECONDS = 24 * 60 * 60  # 24 hours

# --- Redis Client Instance ---
# The client should be initialized once and shared.
# FastAPI's startup/shutdown events are a good place for this.
redis_client: Optional[aioredis.Redis] = None

async def get_redis_client() -> aioredis.Redis:
    """
    Returns the Redis client, initializing it if necessary.
    This simple approach creates a client on first use.
    For robust applications, manage lifecycle with FastAPI app events.
    """
    global redis_client
    reinitialize = False
    if redis_client is None:
        reinitialize = True
    else:
        try:
            # Ping existing client to ensure it's alive
            await redis_client.ping()
            print(f"Redis client already connected and responsive at {REDIS_URL}")
        except Exception as e:
            print(f"Existing Redis client at {REDIS_URL} failed ping: {e}. Re-initializing.")
            if redis_client: # Attempt to close it before re-initializing
                try:
                    await redis_client.aclose() # Use aclose()
                except Exception as close_e:
                    print(f"Error closing stale Redis client: {close_e}")
            redis_client = None # Ensure it's None so reinitialization occurs
            reinitialize = True

    if reinitialize:
        try:
            # redis.asyncio.from_url will create a connection pool
            new_client = aioredis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)
            # Test connection
            await new_client.ping()
            redis_client = new_client # Assign to global only after successful ping
            print(f"Successfully (re)connected to Redis at {REDIS_URL}")
        except Exception as e:
            print(f"Error (re)connecting to Redis at {REDIS_URL}: {e}")
            redis_client = None # Ensure it's None if connection failed
            
    return redis_client

async def close_redis_client():
    """Closes the Redis connection pool."""
    global redis_client
    if redis_client:
        await redis_client.close()
        redis_client = None
        print("Redis connection closed.")

# --- Caching Functions ---
async def get_from_cache(key: str) -> Optional[str]:
    """
    Retrieves an item from the cache.
    Returns the string value if found, None otherwise.
    """
    client = await get_redis_client()
    if not client:
        print(f"Cache GET failed: Redis client not available for key: {key}")
        return None
    try:
        cached_value = await client.get(key)
        if cached_value:
            print(f"Cache HIT for key: {key}")
            return cached_value
        else:
            print(f"Cache MISS for key: {key}")
            return None
    except Exception as e:
        print(f"Error getting from cache for key {key}: {e}")
        return None

async def set_to_cache(key: str, value: Any, expire_seconds: int = DEFAULT_CACHE_TTL_SECONDS):
    """
    Sets an item in the cache.
    The value will be JSON serialized if it's not a string.
    """
    client = await get_redis_client()
    if not client:
        print(f"Cache SET failed: Redis client not available for key: {key}")
        return

    try:
        if not isinstance(value, str):
            value_to_store = json.dumps(value) # Serialize if not a string (e.g. Pydantic model.model_dump_json())
        else:
            value_to_store = value

        await client.set(key, value_to_store, ex=expire_seconds)
        print(f"Cache SET for key: {key}, TTL: {expire_seconds}s")
    except Exception as e:
        print(f"Error setting to cache for key {key}: {e}")

async def invalidate_cache_key(key: str) -> bool:
    """
    Invalidates/deletes a specific key from the cache.
    Returns True if key was deleted, False otherwise.
    """
    client = await get_redis_client()
    if not client:
        print(f"Cache DELETE failed: Redis client not available for key: {key}")
        return False
    try:
        result = await client.delete(key)
        if result > 0:
            print(f"Cache key DELETED: {key}")
            return True
        else:
            print(f"Cache key NOT FOUND for deletion: {key}")
            return False
    except Exception as e:
        print(f"Error deleting cache key {key}: {e}")
        return False

async def invalidate_cache_prefix(prefix: str) -> int:
    """
    Invalidates/deletes all keys matching a given prefix.
    Warning: SCAN can be slow on large Redis instances. Use with caution.
    Returns the number of keys deleted.
    """
    client = await get_redis_client()
    if not client:
        print(f"Cache SCAN/DELETE failed: Redis client not available for prefix: {prefix}")
        return 0
    
    deleted_count = 0
    try:
        async for key in client.scan_iter(match=f"{prefix}*"):
            await client.delete(key)
            deleted_count += 1
        print(f"Cache keys DELETED with prefix '{prefix}': {deleted_count}")
        return deleted_count
    except Exception as e:
        print(f"Error deleting cache keys with prefix {prefix}: {e}")
        return 0

# --- FastAPI Event Handlers (to be registered in main.py) ---
async def startup_redis_event():
    """FastAPI startup event to initialize Redis client."""
    await get_redis_client() # Initialize and test connection

async def shutdown_redis_event():
    """FastAPI shutdown event to close Redis client."""
    await close_redis_client()

# Example of how to use (for direct testing of this module)
if __name__ == "__main__":
    import asyncio

    async def test_cache():
        print("Testing Redis Cache Module...")
        # Ensure Redis server is running for this test
        
        # Test SET
        await set_to_cache("mykey", {"data": "example", "value": 123}, expire_seconds=60)
        await set_to_cache("myotherkey", "simple_string", expire_seconds=60)

        # Test GET
        retrieved_value = await get_from_cache("mykey")
        if retrieved_value:
            print(f"Retrieved (mykey): {json.loads(retrieved_value)}")
        
        retrieved_string = await get_from_cache("myotherkey")
        if retrieved_string:
            print(f"Retrieved (myotherkey): {retrieved_string}")

        # Test GET non-existent
        await get_from_cache("nonexistentkey")

        # Test Invalidate
        await invalidate_cache_key("mykey")
        retrieved_value_after_delete = await get_from_cache("mykey")
        print(f"Retrieved (mykey) after delete: {retrieved_value_after_delete}")

        # Test prefix invalidation
        await set_to_cache("prefix:test1", "data1")
        await set_to_cache("prefix:test2", "data2")
        await invalidate_cache_prefix("prefix:")
        print(f"Retrieved (prefix:test1) after prefix delete: {await get_from_cache('prefix:test1')}")


        # Important: Close client when done with tests if not managed by FastAPI
        await close_redis_client()

    asyncio.run(test_cache())