import redis.asyncio as aioredis

from schemas.cache import CacheEntry


class CacheService:
    def __init__(self, client: aioredis.Redis) -> None:
        self._client = client

    async def set(self, key: str, value: str, ttl_seconds: int | None) -> CacheEntry:
        await self._client.set(key, value, ex=ttl_seconds)
        return CacheEntry(key=key, value=value, ttl_seconds=ttl_seconds)

    async def get(self, key: str) -> CacheEntry | None:
        value = await self._client.get(key)
        if value is None:
            return None
        ttl = await self._client.ttl(key)
        return CacheEntry(key=key, value=value.decode(), ttl_seconds=ttl if ttl >= 0 else None)
