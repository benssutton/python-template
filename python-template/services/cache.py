from typing import Any

import redis.asyncio as aioredis

from schemas.cache import CacheEntry


class CacheService:
    def __init__(self, client: aioredis.Redis) -> None:
        self._client = client

    async def set(self, key: str, value: Any, ttl_seconds: int | None) -> CacheEntry:
        await self._client.json().set(key, "$", value)
        if ttl_seconds is not None:
            await self._client.expire(key, ttl_seconds)
        return CacheEntry(key=key, value=value, ttl_seconds=ttl_seconds)

    async def get(self, key: str) -> CacheEntry | None:
        result = await self._client.json().get(key)
        if result is None:
            return None
        ttl = await self._client.ttl(key)
        return CacheEntry(key=key, value=result, ttl_seconds=ttl if ttl >= 0 else None)
