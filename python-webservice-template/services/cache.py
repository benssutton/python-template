import time
from typing import Any

import redis.asyncio as aioredis

from schemas.cache import CacheEntry
from schemas.health import ProbeResult

class CacheService:
    def __init__(self, client: aioredis.Redis) -> None:
        self._client = client

    async def set(self, key: str, value: Any, ttl_seconds: int | None) -> CacheEntry:
        async with self._client.pipeline(transaction=True) as pipe:
            pipe.json().set(key, "$", value)
            if ttl_seconds is not None:
                pipe.expire(key, ttl_seconds)
            await pipe.execute()
        return CacheEntry(key=key, value=value, ttl_seconds=ttl_seconds)

    async def get(self, key: str) -> CacheEntry | None:
        result = await self._client.json().get(key)
        if result is None:
            return None
        ttl = await self._client.ttl(key)
        return CacheEntry(key=key, value=result, ttl_seconds=ttl if ttl >= 0 else None)

    async def health_check(self) -> ProbeResult:

        start = time.perf_counter()
        try:
            await self._client.ping()
            latency_ms = (time.perf_counter() - start) * 1000
            return ProbeResult(name="redis", status="up", latency_ms=round(latency_ms, 2))
        except Exception as exc:
            latency_ms = (time.perf_counter() - start) * 1000
            return ProbeResult(
                name="redis",
                status="down",
                latency_ms=round(latency_ms, 2),
                error=str(exc),
            )
