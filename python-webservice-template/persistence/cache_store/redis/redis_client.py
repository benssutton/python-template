import redis.asyncio as aioredis

from core.retry import connect_with_backoff
from settings import Settings


class RedisClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client: aioredis.Redis | None = None

    async def __aenter__(self) -> aioredis.Redis:
        async def _connect() -> aioredis.Redis:
            client = aioredis.Redis.from_url(self._settings.redis_url)
            await client.ping()          # smoke-test: raises if Redis is unreachable
            return client

        self._client = await connect_with_backoff(_connect, label="Redis")
        return self._client

    async def __aexit__(self, *_: object) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None
