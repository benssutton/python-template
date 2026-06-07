import redis.asyncio as aioredis

from settings import Settings


class RedisClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client: aioredis.Redis | None = None

    async def __aenter__(self) -> aioredis.Redis:
        self._client = aioredis.Redis.from_url(self._settings.redis_url)
        return self._client

    async def __aexit__(self, *_: object) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None
