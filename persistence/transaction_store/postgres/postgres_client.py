from __future__ import annotations

import asyncpg

from core.settings import Settings


class PostgresClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._pool: asyncpg.Pool | None = None

    async def __aenter__(self) -> asyncpg.Pool:
        self._pool = await asyncpg.create_pool(
            self._settings.postgres_url,
            min_size=self._settings.postgres_pool_min_size,
            max_size=self._settings.postgres_pool_max_size,
        )
        return self._pool

    async def __aexit__(self, *_: object) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
