import asyncpg

from schemas.config import ConfigEntry


class ConfigService:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def get_all(self) -> list[ConfigEntry]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("SELECT key, value FROM configuration ORDER BY key")
            return [ConfigEntry(key=row["key"], value=row["value"]) for row in rows]

    async def set(self, key: str, value: str) -> ConfigEntry:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO configuration (key, value)
                VALUES ($1, $2)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                RETURNING key, value
                """,
                key,
                value,
            )
            return ConfigEntry(key=row["key"], value=row["value"])
