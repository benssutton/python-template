from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from persistence.transaction_store.models.config import Configuration
from schemas.config import ConfigEntry


class ConfigService:
    def __init__(self, session: AsyncSession):
        self._session = session

    async def get_all(self) -> list[ConfigEntry]:
        result = await self._session.execute(select(Configuration))
        return [ConfigEntry.model_validate(row) for row in result.scalars()]

    async def set(self, key: str, value: str) -> ConfigEntry:
        entry = await self._session.merge(Configuration(key=key, value=value))
        await self._session.flush()
        return ConfigEntry.model_validate(entry)
