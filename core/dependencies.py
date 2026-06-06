from typing import Annotated, AsyncGenerator

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from core.settings import Settings, get_settings
from core.container import service_container
from services.health import HealthService
from services.data import DataService
from services.config import ConfigService
from persistence.transaction_store.postgres.postgres_engine import AsyncSessionLocal


def get_health_service():
    return service_container.get(HealthService)

def get_data_service():
    return service_container.get(DataService)

async def get_transaction_session() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise

SettingDep = Annotated[Settings, Depends(get_settings)]
HealthServiceDep = Annotated[HealthService, Depends(get_health_service)]
DataServiceDep = Annotated[DataService, Depends(get_data_service)]
TransactionSessionDep = Annotated[AsyncSession, Depends(get_transaction_session)]

def get_config_service(session: TransactionSessionDep) -> ConfigService:
    return ConfigService(session)

ConfigServiceDep = Annotated[ConfigService, Depends(get_config_service)]