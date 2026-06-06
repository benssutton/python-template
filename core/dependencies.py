from typing import Annotated

from fastapi import Depends

from core.settings import Settings, get_settings
from core.container import service_container
from services.health import HealthService
from services.data import DataService
from services.config import ConfigService


def get_health_service() -> HealthService:
    return service_container.get(HealthService)


def get_data_service() -> DataService:
    return service_container.get(DataService)


def get_config_service() -> ConfigService:
    return service_container.get(ConfigService)


SettingDep = Annotated[Settings, Depends(get_settings)]
HealthServiceDep = Annotated[HealthService, Depends(get_health_service)]
DataServiceDep = Annotated[DataService, Depends(get_data_service)]
ConfigServiceDep = Annotated[ConfigService, Depends(get_config_service)]