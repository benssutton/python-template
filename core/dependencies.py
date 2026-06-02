from typing import Annotated

from fastapi import Depends

from core.settings import Settings
from core.container import service_container
from services.health import HealthService
from services.data import DataService


def get_settings():
    return service_container.get_settings()


def get_health_service():
    return service_container.get(HealthService)

def get_data_service():
    return service_container.get(DataService)

SettingDep = Annotated[Settings, Depends(get_settings)]
HealthServiceDep = Annotated[HealthService, Depends(get_health_service)]
DataServiceDep = Annotated[DataService, Depends(get_data_service)]