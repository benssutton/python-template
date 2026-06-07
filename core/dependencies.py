from typing import Annotated

from fastapi import Depends

from settings import Settings, get_settings
from core.container import service_container
from services.health import HealthService
from services.data import DataService
from services.config import ConfigService
from services.cache import CacheService
from services.flight_cache import FlightCacheService


def get_health_service() -> HealthService:
    return service_container.get(HealthService)


def get_data_service() -> DataService:
    return service_container.get(DataService)


def get_config_service() -> ConfigService:
    return service_container.get(ConfigService)


def get_cache_service() -> CacheService:
    return service_container.get(CacheService)


def get_flight_cache_service() -> FlightCacheService:
    return service_container.get(FlightCacheService)


SettingDep = Annotated[Settings, Depends(get_settings)]
HealthServiceDep = Annotated[HealthService, Depends(get_health_service)]
DataServiceDep = Annotated[DataService, Depends(get_data_service)]
ConfigServiceDep = Annotated[ConfigService, Depends(get_config_service)]
CacheServiceDep = Annotated[CacheService, Depends(get_cache_service)]
FlightCacheServiceDep = Annotated[FlightCacheService, Depends(get_flight_cache_service)]
