from typing import Annotated

from fastapi import Depends, Request

from settings import Settings
from core.container import Container
from services.health import HealthService
from services.data import DataService
from services.config import ConfigService
from services.cache import CacheService
from services.stream_ingest import StreamIngestService


def get_container(request: Request) -> Container:
    # Resolved per-request from the owning app, so each app (including
    # isolated test apps in the same process) sees only its own services.
    return request.app.state.container


ContainerDep = Annotated[Container, Depends(get_container)]


def get_settings_dep(container: ContainerDep) -> Settings:
    return container.settings


def get_health_service(container: ContainerDep) -> HealthService:
    return container.get(HealthService)


def get_data_service(container: ContainerDep) -> DataService:
    return container.get(DataService)


def get_config_service(container: ContainerDep) -> ConfigService:
    return container.get(ConfigService)


def get_cache_service(container: ContainerDep) -> CacheService:
    return container.get(CacheService)


def get_stream_ingest_service(container: ContainerDep) -> StreamIngestService:
    return container.get(StreamIngestService)


SettingDep = Annotated[Settings, Depends(get_settings_dep)]
HealthServiceDep = Annotated[HealthService, Depends(get_health_service)]
DataServiceDep = Annotated[DataService, Depends(get_data_service)]
ConfigServiceDep = Annotated[ConfigService, Depends(get_config_service)]
CacheServiceDep = Annotated[CacheService, Depends(get_cache_service)]
StreamIngestServiceDep = Annotated[StreamIngestService, Depends(get_stream_ingest_service)]


from services.metrics import MetricsService


def get_metrics_service(container: ContainerDep) -> MetricsService:
    return container.get(MetricsService)


MetricsServiceDep = Annotated[MetricsService, Depends(get_metrics_service)]
