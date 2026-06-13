import logging
from datetime import datetime

from settings import Settings
from services.health import HealthService

log = logging.getLogger(__name__)


class Container:
    """Per-application singleton registry.

    Each FastAPI app built by main.create_app() owns one Container instance
    (stored on app.state.container), so multiple apps — e.g. isolated test
    apps running in one pytest process — never share or clobber each other's
    services.
    """

    def __init__(self, settings: Settings):
        self._singletons = {}
        self.settings = settings
        self.last_request_at: datetime | None = None
        self.register_singleton(HealthService, HealthService(settings, self))

    def register_singleton(self, service_type: type, instance):
        self._singletons[service_type] = instance

    def get(self, service_type: type):
        if service_type in self._singletons:
            return self._singletons[service_type]
        raise ValueError(f"No service registered for type {service_type.__name__}")
