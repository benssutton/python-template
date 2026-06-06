import logging

from core.settings import get_settings
from services.health import HealthService

log = logging.getLogger(__name__)


class Container:
    def __init__(self):
        self._singletons = {}
        self.settings = get_settings()
        self.initialise_container()

    def initialise_container(self):
        self.register_singleton(HealthService, HealthService(self.settings))

    def get_settings(self):
        return self.settings

    def register_singleton(self, service_type: type, instance):
        self._singletons[service_type] = instance

    def get(self, service_type: type):
        if service_type in self._singletons:
            return self._singletons[service_type]
        raise ValueError(f"No service registered for type {service_type.__name__}")

    def clear(self):
        self._singletons.clear()


def create_container():
    return Container()


service_container = create_container()
