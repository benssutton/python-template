import logging

from core.settings import Settings
from services.health import HealthService

log = logging.getLogger(__name__)


class Container:
    def __init__(self):
        self._singletons = {}
        self._factories = {}
        self.settings = Settings()
        self.initialise_container()

    def initialise_container(self):
        self.register_singleton(HealthService, HealthService(self.settings))

    def get_settings(self):
        return self.settings

    def register_singleton(self, service_type: type, instance):
        self._singletons[service_type] = instance

    def register_factory(self, service_type: type, factory_func: callable):
        self._factories[service_type] = factory_func

    def get(self, service_type: type):
        if service_type in self._singletons:
            return self._singletons[service_type]
        if service_type in self._factories:
            return self._factories[service_type]()
        try:
            return service_type()
        except Exception as e:
            raise ValueError(f"Cannot resolve service of type {service_type}: {e}")

    def clear(self):
        self._singletons.clear()
        self._factories.clear()


def create_container():
    return Container()


service_container = create_container()
