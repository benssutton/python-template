import logging

from core.settings import Settings
from schemas.health import HealthStatusResponse

log = logging.getLogger(__name__)


class HealthService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def status(self) -> HealthStatusResponse:
        return HealthStatusResponse(status=self.settings.status)
