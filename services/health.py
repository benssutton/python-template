import logging

from schemas.health import StatusResponse
from core.settings import Settings

log = logging.getLogger(__name__)

class HealthService:
    def __init__(self, settings):
        self.settings = settings
    
    def status(self):
        response = StatusResponse(status=self.settings.status)
        return self.settings.status