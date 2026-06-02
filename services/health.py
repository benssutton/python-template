import logging

from schemas.health import HealthStatusResponse

log = logging.getLogger(__name__)

class HealthService:
    def __init__(self, settings):
        self.settings = settings
    
    def status(self):
        response = HealthStatusResponse(status=self.settings.status)
        return self.settings.status