import logging

from fastapi import APIRouter

from core.dependencies import HealthServiceDep
from schemas.health import HealthStatusResponse

log = logging.getLogger(__name__)

TAG = "Application Health"
TAG_METADATA = {
    "name": TAG,
    "description": "Endpoints for checking the status and health of the application"
    }

router = APIRouter(tags=[TAG])


@router.get("/status", response_model=HealthStatusResponse)
async def get_health(health_service: HealthServiceDep):
    return health_service.status()
    


