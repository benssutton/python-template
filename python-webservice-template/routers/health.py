import logging

from fastapi import APIRouter, Response

from core.dependencies import HealthServiceDep
from schemas.health import DetailedStatusResponse, LivenessResponse, ReadinessResponse

log = logging.getLogger(__name__)

TAG = "Application Health"
TAG_METADATA = {
    "name": TAG,
    "description": "Liveness, readiness and detailed status endpoints",
}

router = APIRouter(tags=[TAG])


@router.get("/live", response_model=LivenessResponse)
async def get_live(health_service: HealthServiceDep):
    return health_service.liveness()


@router.get("/ready", response_model=ReadinessResponse, response_model_exclude_none=True)
async def get_ready(health_service: HealthServiceDep, response: Response):
    result = await health_service.readiness()
    if result.status != "ready":
        response.status_code = 503
    return result


@router.get("/status", response_model=DetailedStatusResponse)
async def get_status(health_service: HealthServiceDep):
    return await health_service.detailed_status()
