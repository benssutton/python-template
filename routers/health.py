import logging

from fastapi import APIRouter

from core.dependencies import HealthServiceDep

log = logging.getLogger(__name__)

router = APIRouter(tags=["Application Health"])

@router.get("/status")
async def get_health(health_service: HealthServiceDep):
    return health_service.status()
    


