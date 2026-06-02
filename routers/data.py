import logging

from fastapi import APIRouter

from core.dependencies import DataServiceDep

log = logging.getLogger(__name__)

router = APIRouter(tags=["Data Service"])

@router.get("/shape")
async def get_health(data_service: DataServiceDep):
    return data_service.get_shape()
    


