import logging

from fastapi import APIRouter

from core.dependencies import DataServiceDep

log = logging.getLogger(__name__)

TAG = "Data Service"
TAG_METADATA = {"name": TAG, "description": "Endpoints for retrieving data"}

router = APIRouter(tags=[TAG])

@router.get("/shape")
async def get_health(data_service: DataServiceDep):
    return data_service.get_shape()
    


