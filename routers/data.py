import logging

from fastapi import APIRouter, Query

from core.dependencies import DataServiceDep
from schemas.data import DataCountResponse, DataRowsResponse

log = logging.getLogger(__name__)

TAG = "Data Service"
TAG_METADATA = {"name": TAG, "description": "Endpoints for retrieving data"}

router = APIRouter(tags=[TAG])


@router.get("/count", response_model=DataCountResponse)
async def get_count(data_service: DataServiceDep):
    return await data_service.get_count()


@router.get("/rows", response_model=DataRowsResponse)
async def get_rows(
    data_service: DataServiceDep,
    limit: int = Query(default=10, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
):
    return await data_service.get_rows(limit=limit, offset=offset)
