import logging

from fastapi import APIRouter, Query

from core.dependencies import DataServiceDep
from schemas.data import DataRowsResponse

log = logging.getLogger(__name__)

TAG = "Data Service"
TAG_METADATA = {
    "name": TAG,
    "description": "Endpoints for retrieving data"
    }

router = APIRouter(tags=[TAG])


@router.get("", response_model=DataRowsResponse)
async def get_data(
    data_service: DataServiceDep,
    limit: int = Query(default=10, ge=1, le=100),
):
    return await data_service.get_data(limit=limit)
