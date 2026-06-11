import logging

import pyarrow as pa
from fastapi import APIRouter, HTTPException, Query, Request

from core.dependencies import DataServiceDep, StreamIngestServiceDep
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


@router.get("/cache", response_model=DataRowsResponse)
async def get_cached_data(
    svc: StreamIngestServiceDep,
    limit: int = Query(default=10, ge=1, le=100),
):
    return await svc.get_data(limit=limit)


@router.post("/ingest", status_code=202)
async def ingest_batch(
    request: Request,
    svc: StreamIngestServiceDep,
) -> dict:
    body = await request.body()
    try:
        reader = pa.ipc.open_stream(pa.BufferReader(body))
        for batch in reader:
            await svc.ingest_batch(batch)
    except pa.ArrowInvalid as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"accepted": True}
