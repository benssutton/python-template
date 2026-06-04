import logging

from fastapi import APIRouter

from core.dependencies import ConfigServiceDep
from schemas.config import ConfigEntry, ConfigSetRequest

log = logging.getLogger(__name__)

TAG = "Configuration"
TAG_METADATA = {
    "name": TAG,
    "description": "Endpoints for managing key-value configuration settings",
}

router = APIRouter(tags=[TAG])


@router.post("/", response_model=ConfigEntry, status_code=201)
async def set_config(body: ConfigSetRequest, config_service: ConfigServiceDep):
    return await config_service.set(body.key, body.value)


@router.get("/", response_model=list[ConfigEntry])
async def get_config(config_service: ConfigServiceDep):
    return await config_service.get_all()
