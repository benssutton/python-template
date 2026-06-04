import logging

from fastapi import APIRouter

from core.dependencies import TransactionSessionDep
from schemas.config import ConfigEntry, ConfigSetRequest
from services.config import ConfigService

log = logging.getLogger(__name__)

TAG = "Configuration"
TAG_METADATA = {
    "name": TAG,
    "description": "Endpoints for managing key-value configuration settings",
}

router = APIRouter(tags=[TAG])


@router.post("/", response_model=ConfigEntry, status_code=201)
async def set_config(body: ConfigSetRequest, session: TransactionSessionDep):
    return await ConfigService(session).set(body.key, body.value)


@router.get("/", response_model=list[ConfigEntry])
async def get_config(session: TransactionSessionDep):
    return await ConfigService(session).get_all()
