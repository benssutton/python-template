from typing import Any

from pydantic import BaseModel


class CacheSetRequest(BaseModel):
    key: str
    value: Any
    ttl_seconds: int | None = None


class CacheEntry(BaseModel):
    key: str
    value: Any
    ttl_seconds: int | None = None
