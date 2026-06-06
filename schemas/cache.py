from pydantic import BaseModel


class CacheSetRequest(BaseModel):
    key: str
    value: str
    ttl_seconds: int | None = None


class CacheEntry(BaseModel):
    key: str
    value: str
    ttl_seconds: int | None = None
