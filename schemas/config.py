from pydantic import BaseModel


class ConfigSetRequest(BaseModel):
    key: str
    value: str


class ConfigEntry(BaseModel):
    key: str
    value: str
