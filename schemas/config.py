from pydantic import BaseModel, ConfigDict


class ConfigSetRequest(BaseModel):
    key: str
    value: str


class ConfigEntry(BaseModel):
    key: str
    value: str
    model_config = ConfigDict(from_attributes=True)
