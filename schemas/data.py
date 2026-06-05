from pydantic import BaseModel


class DataCountResponse(BaseModel):
    count: int


class DataRowResponse(BaseModel):
    id: int
    name: str
    value: str


class DataRowsResponse(BaseModel):
    rows: list[DataRowResponse]
    total: int
    limit: int
    offset: int
