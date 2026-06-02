from pydantic import BaseModel

class DataShapeResponse(BaseModel):
    height: int
    width: int
    