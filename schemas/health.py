from datetime import datetime
from pydantic import BaseModel

class StatusResponse(BaseModel):
    status: str

