import logging

import polars as pl

from core.settings import Settings
from schemas.data import DataShapeResponse

log = logging.getLogger(__name__)

class DataService:
    def __init__(self, settings: Settings):
        self.settings = settings

    def get_shape(self):
        df = pl.read_ipc_stream(self.settings.data_dir + "/data.ipc_stream")

        response = DataShapeResponse(
            height=df.height,
            width=df.width
        )

        return response