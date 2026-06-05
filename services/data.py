import logging

import polars as pl

from core.settings import Settings

log = logging.getLogger(__name__)


class DataService:
    def __init__(self, settings: Settings):
        self.settings = settings
