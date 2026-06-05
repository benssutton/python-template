import logging

from clickhouse_connect.driver.asyncclient import AsyncClient

from schemas.data import DataCountResponse, DataRowResponse, DataRowsResponse

log = logging.getLogger(__name__)


class DataService:
    def __init__(self, client: AsyncClient):
        self._client = client

    async def get_count(self) -> DataCountResponse:
        result = await self._client.query("SELECT count() FROM items")
        return DataCountResponse(count=result.first_row[0])

    async def get_rows(self, limit: int, offset: int) -> DataRowsResponse:
        count_result = await self._client.query("SELECT count() FROM items")
        total = count_result.first_row[0]

        result = await self._client.query(
            f"SELECT id, name, value FROM items LIMIT {limit} OFFSET {offset}"
        )
        rows = [
            DataRowResponse(id=row[0], name=row[1], value=row[2])
            for row in result.result_rows
        ]
        return DataRowsResponse(rows=rows, total=total, limit=limit, offset=offset)
