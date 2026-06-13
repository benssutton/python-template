import logging
import time

from clickhouse_connect.driver.asyncclient import AsyncClient

from schemas.data import DataRowResponse, DataRowsResponse
from schemas.health import ProbeResult

log = logging.getLogger(__name__)


class DataService:
    def __init__(self, client: AsyncClient):
        self._client = client

    async def get_data(self, limit: int) -> DataRowsResponse:
        count_result = await self._client.query("SELECT count() FROM items")
        total = count_result.first_row[0]

        result = await self._client.query(
            "SELECT id, name, value FROM items LIMIT %(limit)s",
            parameters={"limit": limit},
        )
        rows = [
            DataRowResponse(id=row[0], name=row[1], value=row[2])
            for row in result.result_rows
        ]
        return DataRowsResponse(rows=rows, total=total, limit=limit)

    async def health_check(self) -> ProbeResult:

        start = time.perf_counter()
        try:
            ok = await self._client.ping()
            latency_ms = (time.perf_counter() - start) * 1000
            if ok:
                return ProbeResult(
                    name="clickhouse", status="up", latency_ms=round(latency_ms, 2)
                )
            return ProbeResult(
                name="clickhouse",
                status="down",
                latency_ms=round(latency_ms, 2),
                error="ping returned False",
            )
        except Exception as exc:
            latency_ms = (time.perf_counter() - start) * 1000
            return ProbeResult(
                name="clickhouse",
                status="down",
                latency_ms=round(latency_ms, 2),
                error=str(exc),
            )
