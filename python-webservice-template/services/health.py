import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import psutil

from core.system_metrics import collect_system_snapshot
from schemas.health import (
    AppInfo,
    CheckResult,
    DetailedStatusResponse,
    HealthStatusResponse,
    IngestHealth,
    LivenessResponse,
    ProbeResult,
    ReadinessResponse,
    RequestInfo,
    UptimeInfo,
)
from services.cache import CacheService
from services.config import ConfigService
from services.data import DataService
from services.stream_ingest import StreamIngestService
from settings import Settings

if TYPE_CHECKING:
    from core.container import Container

log = logging.getLogger(__name__)


class HealthService:
    def __init__(self, settings: Settings, container: "Container") -> None:
        self.settings = settings
        self._container = container
        self._started_at = time.monotonic()
        self._process = psutil.Process()
        self._process.cpu_percent()        # prime process CPU delta
        psutil.cpu_percent(interval=None)  # prime host CPU delta

    # Kept for the MCP get_health_status tool (REST-mirroring, simple string).
    def status(self) -> HealthStatusResponse:
        return HealthStatusResponse(status=self.settings.status)

    def _uptime_seconds(self) -> float:
        return time.monotonic() - self._started_at

    def liveness(self) -> LivenessResponse:
        return LivenessResponse(status="alive", uptime_seconds=round(self._uptime_seconds(), 3))

    async def _probe(self, service_type: type, name: str) -> ProbeResult:
        try:
            service = self._container.get(service_type)
        except ValueError:
            return ProbeResult(name=name, status="down", latency_ms=0.0, error="initializing")
        try:
            return await asyncio.wait_for(
                service.health_check(), timeout=self.settings.health_check_timeout_seconds
            )
        except asyncio.TimeoutError:
            return ProbeResult(
                name=name, status="down",
                latency_ms=round(self.settings.health_check_timeout_seconds * 1000, 2),
                error="timeout",
            )
        except Exception as exc:
            return ProbeResult(name=name, status="down", latency_ms=0.0, error=str(exc))

    async def _gather_dependencies(self) -> list[ProbeResult]:
        return list(await asyncio.gather(
            self._probe(ConfigService, "postgres"),
            self._probe(DataService, "clickhouse"),
            self._probe(CacheService, "redis"),
        ))

    async def _ingest_health(self) -> IngestHealth:
        try:
            service = self._container.get(StreamIngestService)
        except ValueError:
            return IngestHealth(
                transport=self.settings.ingest_transport,
                connection_state="down", thread_alive=False,
            )
        return await service.health_check()

    def _ingest_status(self, ingest: IngestHealth) -> str:
        if ingest.connection_state != "connected":
            return "down"
        if ingest.stale and self.settings.ingest_stale_fails_readiness:
            return "down"
        return "up"

    async def readiness(self) -> ReadinessResponse:
        deps = await self._gather_dependencies()
        ingest = await self._ingest_health()
        ingest_status = self._ingest_status(ingest)

        checks = [
            CheckResult(name=d.name, status=d.status, latency_ms=d.latency_ms, error=d.error)
            for d in deps
        ]
        checks.append(CheckResult(
            name="ingest", status=ingest_status, transport=ingest.transport,
            connection_state=ingest.connection_state, thread_alive=ingest.thread_alive,
            last_batch_at=ingest.last_batch_at,
            seconds_since_last_batch=ingest.seconds_since_last_batch,
        ))

        all_up = all(d.status == "up" for d in deps) and ingest_status == "up"
        return ReadinessResponse(status="ready" if all_up else "not_ready", checks=checks)

    async def detailed_status(self) -> DetailedStatusResponse:
        deps = await self._gather_dependencies()
        ingest = await self._ingest_health()
        snapshot = collect_system_snapshot(self._process)
        return DetailedStatusResponse(
            app=AppInfo(
                title=self.settings.app_title,
                version=self.settings.app_version,
                status=self.settings.status,
            ),
            uptime=UptimeInfo(
                process_seconds=round(self._uptime_seconds(), 3),
                system_boot_seconds=psutil.boot_time(),
            ),
            dependencies=deps,
            ingest=ingest,
            requests=RequestInfo(last_request_at=self._container.last_request_at),
            system=snapshot,
        )
