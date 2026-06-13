from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, Gauge, Info, generate_latest
from prometheus_fastapi_instrumentator import Instrumentator

from settings import Settings


class MetricsService:
    """Owns a per-app Prometheus registry (the multi-app test pattern forbids
    the global default registry) and refreshes custom gauges on each scrape."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self.registry = CollectorRegistry()
        self._instrumentator = Instrumentator(registry=self.registry)

        self.app_info = Info("app", "Application info", registry=self.registry)
        self.app_info.info({"title": settings.app_title, "version": settings.app_version})

        self.dep_up = Gauge(
            "dependency_up", "1 if dependency reachable else 0", ["name"], registry=self.registry)
        self.dep_latency = Gauge(
            "dependency_check_latency_seconds", "Dependency health-check latency",
            ["name"], registry=self.registry)

        self.ingest_state = Gauge(
            "ingest_connection_state", "Ingest transport connection state (1=active)",
            ["transport", "state"], registry=self.registry)
        self.ingest_secs = Gauge(
            "ingest_seconds_since_last_batch", "Seconds since last ingested batch",
            registry=self.registry)
        self.ingest_rows = Gauge(
            "ingest_rows_ingested", "Total rows ingested", registry=self.registry)

        self.proc_cpu = Gauge("process_cpu_percent", "Process CPU percent", registry=self.registry)
        self.proc_mem = Gauge(
            "process_memory_rss_bytes", "Process resident memory bytes", registry=self.registry)
        self.proc_threads = Gauge(
            "process_num_threads", "Process thread count", registry=self.registry)
        self.proc_fds = Gauge(
            "process_open_files", "Process open file count", registry=self.registry)
        self.proc_uptime = Gauge(
            "process_uptime_seconds", "Process uptime seconds", registry=self.registry)

        self.sys_cpu = Gauge("system_cpu_percent", "Host CPU percent", registry=self.registry)
        self.sys_mem_total = Gauge(
            "system_memory_total_bytes", "Host total memory bytes", registry=self.registry)
        self.sys_mem_avail = Gauge(
            "system_memory_available_bytes", "Host available memory bytes", registry=self.registry)
        self.sys_mem_pct = Gauge(
            "system_memory_used_percent", "Host memory used percent", registry=self.registry)
        self.boot_time = Gauge(
            "system_boot_time_seconds", "Host boot time (unix seconds)", registry=self.registry)

    def instrument(self, app) -> None:
        self._instrumentator.instrument(app)

    async def refresh(self, health_service) -> None:
        status = await health_service.detailed_status()

        for dep in status.dependencies:
            self.dep_up.labels(name=dep.name).set(1.0 if dep.status == "up" else 0.0)
            self.dep_latency.labels(name=dep.name).set(dep.latency_ms / 1000.0)

        ingest = status.ingest
        for state in ("connected", "reconnecting", "down"):
            self.ingest_state.labels(transport=ingest.transport, state=state).set(
                1.0 if ingest.connection_state == state else 0.0)
        self.ingest_secs.set(ingest.seconds_since_last_batch or 0.0)
        self.ingest_rows.set(ingest.rows_ingested_total)

        proc = status.system.process
        host = status.system.host
        self.proc_cpu.set(proc.cpu_percent)
        self.proc_mem.set(proc.memory_rss_bytes)
        self.proc_threads.set(proc.num_threads)
        self.proc_fds.set(proc.open_files)
        self.proc_uptime.set(status.uptime.process_seconds)
        self.sys_cpu.set(host.cpu_percent)
        self.sys_mem_total.set(host.memory_total_bytes)
        self.sys_mem_avail.set(host.memory_available_bytes)
        self.sys_mem_pct.set(host.memory_percent)
        self.boot_time.set(status.uptime.system_boot_seconds)

    def render(self) -> tuple[bytes, str]:
        return generate_latest(self.registry), CONTENT_TYPE_LATEST
