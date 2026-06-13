import psutil

from schemas.health import HostStats, ProcessStats, SystemSnapshot


def collect_system_snapshot(process: psutil.Process) -> SystemSnapshot:
    """Snapshot of process- and container-visible system resources.

    `cpu_percent()` returns a delta since the previous call, so callers must
    prime it once at startup (the first call always returns 0.0).
    """
    with process.oneshot():
        cpu_percent = process.cpu_percent()
        memory_rss = process.memory_info().rss
        num_threads = process.num_threads()
        try:
            open_files = len(process.open_files())
        except (psutil.AccessDenied, OSError):
            open_files = 0

    vm = psutil.virtual_memory()
    return SystemSnapshot(
        process=ProcessStats(
            cpu_percent=cpu_percent,
            memory_rss_bytes=memory_rss,
            num_threads=num_threads,
            open_files=open_files,
        ),
        host=HostStats(
            cpu_percent=psutil.cpu_percent(interval=None),
            memory_total_bytes=vm.total,
            memory_available_bytes=vm.available,
            memory_percent=vm.percent,
        ),
    )
