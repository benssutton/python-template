import psutil

from core.system_metrics import collect_system_snapshot


def test_collect_system_snapshot_returns_plausible_values():
    process = psutil.Process()
    process.cpu_percent()           # prime (first call returns 0.0)
    snapshot = collect_system_snapshot(process)

    assert snapshot.process.memory_rss_bytes > 0
    assert snapshot.process.num_threads >= 1
    assert snapshot.process.open_files >= 0
    assert snapshot.process.cpu_percent >= 0.0

    assert snapshot.host.memory_total_bytes > 0
    assert 0 <= snapshot.host.memory_available_bytes <= snapshot.host.memory_total_bytes
    assert 0.0 <= snapshot.host.memory_percent <= 100.0
    assert snapshot.host.cpu_percent >= 0.0
