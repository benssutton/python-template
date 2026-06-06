from core.settings import Settings


def test_settings_has_flight_defaults():
    s = Settings()
    assert s.flight_host == "localhost"
    assert s.flight_port == 8815
    assert s.flight_ticket == "items"
    assert s.lsm_flush_rows == 1000
    assert s.lsm_compaction_runs == 4
