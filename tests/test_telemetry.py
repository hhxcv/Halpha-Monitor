from halpha_monitor.telemetry import NetworkRequestWindow


def test_network_request_window_counts_only_recent_attempts() -> None:
    current = [100.0]
    window = NetworkRequestWindow(monotonic=lambda: current[0])

    window.record()
    current[0] = 110.0
    window.record()
    assert window.count(window_seconds=60) == 2

    current[0] = 161.0
    assert window.count(window_seconds=60) == 1
