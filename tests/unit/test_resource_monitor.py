"""Unit tests for the zero-dependency ResourceMonitor."""

from meshweaver.gossip.resource_monitor import ResourceMonitor


def test_resource_monitor_sample():
    monitor = ResourceMonitor(max_tasks=10)
    metrics = monitor.sample(active_tasks=2)

    assert metrics.max_tasks == 10
    assert metrics.active_tasks == 2
    assert metrics.available_capacity == 8
    assert metrics.memory_total_mb > 0
    assert metrics.memory_available_mb > 0
    assert 0.0 <= metrics.memory_percent <= 100.0
    assert 0.0 <= metrics.cpu_percent <= 100.0
    assert metrics.timestamp > 0
