"""Unit tests for MetricsCollector and TerminalDashboard."""

import pytest
from meshweaver.core.config import NodeConfig
from meshweaver.core.events import EVENT_TASK_COMPLETED, EVENT_TASK_CREATED, EVENT_TASK_FAILED
from meshweaver.core.node import Node
from meshweaver.monitoring.dashboard import TerminalDashboard
from meshweaver.monitoring.metrics import MetricsCollector


@pytest.mark.asyncio
async def test_metrics_collector():
    cfg = NodeConfig(name="metric-node")
    cfg.network.tcp_port = 0
    cfg.network.udp_port = 0
    node = Node(cfg)
    collector = MetricsCollector(node.events)
    try:
        # Emit task events
        await node.events.emit(EVENT_TASK_CREATED, {"name": "test.task"})
        await node.events.emit(EVENT_TASK_COMPLETED, {"task_id": "abc-123"})
        await node.events.emit(EVENT_TASK_FAILED, {"task_id": "def-456", "error": "test err"})

        collector.record_duration(0.25)
        collector.record_duration(0.35)

        snap = collector.snapshot()
        assert snap.total_tasks_submitted == 1
        assert snap.total_tasks_completed == 1
        assert snap.total_tasks_failed == 1
        assert snap.avg_task_duration_sec == 0.3
        assert len(snap.recent_events) >= 3
    finally:
        collector.stop()
        await node.stop()


@pytest.mark.asyncio
async def test_terminal_dashboard_render_layout():
    cfg = NodeConfig(name="dash-node")
    cfg.network.tcp_port = 0
    cfg.network.udp_port = 0
    node = Node(cfg)

    dashboard = TerminalDashboard(node)
    try:
        layout = dashboard.render_layout()

        # Verify layout contains required panels
        assert layout is not None
        assert layout["header"] is not None
        assert layout["body"] is not None
        assert layout["footer"] is not None
    finally:
        dashboard.metrics_collector.stop()
        await node.stop()
