
"""Unit tests for MetricsCollector and TerminalDashboard."""

import asyncio

import pytest

from meshweaver.core.config import NodeConfig
from meshweaver.core.events import (
    EventBus,
    EVENT_TASK_COMPLETED,
    EVENT_TASK_CREATED,
    EVENT_TASK_FAILED,
)
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
        await node.events.emit(EVENT_TASK_CREATED, {"name": "test.task"})
        await node.events.emit(EVENT_TASK_COMPLETED, {"task_id": "abc-123"})
        await node.events.emit(
            EVENT_TASK_FAILED,
            {"task_id": "def-456", "error": "test err"},
        )

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

        assert layout is not None
        assert layout["header"] is not None
        assert layout["body"] is not None
        assert layout["footer"] is not None

    finally:
        dashboard.metrics_collector.stop()
        await node.stop()


@pytest.mark.asyncio
async def test_extended_metrics_snapshot():
    events = EventBus()
    collector = MetricsCollector(events)

    try:
        await events.emit(EVENT_TASK_COMPLETED, {"task_id": "task-1"})
        await events.emit(EVENT_TASK_FAILED, {"task_id": "task-2"})

        collector.record_duration(0.2)
        collector.record_duration(0.4)

        collector.record_frame_sent(3)
        collector.record_frame_received(5)

        await asyncio.sleep(0.01)

        snap = collector.snapshot()

        assert snap.total_tasks_completed == 1
        assert snap.total_tasks_failed == 1
        assert snap.avg_task_duration_sec == 0.3
        assert snap.min_task_duration_sec == 0.2
        assert snap.max_task_duration_sec == 0.4
        assert snap.success_rate == 50.0
        assert snap.failure_rate == 50.0
        assert snap.total_frames_sent == 3
        assert snap.total_frames_received == 5
        assert snap.throughput_tasks_per_sec > 0

    finally:
        collector.stop()


def test_metrics_input_validation():
    collector = MetricsCollector(EventBus())

    try:
        with pytest.raises(ValueError):
            collector.record_duration(-1)

        with pytest.raises(TypeError):
            collector.record_duration("invalid")

        with pytest.raises(ValueError):
            collector.record_frame_sent(-1)

        with pytest.raises(TypeError):
            collector.record_frame_received(1.5)

    finally:
        collector.stop()
