"""Performance metrics collector and circular event log buffer."""

import collections
from dataclasses import dataclass, field
import time
from typing import Any
from meshweaver.core.events import Event, EventBus
from meshweaver.utils.helpers import utc_now


@dataclass
class NodeMetricsSnapshot:
    total_tasks_submitted: int = 0
    total_tasks_completed: int = 0
    total_tasks_failed: int = 0
    total_tasks_retried: int = 0
    total_frames_sent: int = 0
    total_frames_received: int = 0
    avg_task_duration_sec: float = 0.0
    recent_events: list[tuple[float, str, str]] = field(default_factory=list)


class MetricsCollector:
    """Collects throughput, latency, and circular event feed for monitoring."""

    def __init__(self, events: EventBus, max_log_history: int = 20) -> None:
        self.events = events
        self.max_log_history = max_log_history

        self.tasks_submitted = 0
        self.tasks_completed = 0
        self.tasks_failed = 0
        self.tasks_retried = 0
        self.frames_sent = 0
        self.frames_received = 0

        self._task_durations: collections.deque[float] = collections.deque(maxlen=50)
        self._event_log: collections.deque[tuple[float, str, str]] = collections.deque(maxlen=max_log_history)

        self._subscribe_events()

    def _subscribe_events(self) -> None:
        self.events.subscribe("*", self._on_any_event)

    def _on_any_event(self, event: Event) -> None:
        now = utc_now()
        name = event.name
        summary = ""

        if name == "task.created":
            self.tasks_submitted += 1
            summary = f"Task created: {event.data.get('name', 'unnamed')}"
        elif name == "task.completed":
            self.tasks_completed += 1
            summary = f"Task {event.data.get('task_id', '')[:8]} completed"
        elif name == "task.failed":
            self.tasks_failed += 1
            summary = f"Task {event.data.get('task_id', '')[:8]} failed: {event.data.get('error', '')[:30]}"
        elif name == "task.retried":
            self.tasks_retried += 1
            summary = f"Task {event.data.get('task_id', '')[:8]} retry #{event.data.get('attempt', 1)}"
        elif name == "peer.connected":
            summary = f"Peer connected: {event.data.get('node_id', '')[:8]}"
        elif name == "heartbeat.lost":
            summary = f"Peer LOST heartbeat: {event.data.get('node_id', '')[:8]}"
        elif name == "gossip.received":
            summary = f"Gossip update from {event.data.get('sender', '')[:8]}"
        else:
            summary = f"Event: {name}"

        self._event_log.append((now, name, summary))

    def record_duration(self, duration_sec: float) -> None:
        self._task_durations.append(duration_sec)

    def snapshot(self) -> NodeMetricsSnapshot:
        avg_dur = sum(self._task_durations) / len(self._task_durations) if self._task_durations else 0.0
        return NodeMetricsSnapshot(
            total_tasks_submitted=self.tasks_submitted,
            total_tasks_completed=self.tasks_completed,
            total_tasks_failed=self.tasks_failed,
            total_tasks_retried=self.tasks_retried,
            total_frames_sent=self.frames_sent,
            total_frames_received=self.frames_received,
            avg_task_duration_sec=round(avg_dur, 3),
            recent_events=list(self._event_log),
        )
