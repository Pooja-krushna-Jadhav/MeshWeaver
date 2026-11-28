
"""Performance metrics collector and circular event log buffer."""

import collections
import time
from dataclasses import dataclass, field

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
    min_task_duration_sec: float = 0.0
    max_task_duration_sec: float = 0.0
    success_rate: float = 0.0
    failure_rate: float = 0.0
    throughput_tasks_per_sec: float = 0.0
    recent_events: list[tuple[float, str, str]] = field(default_factory=list)


class MetricsCollector:
    """Collects task performance, network counters, and event history."""

    def __init__(self, events: EventBus, max_log_history: int = 20) -> None:
        if max_log_history < 1:
            raise ValueError("max_log_history must be at least 1")

        self.events = events
        self.max_log_history = max_log_history

        self.tasks_submitted = 0
        self.tasks_completed = 0
        self.tasks_failed = 0
        self.tasks_retried = 0
        self.frames_sent = 0
        self.frames_received = 0

        self._task_durations: collections.deque[float] = collections.deque(maxlen=50)
        self._event_log: collections.deque[tuple[float, str, str]] = (
            collections.deque(maxlen=max_log_history)
        )

        self._started_at = time.monotonic()
        self._subscribe_events()

    def _subscribe_events(self) -> None:
        self.events.subscribe("*", self._on_any_event)

    def stop(self) -> None:
        """Unsubscribe from the event bus."""
        self.events.unsubscribe("*", self._on_any_event)

    def _on_any_event(self, event: Event) -> None:
        now = utc_now()
        name = event.name
        data = event.data

        if name == "task.created":
            self.tasks_submitted += 1
            summary = f"Task created: {data.get('name') or 'unnamed'}"

        elif name == "task.completed":
            self.tasks_completed += 1
            task_id = str(data.get("task_id") or "")
            summary = f"Task {task_id[:8]} completed"

            duration = data.get("execution_time_sec")
            if duration is not None:
                self.record_duration(duration)

        elif name == "task.failed":
            self.tasks_failed += 1
            task_id = str(data.get("task_id") or "")
            error = str(data.get("error") or "")
            summary = f"Task {task_id[:8]} failed: {error[:30]}"

            duration = data.get("execution_time_sec")
            if duration is not None:
                self.record_duration(duration)

        elif name == "task.retried":
            self.tasks_retried += 1
            task_id = str(data.get("task_id") or "")
            summary = f"Task {task_id[:8]} retry #{data.get('attempt', 1)}"

        elif name == "peer.connected":
            node_id = str(data.get("node_id") or "")
            summary = f"Peer connected: {node_id[:8]}"

        elif name == "heartbeat.lost":
            node_id = str(data.get("node_id") or "")
            summary = f"Peer LOST heartbeat: {node_id[:8]}"

        elif name == "gossip.received":
            sender = str(data.get("sender") or "")
            summary = f"Gossip update from {sender[:8]}"

        else:
            summary = f"Event: {name}"

        self._event_log.append((now, name, summary))

    def record_duration(self, duration_sec: float) -> None:
        """Record a valid task execution duration."""
        if isinstance(duration_sec, bool) or not isinstance(duration_sec, (int, float)):
            raise TypeError("duration_sec must be a number")

        if duration_sec < 0:
            raise ValueError("duration_sec cannot be negative")

        self._task_durations.append(float(duration_sec))

    def record_frame_sent(self, count: int = 1) -> None:
        """Record outgoing network frames."""
        if isinstance(count, bool) or not isinstance(count, int):
            raise TypeError("count must be an integer")
        if count < 0:
            raise ValueError("count cannot be negative")
        self.frames_sent += count

    def record_frame_received(self, count: int = 1) -> None:
        """Record incoming network frames."""
        if isinstance(count, bool) or not isinstance(count, int):
            raise TypeError("count must be an integer")
        if count < 0:
            raise ValueError("count cannot be negative")
        self.frames_received += count

    def snapshot(self) -> NodeMetricsSnapshot:
        """Return a snapshot of current node metrics."""
        durations = list(self._task_durations)

        avg_duration = (
            sum(durations) / len(durations) if durations else 0.0
        )
        min_duration = min(durations) if durations else 0.0
        max_duration = max(durations) if durations else 0.0

        total_finished = self.tasks_completed + self.tasks_failed

        success_rate = (
            self.tasks_completed / total_finished * 100
            if total_finished else 0.0
        )

        failure_rate = (
            self.tasks_failed / total_finished * 100
            if total_finished else 0.0
        )

        elapsed = time.monotonic() - self._started_at

        throughput = (
            total_finished / elapsed if elapsed > 0 else 0.0
        )

        return NodeMetricsSnapshot(
            total_tasks_submitted=self.tasks_submitted,
            total_tasks_completed=self.tasks_completed,
            total_tasks_failed=self.tasks_failed,
            total_tasks_retried=self.tasks_retried,
            total_frames_sent=self.frames_sent,
            total_frames_received=self.frames_received,
            avg_task_duration_sec=round(avg_duration, 3),
            min_task_duration_sec=round(min_duration, 3),
            max_task_duration_sec=round(max_duration, 3),
            success_rate=round(success_rate, 2),
            failure_rate=round(failure_rate, 2),
            throughput_tasks_per_sec=round(throughput, 3),
            recent_events=list(self._event_log),
        )
