"""Adaptive failure detector tracking peer liveness and heartbeat intervals."""

import collections
import logging
import math
from meshweaver.core.state import PeerHealth
from meshweaver.utils.helpers import utc_now

logger = logging.getLogger("meshweaver.failure_detector")


class FailureDetector:
    """Monitors heartbeat arrival intervals and determines peer health states."""

    def __init__(
        self,
        suspect_timeout: float = 4.0,
        dead_timeout: float = 8.0,
        history_window_size: int = 16,
    ) -> None:
        self.suspect_timeout = suspect_timeout
        self.dead_timeout = dead_timeout
        self.window_size = history_window_size

        # node_id -> deque of inter-arrival intervals
        self._intervals: dict[str, collections.deque[float]] = {}
        # node_id -> last heartbeat timestamp
        self._last_heartbeat: dict[str, float] = {}

    def record_heartbeat(self, node_id: str) -> None:
        """Record successful heartbeat receipt."""
        now = utc_now()
        prev = self._last_heartbeat.get(node_id)
        self._last_heartbeat[node_id] = now

        if prev is not None:
            interval = now - prev
            if node_id not in self._intervals:
                self._intervals[node_id] = collections.deque(maxlen=self.window_size)
            self._intervals[node_id].append(interval)

    def record_failure(self, node_id: str) -> None:
        """Explicitly record a communication failure to accelerate detection."""
        # Shift last heartbeat backwards to trigger suspect/dead
        if node_id in self._last_heartbeat:
            self._last_heartbeat[node_id] -= self.suspect_timeout

    def get_health(self, node_id: str) -> PeerHealth:
        """Determine peer health status based on elapsed time since last heartbeat."""
        last = self._last_heartbeat.get(node_id)
        if last is None:
            return PeerHealth.DEAD

        elapsed = utc_now() - last
        if elapsed > self.dead_timeout:
            return PeerHealth.DEAD
        elif elapsed > self.suspect_timeout:
            return PeerHealth.SUSPECT
        return PeerHealth.HEALTHY

    def remove_peer(self, node_id: str) -> None:
        self._intervals.pop(node_id, None)
        self._last_heartbeat.pop(node_id, None)
