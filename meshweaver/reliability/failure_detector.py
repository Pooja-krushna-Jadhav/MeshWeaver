"""Adaptive failure detector tracking peer liveness and heartbeat intervals."""

import collections
import logging
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
        dead_misses: int = 3,
    ) -> None:
        self.suspect_timeout = suspect_timeout
        self.dead_timeout = dead_timeout
        self.window_size = history_window_size
        self.dead_misses = dead_misses

        # node_id -> deque of inter-arrival intervals
        self._intervals: dict[str, collections.deque[float]] = {}
        # node_id -> last heartbeat timestamp
        self._last_heartbeat: dict[str, float] = {}
        # node_id -> consecutive missed probes
        self._miss_counts: dict[str, int] = {}

    def register_peer(self, node_id: str) -> None:
        """Register a known peer before probing so initial failure counts track properly."""
        if node_id not in self._last_heartbeat:
            self._last_heartbeat[node_id] = utc_now()
            self._miss_counts.setdefault(node_id, 0)

    def record_heartbeat(self, node_id: str) -> None:
        """Record successful heartbeat receipt."""
        now = utc_now()
        prev = self._last_heartbeat.get(node_id)
        self._last_heartbeat[node_id] = now
        self._miss_counts[node_id] = 0

        if prev is not None:
            interval = now - prev
            if node_id not in self._intervals:
                self._intervals[node_id] = collections.deque(maxlen=self.window_size)
            self._intervals[node_id].append(interval)

    def record_failure(self, node_id: str) -> None:
        """Explicitly record a communication failure to accelerate detection."""
        if node_id not in self._last_heartbeat:
            self.register_peer(node_id)
        self._miss_counts[node_id] = self._miss_counts.get(node_id, 0) + 1

    def get_health(self, node_id: str) -> PeerHealth:
        """Determine peer health status based on missed probes and elapsed time."""
        last = self._last_heartbeat.get(node_id)
        if last is None:
            return PeerHealth.DEAD

        misses = self._miss_counts.get(node_id, 0)
        elapsed = utc_now() - last
        if misses >= self.dead_misses or elapsed > self.dead_timeout:
            return PeerHealth.DEAD
        elif misses >= 1 or elapsed > self.suspect_timeout:
            return PeerHealth.SUSPECT
        return PeerHealth.HEALTHY

    def remove_peer(self, node_id: str) -> None:
        self._intervals.pop(node_id, None)
        self._last_heartbeat.pop(node_id, None)
        self._miss_counts.pop(node_id, None)
