"""Pluggable load-balancing and peer selection strategies for task scheduling."""

from abc import ABC, abstractmethod
import itertools
from typing import Sequence
from meshweaver.core.state import PeerHealth, PeerState
from meshweaver.execution.task import Task


class SchedulingStrategy(ABC):
    """Abstract base class for task scheduling strategies."""

    @abstractmethod
    def select_peer(
        self,
        task: Task,
        candidates: Sequence[PeerState],
        local_peer: PeerState | None = None,
    ) -> PeerState | None:
        """Select the best candidate peer for the given task."""
        pass


class LeastLoadedStrategy(SchedulingStrategy):
    """Select the peer with the lowest reported CPU load."""

    def select_peer(
        self,
        task: Task,
        candidates: Sequence[PeerState],
        local_peer: PeerState | None = None,
    ) -> PeerState | None:
        valid_candidates = [
            p for p in candidates
            if p.health != PeerHealth.DEAD and p.metrics.available_capacity > 0 and not p.metrics.is_overloaded
        ]
        if not valid_candidates:
            return None

        # Sort primarily by healthy status (HEALTHY before SUSPECT), then lowest CPU%, then capacity
        valid_candidates.sort(
            key=lambda p: (
                1 if p.health == PeerHealth.SUSPECT else 0,
                p.metrics.cpu_percent,
                -p.metrics.available_capacity,
            )
        )
        return valid_candidates[0]


class MultiMetricStrategy(SchedulingStrategy):
    """Select peer based on weighted composite score of CPU, RAM, queue depth, and latency."""

    def __init__(
        self,
        cpu_weight: float = 0.5,
        ram_weight: float = 0.3,
        queue_weight: float = 0.2,
    ) -> None:
        self.cpu_weight = cpu_weight
        self.ram_weight = ram_weight
        self.queue_weight = queue_weight

    def score(self, peer: PeerState) -> float:
        health_penalty = 100.0 if peer.health == PeerHealth.SUSPECT else 0.0
        cpu_ratio = peer.metrics.cpu_percent / 100.0
        ram_ratio = peer.metrics.memory_percent / 100.0
        queue_ratio = (
            peer.metrics.active_tasks / peer.metrics.max_tasks
            if peer.metrics.max_tasks > 0 else 1.0
        )
        return (
            health_penalty
            + self.cpu_weight * cpu_ratio
            + self.ram_weight * ram_ratio
            + self.queue_weight * queue_ratio
        )

    def select_peer(
        self,
        task: Task,
        candidates: Sequence[PeerState],
        local_peer: PeerState | None = None,
    ) -> PeerState | None:
        valid_candidates = [
            p for p in candidates
            if p.health != PeerHealth.DEAD and p.metrics.available_capacity > 0 and not p.metrics.is_overloaded
        ]
        if not valid_candidates:
            return None

        valid_candidates.sort(key=self.score)
        return valid_candidates[0]


class RoundRobinStrategy(SchedulingStrategy):
    """Evenly cycle task placement across available candidate peers."""

    def __init__(self) -> None:
        self._counter = 0

    def select_peer(
        self,
        task: Task,
        candidates: Sequence[PeerState],
        local_peer: PeerState | None = None,
    ) -> PeerState | None:
        valid = [
            p for p in candidates
            if p.health != PeerHealth.DEAD and p.metrics.available_capacity > 0
        ]
        if not valid:
            return None

        idx = self._counter % len(valid)
        self._counter += 1
        return valid[idx]
