"""Distributed Task Scheduler coordinating peer selection and task dispatch."""

import asyncio
from dataclasses import replace
import logging
from typing import Any, Callable, Coroutine
from meshweaver.core.config import SchedulerConfig
from meshweaver.core.events import EVENT_TASK_ASSIGNED, EVENT_TASK_QUEUED, EventBus
from meshweaver.core.state import NodeInfo, PeerHealth, PeerState, ResourceMetrics
from meshweaver.execution.results import TaskResult
from meshweaver.execution.task import Task, TaskState
from meshweaver.gossip.peer_state import PeerTable
from meshweaver.gossip.resource_monitor import ResourceMonitor
from meshweaver.scheduler.load_balancer import (
    LeastLoadedStrategy,
    MultiMetricStrategy,
    RoundRobinStrategy,
    SchedulingStrategy,
)
from meshweaver.utils.exceptions import SchedulingError

logger = logging.getLogger("meshweaver.scheduler")


class TaskScheduler:
    """Intelligent load-aware distributed task scheduler."""

    def __init__(
        self,
        node_id: str,
        node_info: NodeInfo,
        peer_table: PeerTable,
        resource_monitor: ResourceMonitor,
        events: EventBus,
        config: SchedulerConfig | None = None,
    ) -> None:
        self.node_id = node_id
        self.node_info = node_info
        self.peer_table = peer_table
        self.resource_monitor = resource_monitor
        self.events = events
        self.config = config or SchedulerConfig()

        self.strategy = self._create_strategy(self.config.strategy)
        self._in_flight_reservations: dict[str, int] = {}

    def reserve_peer(self, node_id: str) -> None:
        """Optimistically reserve one in-flight task slot on a peer."""
        self._in_flight_reservations[node_id] = self._in_flight_reservations.get(node_id, 0) + 1

    def release_peer(self, node_id: str) -> None:
        """Release one optimistic in-flight task slot reservation on a peer."""
        count = self._in_flight_reservations.get(node_id, 0)
        if count <= 1:
            self._in_flight_reservations.pop(node_id, None)
        else:
            self._in_flight_reservations[node_id] = count - 1

    def _create_strategy(self, strategy_name: str) -> SchedulingStrategy:
        if strategy_name == "least_loaded":
            return LeastLoadedStrategy()
        elif strategy_name == "round_robin":
            return RoundRobinStrategy()
        else:
            return MultiMetricStrategy(
                cpu_weight=self.config.cpu_weight,
                ram_weight=self.config.ram_weight,
                queue_weight=self.config.queue_weight,
            )

    def get_local_peer_state(self, active_tasks: int = 0) -> PeerState:
        """Construct a live PeerState representation of this local node."""
        metrics = self.resource_monitor.sample(active_tasks=active_tasks)
        return PeerState(
            info=self.node_info,
            metrics=metrics,
            health=PeerHealth.HEALTHY,
        )

    def select_peer_for_task(
        self,
        task: Task,
        active_local_tasks: int = 0,
        excluded_peers: set[str] | None = None,
    ) -> tuple[PeerState | None, bool]:
        """Select optimal peer to execute task. Returns (selected_peer, is_local)."""
        local_peer = self.get_local_peer_state(active_tasks=active_local_tasks)

        # Include local peer plus all known available remote peers
        raw_candidates: list[PeerState] = [local_peer]
        raw_candidates.extend(self.peer_table.available_peers())

        # Filter out duplicates of self and excluded peers
        raw_candidates = [
            p for p in raw_candidates
            if (p.info.node_id != self.node_id or p is local_peer)
            and (not excluded_peers or p.info.node_id not in excluded_peers)
        ]

        # Apply optimistic in-flight reservations without mutating shared peer_table state
        orig_by_id: dict[str, PeerState] = {}
        adjusted_candidates: list[PeerState] = []
        for p in raw_candidates:
            orig_by_id[p.info.node_id] = p
            reserved = self._in_flight_reservations.get(p.info.node_id, 0)
            if reserved > 0:
                adjusted_metrics = replace(p.metrics, active_tasks=p.metrics.active_tasks + reserved)
                adjusted_candidates.append(replace(p, metrics=adjusted_metrics))
            else:
                adjusted_candidates.append(p)

        selected = self.strategy.select_peer(task, adjusted_candidates, local_peer=local_peer)
        if not selected:
            return (None, False)

        is_local = (selected.info.node_id == self.node_id)
        return (orig_by_id[selected.info.node_id], is_local)
