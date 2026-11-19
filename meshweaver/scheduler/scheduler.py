"""Distributed Task Scheduler coordinating peer selection and task dispatch."""

import asyncio
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

    def select_peer_for_task(self, task: Task, active_local_tasks: int = 0) -> tuple[PeerState | None, bool]:
        """Select optimal peer to execute task. Returns (selected_peer, is_local)."""
        local_peer = self.get_local_peer_state(active_tasks=active_local_tasks)

        # Include local peer plus all known available remote peers
        candidates: list[PeerState] = [local_peer]
        candidates.extend(self.peer_table.available_peers())

        # Filter out duplicates of self in peer table if any
        candidates = [p for p in candidates if p.info.node_id != self.node_id or p is local_peer]

        selected = self.strategy.select_peer(task, candidates, local_peer=local_peer)
        if not selected:
            return (None, False)

        is_local = (selected.info.node_id == self.node_id)
        return (selected, is_local)
