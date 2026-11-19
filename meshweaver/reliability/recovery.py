"""Automatic task failover, re-routing, and recovery coordinator."""

import asyncio
import logging
from typing import Any, Callable, Coroutine
from meshweaver.core.events import (
    EVENT_HEARTBEAT_LOST,
    EVENT_TASK_FAILED,
    EVENT_TASK_RETRIED,
    Event,
    EventBus,
)
from meshweaver.execution.results import TaskResult
from meshweaver.execution.task import Task, TaskState
from meshweaver.reliability.retry import RetryPolicy

logger = logging.getLogger("meshweaver.recovery")


class RecoveryManager:
    """Detects lost peer assignments and automatically reschedules tasks to surviving peers."""

    def __init__(
        self,
        node_id: str,
        tasks_map: dict[str, Task],
        events: EventBus,
        reschedule_task_fn: Callable[[Task], Coroutine[Any, Any, TaskResult]],
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        self.node_id = node_id
        self.tasks_map = tasks_map
        self.events = events
        self.reschedule_task = reschedule_task_fn
        self.retry_policy = retry_policy or RetryPolicy()

        # Listen for lost heartbeat events
        self.events.subscribe(EVENT_HEARTBEAT_LOST, self._on_heartbeat_lost)

    async def _on_heartbeat_lost(self, event: Event) -> None:
        dead_node_id = event.data.get("node_id")
        if not dead_node_id:
            return

        logger.warning(
            "Node %s lost heartbeat. Checking in-flight tasks for failover...",
            dead_node_id[:8],
        )

        affected_tasks = [
            t for t in self.tasks_map.values()
            if t.assigned_node_id == dead_node_id and not t.is_terminal and t.state != TaskState.RETRYING
        ]

        if not affected_tasks:
            return

        logger.info("Found %d affected task(s) for dead peer %s. Initiating recovery.", len(affected_tasks), dead_node_id[:8])

        for task in affected_tasks:
            asyncio.create_task(self._recover_task(task, dead_node_id))

    async def _recover_task(self, task: Task, dead_node_id: str) -> None:
        if task.retry_count < task.max_retries:
            task.retry_count += 1
            delay = self.retry_policy.get_delay(task.retry_count)
            logger.info(
                "Recovering task %s (attempt %d/%d) in %.2fs...",
                task.task_id[:8],
                task.retry_count,
                task.max_retries,
                delay,
            )
            task.transition_to(
                TaskState.RETRYING,
                reason=f"Peer {dead_node_id[:8]} offline; reassignment attempt {task.retry_count}",
            )
            await self.events.emit(EVENT_TASK_RETRIED, {"task_id": task.task_id, "attempt": task.retry_count})

            await asyncio.sleep(delay)

            try:
                # Re-submit to mesh (will select a different, alive node)
                result = await self.reschedule_task(task)
                logger.info(
                    "Task %s successfully recovered on peer %s",
                    task.task_id[:8],
                    task.assigned_node_id[:8] if task.assigned_node_id else "unknown",
                )
            except Exception as e:
                logger.error("Failed to re-route task %s during recovery: %s", task.task_id[:8], e)
        else:
            err_msg = f"Task failed: assigned peer {dead_node_id[:8]} died and max retries ({task.max_retries}) exhausted"
            logger.error(err_msg)
            task.error = err_msg
            task.transition_to(TaskState.FAILED, reason=err_msg)
            await self.events.emit(EVENT_TASK_FAILED, {"task_id": task.task_id, "error": err_msg})
