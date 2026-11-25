"""Automatic task failover, re-routing, and recovery coordinator."""

import asyncio
from collections.abc import KeysView
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
from meshweaver.utils.exceptions import ProtocolError

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
        cancel_inflight_fn: Callable[[str, str], None] | None = None,
        resolve_task_fn: Callable[[str, TaskResult | None, Exception | None], None] | None = None,
    ) -> None:
        self.node_id = node_id
        self.tasks_map = tasks_map
        self.events = events
        self.reschedule_task = reschedule_task_fn
        self.retry_policy = retry_policy or RetryPolicy()
        self.cancel_inflight_fn = cancel_inflight_fn
        self.resolve_task_fn = resolve_task_fn

        self._recovery_tasks: set[asyncio.Task[Any]] = set()
        self._recovering_tasks: dict[str, str] = {}  # task_id -> dead_node_id
        self._active_recovery_by_task: dict[str, asyncio.Task[Any]] = {}

        # Listen for lost heartbeat events
        self.events.subscribe(EVENT_HEARTBEAT_LOST, self._on_heartbeat_lost)

    @property
    def _recovering_task_ids(self) -> KeysView[str]:
        return self._recovering_tasks.keys()

    async def stop(self) -> None:
        """Unsubscribe from events and cancel all active recovery tasks."""
        self.events.unsubscribe(EVENT_HEARTBEAT_LOST, self._on_heartbeat_lost)
        for t in list(self._recovery_tasks):
            t.cancel()
        if self._recovery_tasks:
            await asyncio.gather(*self._recovery_tasks, return_exceptions=True)
            self._recovery_tasks.clear()
        self._recovering_tasks.clear()
        self._active_recovery_by_task.clear()

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
            if t.assigned_node_id == dead_node_id
            and t.state in (TaskState.ASSIGNED, TaskState.RUNNING, TaskState.QUEUED)
            and not t.is_terminal
            and self._recovering_tasks.get(t.task_id) != dead_node_id
        ]

        if not affected_tasks:
            return

        logger.info("Found %d affected task(s) for dead peer %s. Initiating recovery.", len(affected_tasks), dead_node_id[:8])

        for task in affected_tasks:
            prev_rtask = self._active_recovery_by_task.get(task.task_id)
            if prev_rtask is not None and not prev_rtask.done():
                prev_rtask.cancel()
            if self.cancel_inflight_fn:
                try:
                    self.cancel_inflight_fn(task.task_id, dead_node_id)
                except Exception as e:
                    logger.debug("cancel_inflight_fn failed for task %s: %s", task.task_id[:8], e)
            self._recovering_tasks[task.task_id] = dead_node_id
            rtask = asyncio.create_task(self._recover_task(task, dead_node_id))
            self._active_recovery_by_task[task.task_id] = rtask
            self._recovery_tasks.add(rtask)

            def _cleanup(t: asyncio.Task[Any], tid: str = task.task_id) -> None:
                self._recovery_tasks.discard(t)
                if self._active_recovery_by_task.get(tid) is t:
                    self._active_recovery_by_task.pop(tid, None)
                    self._recovering_tasks.pop(tid, None)

            rtask.add_done_callback(_cleanup)

    async def _recover_task(self, task: Task, dead_node_id: str) -> None:
        if task.is_terminal or task.state in (TaskState.COMPLETED, TaskState.CANCELLED):
            return

        if task.retry_count < task.max_retries:
            if task.is_terminal or task.state in (TaskState.COMPLETED, TaskState.CANCELLED):
                return
            task.retry_count += 1
            delay = self.retry_policy.get_delay(task.retry_count)
            logger.info(
                "Recovering task %s (attempt %d/%d) in %.2fs...",
                task.task_id[:8],
                task.retry_count,
                task.max_retries,
                delay,
            )
            try:
                task.transition_to(
                    TaskState.RETRYING,
                    reason=f"Peer {dead_node_id[:8]} offline; reassignment attempt {task.retry_count}",
                )
            except ProtocolError as e:
                logger.warning("Skipping recovery for task %s: %s", task.task_id[:8], e)
                return
            await self.events.emit(EVENT_TASK_RETRIED, {"task_id": task.task_id, "attempt": task.retry_count})

            await asyncio.sleep(delay)

            if task.is_terminal or task.state in (TaskState.COMPLETED, TaskState.CANCELLED):
                return

            try:
                # Re-submit to mesh (will select a different, alive node)
                result = await self.reschedule_task(task)
                logger.info(
                    "Task %s successfully recovered on peer %s",
                    task.task_id[:8],
                    task.assigned_node_id[:8] if task.assigned_node_id else "unknown",
                )
                if self.resolve_task_fn:
                    self.resolve_task_fn(task.task_id, result, None)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("Failed to re-route task %s during recovery: %s", task.task_id[:8], e)
                if self.resolve_task_fn:
                    self.resolve_task_fn(task.task_id, None, e)
        else:
            err_msg = f"Task failed: assigned peer {dead_node_id[:8]} died and max retries ({task.max_retries}) exhausted"
            logger.error(err_msg)
            task.error = err_msg
            try:
                task.transition_to(TaskState.FAILED, reason=err_msg)
            except ProtocolError as e:
                logger.warning("Skipping failure transition for task %s: %s", task.task_id[:8], e)
                return
            await self.events.emit(EVENT_TASK_FAILED, {"task_id": task.task_id, "error": err_msg})
            if self.resolve_task_fn:
                self.resolve_task_fn(
                    task.task_id,
                    TaskResult(
                        task_id=task.task_id,
                        state=TaskState.FAILED,
                        error=err_msg,
                        worker_node_id=dead_node_id,
                    ),
                    None,
                )
