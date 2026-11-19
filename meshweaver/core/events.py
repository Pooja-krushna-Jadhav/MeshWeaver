"""Asynchronous Event Bus for intra-node subsystem communication."""

import asyncio
from collections import defaultdict
from dataclasses import dataclass, field
import logging
from typing import Any, Callable, Coroutine

logger = logging.getLogger("meshweaver.events")


# Standard event name constants
EVENT_PEER_CONNECTED = "peer.connected"
EVENT_PEER_DISCONNECTED = "peer.disconnected"
EVENT_PEER_UPDATED = "peer.updated"
EVENT_TASK_CREATED = "task.created"
EVENT_TASK_QUEUED = "task.queued"
EVENT_TASK_ASSIGNED = "task.assigned"
EVENT_TASK_STARTED = "task.started"
EVENT_TASK_COMPLETED = "task.completed"
EVENT_TASK_FAILED = "task.failed"
EVENT_TASK_RETRIED = "task.retried"
EVENT_TASK_CANCELLED = "task.cancelled"
EVENT_GOSSIP_RECEIVED = "gossip.received"
EVENT_HEARTBEAT_LOST = "heartbeat.lost"
EVENT_NODE_STATE_CHANGED = "node.state_changed"


@dataclass
class Event:
    name: str
    data: dict[str, Any] = field(default_factory=dict)
    source: str = ""


HandlerType = Callable[[Event], Coroutine[Any, Any, None] | None]


class EventBus:
    """Asynchronous event bus supporting non-blocking publish/subscribe."""

    def __init__(self) -> None:
        self._subscribers: dict[str, list[HandlerType]] = defaultdict(list)
        self._wildcard_subscribers: list[HandlerType] = []

    def subscribe(self, event_name: str, handler: HandlerType) -> None:
        """Register a handler for a specific event or wildcard '*'."""
        if event_name == "*":
            if handler not in self._wildcard_subscribers:
                self._wildcard_subscribers.append(handler)
        else:
            if handler not in self._subscribers[event_name]:
                self._subscribers[event_name].append(handler)

    def unsubscribe(self, event_name: str, handler: HandlerType) -> None:
        """Remove a previously registered handler."""
        if event_name == "*":
            if handler in self._wildcard_subscribers:
                self._wildcard_subscribers.remove(handler)
        elif event_name in self._subscribers:
            if handler in self._subscribers[event_name]:
                self._subscribers[event_name].remove(handler)

    async def emit(self, event_name: str, data: dict[str, Any] | None = None, source: str = "") -> None:
        """Publish an event to all matching subscribers asynchronously."""
        event = Event(name=event_name, data=data or {}, source=source)
        handlers = list(self._subscribers.get(event_name, [])) + list(self._wildcard_subscribers)

        for handler in handlers:
            try:
                res = handler(event)
                if asyncio.iscoroutine(res):
                    await res
            except Exception as e:
                logger.error("Error in event handler %s for event %s: %s", handler, event_name, e, exc_info=True)

    def emit_nowait(self, event_name: str, data: dict[str, Any] | None = None, source: str = "") -> None:
        """Fire-and-forget emit scheduled on the running asyncio loop."""
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.emit(event_name, data, source))
        except RuntimeError:
            # If no running event loop, ignore or log
            logger.debug("No running event loop to emit %s", event_name)
