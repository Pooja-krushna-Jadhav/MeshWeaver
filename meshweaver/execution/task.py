"""Task models, lifecycle state machine, and priority definitions."""

from dataclasses import dataclass, field
from enum import Enum, IntEnum
import uuid
from typing import Any
from meshweaver.utils.exceptions import ProtocolError
from meshweaver.utils.helpers import utc_now


class TaskState(str, Enum):
    CREATED = "CREATED"
    QUEUED = "QUEUED"
    ASSIGNED = "ASSIGNED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    RETRYING = "RETRYING"
    CANCELLED = "CANCELLED"


class TaskPriority(IntEnum):
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


class TaskType(str, Enum):
    REGISTERED = "REGISTERED"   # Safe predefined handler with structured JSON arguments
    FUNCTION = "FUNCTION"       # Arbitrary Python callable (cloudpickle; requires authorization)


VALID_TRANSITIONS: dict[TaskState, set[TaskState]] = {
    TaskState.CREATED: {TaskState.QUEUED, TaskState.ASSIGNED, TaskState.CANCELLED, TaskState.FAILED},
    TaskState.QUEUED: {TaskState.ASSIGNED, TaskState.CANCELLED, TaskState.FAILED},
    TaskState.ASSIGNED: {TaskState.RUNNING, TaskState.COMPLETED, TaskState.RETRYING, TaskState.FAILED, TaskState.CANCELLED},
    TaskState.RUNNING: {TaskState.COMPLETED, TaskState.FAILED, TaskState.RETRYING, TaskState.CANCELLED},
    TaskState.RETRYING: {TaskState.RETRYING, TaskState.ASSIGNED, TaskState.QUEUED, TaskState.RUNNING, TaskState.COMPLETED, TaskState.FAILED, TaskState.CANCELLED},
    TaskState.COMPLETED: set(),  # Terminal state
    TaskState.FAILED: {TaskState.RETRYING},  # Can be retried by scheduler
    TaskState.CANCELLED: set(),   # Terminal state
}


@dataclass
class Task:
    name: str
    payload: dict[str, Any] = field(default_factory=dict)
    task_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    task_type: TaskType = TaskType.REGISTERED
    origin_node_id: str = ""
    assigned_node_id: str | None = None
    priority: TaskPriority = TaskPriority.NORMAL
    state: TaskState = TaskState.CREATED
    timeout: float = 60.0
    retry_count: int = 0
    max_retries: int = 3
    created_at: float = field(default_factory=utc_now)
    started_at: float | None = None
    completed_at: float | None = None
    execution_time_sec: float | None = None
    result: Any | None = None
    error: str | None = None
    state_history: list[tuple[float, TaskState, str]] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.state_history:
            self.state_history.append((self.created_at, self.state, "Task created"))

    def transition_to(self, new_state: TaskState, reason: str = "") -> None:
        """Enforce valid task state transitions."""
        if self.state == new_state:
            if reason:
                self.state_history.append((utc_now(), new_state, reason))
            return
        if new_state not in VALID_TRANSITIONS.get(self.state, set()):
            raise ProtocolError(
                f"Invalid task state transition from {self.state.value} to {new_state.value}"
            )
        now = utc_now()
        self.state = new_state
        self.state_history.append((now, new_state, reason))

        if new_state == TaskState.RUNNING and self.started_at is None:
            self.started_at = now
        elif new_state in (TaskState.COMPLETED, TaskState.FAILED, TaskState.CANCELLED):
            self.completed_at = now
            if self.started_at is not None:
                self.execution_time_sec = now - self.started_at

    @property
    def is_terminal(self) -> bool:
        return self.state in (TaskState.COMPLETED, TaskState.CANCELLED)

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "name": self.name,
            "payload": self.payload,
            "task_type": self.task_type.value,
            "origin_node_id": self.origin_node_id,
            "assigned_node_id": self.assigned_node_id,
            "priority": int(self.priority),
            "state": self.state.value,
            "timeout": self.timeout,
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "execution_time_sec": self.execution_time_sec,
            "result": self.result,
            "error": self.error,
            "state_history": [(t, s.value if isinstance(s, TaskState) else str(s), r) for t, s, r in self.state_history],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Task":
        data = data.copy()
        data["task_type"] = TaskType(data.get("task_type", TaskType.REGISTERED.value))
        data["state"] = TaskState(data.get("state", TaskState.CREATED.value))
        data["priority"] = TaskPriority(data.get("priority", TaskPriority.NORMAL.value))
        raw_history = data.pop("state_history", [])
        history = [(t, TaskState(s), r) for t, s, r in raw_history]
        task = cls(**data)
        if history:
            task.state_history = history
        return task
