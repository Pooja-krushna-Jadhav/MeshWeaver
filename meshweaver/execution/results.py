"""Task execution result models."""

from dataclasses import dataclass, field
from typing import Any
from meshweaver.execution.task import TaskState
from meshweaver.utils.helpers import utc_now


@dataclass
class TaskResult:
    task_id: str
    state: TaskState
    result: Any | None = None
    error: str | None = None
    execution_time_sec: float | None = None
    worker_node_id: str = ""
    completed_at: float = field(default_factory=utc_now)

    @property
    def is_success(self) -> bool:
        return self.state == TaskState.COMPLETED

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "state": self.state.value if isinstance(self.state, TaskState) else str(self.state),
            "result": self.result,
            "error": self.error,
            "execution_time_sec": self.execution_time_sec,
            "worker_node_id": self.worker_node_id,
            "completed_at": self.completed_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TaskResult":
        data = data.copy()
        data["state"] = TaskState(data["state"])
        return cls(**data)
