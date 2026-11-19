"""Serialization routines for tasks, parameters, and optional callables."""

import base64
import logging
from typing import Any, Callable
from meshweaver.execution.task import Task, TaskType
from meshweaver.utils.exceptions import SecurityError, SerializationError
from meshweaver.utils.helpers import json_dumps, json_loads

logger = logging.getLogger("meshweaver.serialization")

try:
    import cloudpickle
    HAS_CLOUDPICKLE = True
except ImportError:
    cloudpickle = None  # type: ignore
    HAS_CLOUDPICKLE = False


def serialize_task(task: Task, allow_cloudpickle: bool = False) -> dict[str, Any]:
    """Serialize a Task into a dictionary suitable for wire transmission."""
    data = task.to_dict()
    if task.task_type == TaskType.FUNCTION:
        if not allow_cloudpickle:
            raise SecurityError("Cloudpickle serialization is disabled on this node for security")
        if not HAS_CLOUDPICKLE:
            raise SerializationError("cloudpickle library is required for FUNCTION tasks")
    return data


def deserialize_task(data: dict[str, Any], allow_cloudpickle: bool = False) -> Task:
    """Deserialize a dictionary into a Task, validating security boundaries."""
    task = Task.from_dict(data)
    if task.task_type == TaskType.FUNCTION and not allow_cloudpickle:
        raise SecurityError(
            "Rejecting incoming FUNCTION task: cloudpickle execution is disabled on this node for security"
        )
    return task


def serialize_callable(fn: Callable[..., Any]) -> str:
    """Serialize a Python callable to base64-encoded cloudpickle string."""
    if not HAS_CLOUDPICKLE:
        raise SerializationError("cloudpickle is not available")
    pickled = cloudpickle.dumps(fn)
    return base64.b64encode(pickled).decode("ascii")


def deserialize_callable(data_b64: str) -> Callable[..., Any]:
    """Deserialize a base64-encoded cloudpickle string to a Python callable."""
    if not HAS_CLOUDPICKLE:
        raise SerializationError("cloudpickle is not available")
    pickled = base64.b64decode(data_b64.encode("ascii"))
    return cloudpickle.loads(pickled)
