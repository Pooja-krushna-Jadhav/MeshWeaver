"""Unit tests for task lifecycle state machine."""

import pytest
from meshweaver.execution.task import Task, TaskPriority, TaskState, TaskType
from meshweaver.utils.exceptions import ProtocolError


def test_task_creation_and_defaults():
    task = Task(name="math.add", payload={"args": [5, 7]})
    assert task.state == TaskState.CREATED
    assert task.priority == TaskPriority.NORMAL
    assert task.task_type == TaskType.REGISTERED
    assert len(task.state_history) == 1


def test_task_valid_state_transitions():
    task = Task(name="test")
    task.transition_to(TaskState.QUEUED, "Queued in memory")
    assert task.state == TaskState.QUEUED

    task.transition_to(TaskState.ASSIGNED, "Assigned to worker")
    assert task.state == TaskState.ASSIGNED

    task.transition_to(TaskState.RUNNING, "Starting")
    assert task.state == TaskState.RUNNING
    assert task.started_at is not None

    task.transition_to(TaskState.COMPLETED, "Done")
    assert task.state == TaskState.COMPLETED
    assert task.completed_at is not None
    assert task.execution_time_sec is not None
    assert task.is_terminal is True


def test_task_invalid_state_transition():
    task = Task(name="test")
    # Cannot jump directly from CREATED to COMPLETED
    with pytest.raises(ProtocolError):
        task.transition_to(TaskState.COMPLETED)


def test_task_serialization_roundtrip():
    task = Task(name="math.multiply", payload={"args": [3, 4]}, priority=TaskPriority.HIGH)
    data = task.to_dict()

    restored = Task.from_dict(data)
    assert restored.task_id == task.task_id
    assert restored.name == "math.multiply"
    assert restored.payload == {"args": [3, 4]}
    assert restored.priority == TaskPriority.HIGH
    assert restored.state == TaskState.CREATED
