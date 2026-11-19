"""Authorization policies and task execution permissions."""

import logging
from meshweaver.execution.task import Task, TaskType
from meshweaver.utils.exceptions import UnauthorizedError

logger = logging.getLogger("meshweaver.permissions")


class AuthorizationPolicy:
    """Enforces node whitelisting and task execution permissions."""

    def __init__(
        self,
        allowed_nodes: list[str] | None = None,
        allow_cloudpickle: bool = False,
        admin_nodes: list[str] | None = None,
    ) -> None:
        self.allowed_nodes = set(allowed_nodes) if allowed_nodes else set()
        self.allow_cloudpickle = allow_cloudpickle
        self.admin_nodes = set(admin_nodes) if admin_nodes else set()

    def check_node_allowed(self, node_id: str) -> None:
        """Verify that the sender node is in the allowed whitelist (if whitelist configured)."""
        if self.allowed_nodes and node_id not in self.allowed_nodes:
            raise UnauthorizedError(f"Node {node_id[:8]} is not authorized to communicate with this mesh")

    def check_task_permission(self, sender_id: str, task: Task) -> None:
        """Validate whether the sender has permission to execute the specified task."""
        self.check_node_allowed(sender_id)

        if task.task_type == TaskType.FUNCTION:
            if not self.allow_cloudpickle:
                raise UnauthorizedError("Arbitrary function execution via cloudpickle is globally disabled")
            if self.admin_nodes and sender_id not in self.admin_nodes:
                raise UnauthorizedError(
                    f"Node {sender_id[:8]} lacks ADMIN privilege to submit serialized functions"
                )
