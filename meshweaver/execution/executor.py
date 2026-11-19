"""Asynchronous task execution engine and registry."""

import asyncio
import inspect
import logging
import traceback
from typing import Any, Callable
from meshweaver.execution.results import TaskResult
from meshweaver.execution.serialization import deserialize_callable
from meshweaver.execution.task import Task, TaskState, TaskType
from meshweaver.utils.exceptions import SecurityError, TaskExecutionError, TaskTimeoutError
from meshweaver.utils.helpers import utc_now

logger = logging.getLogger("meshweaver.executor")


class TaskRegistry:
    """Registry of pre-approved safe task handlers."""

    def __init__(self) -> None:
        self._handlers: dict[str, Callable[..., Any]] = {}
        # Register standard built-in edge computing tasks
        self._register_builtins()

    def register(self, name: str, fn: Callable[..., Any] | None = None) -> Any:
        """Register a function by name, either directly or as a decorator."""
        if fn is None:
            def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
                self._handlers[name] = func
                return func
            return decorator
        self._handlers[name] = fn
        return fn

    def get(self, name: str) -> Callable[..., Any] | None:
        return self._handlers.get(name)

    def list_tasks(self) -> list[str]:
        return sorted(list(self._handlers.keys()))

    def _register_builtins(self) -> None:
        """Register basic safe math/data routines useful for edge computing."""
        def ping(*args: Any, **kwargs: Any) -> str:
            return "pong"

        def add(a: int | float, b: int | float) -> int | float:
            return a + b

        def multiply(a: int | float, b: int | float) -> int | float:
            return a * b

        def vector_norm(values: list[float]) -> float:
            return sum(x * x for x in values) ** 0.5

        def word_count(text: str) -> dict[str, int]:
            words = text.split()
            counts: dict[str, int] = {}
            for w in words:
                counts[w] = counts.get(w, 0) + 1
            return counts

        self.register("system.ping", ping)
        self.register("math.add", add)
        self.register("math.multiply", multiply)
        self.register("math.vector_norm", vector_norm)
        self.register("text.word_count", word_count)


def _isolated_worker_entry(
    pipe_conn: Any,
    fn_b64: str | None,
    fn_name: str | None,
    args: list[Any],
    kwargs: dict[str, Any],
) -> None:
    """Entry point executed inside an isolated worker subprocess."""
    try:
        if fn_b64:
            from meshweaver.execution.serialization import deserialize_callable
            fn = deserialize_callable(fn_b64)
        elif fn_name:
            reg = TaskRegistry()
            fn = reg.get(fn_name)
            if not fn:
                pipe_conn.send((False, ("TaskExecutionError", f"Task handler '{fn_name}' not found in registry")))
                return
        else:
            pipe_conn.send((False, ("TaskExecutionError", "No executable payload provided")))
            return

        res = fn(*args, **kwargs)
        if inspect.iscoroutine(res):
            res = asyncio.run(res)
        pipe_conn.send((True, res))
    except Exception as e:
        pipe_conn.send((False, (type(e).__name__, str(e))))
    finally:
        try:
            pipe_conn.close()
        except Exception:
            pass


class TaskExecutor:
    """Asynchronously executes tasks with concurrency limits and timeouts."""

    def __init__(
        self,
        node_id: str,
        registry: TaskRegistry | None = None,
        max_concurrent_tasks: int = 8,
        allow_cloudpickle: bool = False,
        process_isolation: bool = False,
    ) -> None:
        self.node_id = node_id
        self.registry = registry or TaskRegistry()
        self.max_concurrent_tasks = max_concurrent_tasks
        self.allow_cloudpickle = allow_cloudpickle
        self.process_isolation = process_isolation
        self._semaphore = asyncio.Semaphore(max_concurrent_tasks)
        self._active_tasks: dict[str, Task] = {}

    @property
    def active_task_count(self) -> int:
        return len(self._active_tasks)

    async def execute_task(self, task: Task) -> TaskResult:
        """Execute a task under supervision, handling timeouts and concurrency."""
        task.assigned_node_id = self.node_id
        if task.state == TaskState.CREATED or task.state == TaskState.QUEUED:
            task.transition_to(TaskState.ASSIGNED, reason=f"Assigned to {self.node_id[:8]}")

        async with self._semaphore:
            self._active_tasks[task.task_id] = task
            task.transition_to(TaskState.RUNNING, reason="Execution started")
            logger.info("Executing task %s (%s) on node %s", task.task_id[:8], task.name, self.node_id[:8])

            try:
                # Execute with strict timeout
                result_val = await asyncio.wait_for(
                    self._run_task_payload(task),
                    timeout=task.timeout,
                )
                task.result = result_val
                task.transition_to(TaskState.COMPLETED, reason="Execution successful")
                logger.info(
                    "Task %s completed in %.3fs",
                    task.task_id[:8],
                    task.execution_time_sec or 0.0,
                )
                return TaskResult(
                    task_id=task.task_id,
                    state=TaskState.COMPLETED,
                    result=result_val,
                    execution_time_sec=task.execution_time_sec,
                    worker_node_id=self.node_id,
                )

            except (asyncio.TimeoutError, TaskTimeoutError) as e:
                err_msg = f"Task exceeded timeout limit of {task.timeout}s"
                logger.warning("Task %s timed out: %s", task.task_id[:8], err_msg)
                task.error = err_msg
                task.transition_to(TaskState.FAILED, reason="Execution timed out")
                return TaskResult(
                    task_id=task.task_id,
                    state=TaskState.FAILED,
                    error=err_msg,
                    execution_time_sec=task.execution_time_sec,
                    worker_node_id=self.node_id,
                )

            except Exception as e:
                err_msg = f"{type(e).__name__}: {str(e)}"
                logger.error("Task %s failed: %s", task.task_id[:8], err_msg)
                task.error = err_msg
                task.transition_to(TaskState.FAILED, reason=err_msg)
                return TaskResult(
                    task_id=task.task_id,
                    state=TaskState.FAILED,
                    error=err_msg,
                    execution_time_sec=task.execution_time_sec,
                    worker_node_id=self.node_id,
                )

            finally:
                self._active_tasks.pop(task.task_id, None)

    async def _run_in_process(
        self,
        fn_b64: str | None = None,
        fn_name: str | None = None,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        timeout: float = 60.0,
    ) -> Any:
        import multiprocessing
        args = args or []
        kwargs = kwargs or {}

        parent_conn, child_conn = multiprocessing.Pipe()
        p = multiprocessing.Process(
            target=_isolated_worker_entry,
            args=(child_conn, fn_b64, fn_name, args, kwargs),
        )
        p.daemon = True
        p.start()
        child_conn.close()

        loop = asyncio.get_running_loop()

        def _poll_conn() -> Any:
            if parent_conn.poll(timeout + 1.0):
                try:
                    success, val = parent_conn.recv()
                    if success:
                        return val
                    exc_type, exc_msg = val
                    raise TaskExecutionError(f"{exc_type}: {exc_msg}")
                except EOFError:
                    raise TaskExecutionError("Worker process closed connection unexpectedly")
            raise TimeoutError()

        try:
            return await asyncio.wait_for(
                loop.run_in_executor(None, _poll_conn),
                timeout=timeout,
            )
        except (asyncio.TimeoutError, TimeoutError):
            if p.is_alive():
                p.terminate()
                p.join(timeout=0.5)
                if p.is_alive():
                    p.kill()
                    p.join(timeout=0.5)
            raise TaskTimeoutError(f"Task exceeded timeout limit of {timeout}s (worker process terminated)")
        finally:
            try:
                parent_conn.close()
            except Exception:
                pass
            if p.is_alive():
                p.terminate()
                p.join(timeout=0.2)

    async def _run_task_payload(self, task: Task) -> Any:
        args = task.payload.get("args", [])
        kwargs = task.payload.get("kwargs", {})
        isolate = self.process_isolation or bool(task.payload.get("isolate", False))

        if task.task_type == TaskType.REGISTERED:
            handler = self.registry.get(task.name)
            if not handler:
                raise TaskExecutionError(f"Task handler '{task.name}' not found in registry")

            if inspect.iscoroutinefunction(handler):
                return await handler(*args, **kwargs)
            elif isolate:
                return await self._run_in_process(fn_name=task.name, args=args, kwargs=kwargs, timeout=task.timeout)
            else:
                # Run sync handler in thread pool to avoid blocking asyncio event loop
                return await asyncio.to_thread(handler, *args, **kwargs)

        elif task.task_type == TaskType.FUNCTION:
            if not self.allow_cloudpickle:
                raise SecurityError(
                    "Execution of arbitrary serialized functions is disabled on this node"
                )
            fn_b64 = task.payload.get("fn_b64")
            if not fn_b64:
                raise TaskExecutionError("Missing 'fn_b64' payload in FUNCTION task")

            if isolate:
                return await self._run_in_process(fn_b64=fn_b64, args=args, kwargs=kwargs, timeout=task.timeout)

            fn = deserialize_callable(fn_b64)
            if inspect.iscoroutinefunction(fn):
                return await fn(*args, **kwargs)
            else:
                return await asyncio.to_thread(fn, *args, **kwargs)

        else:
            raise TaskExecutionError(f"Unsupported task type: {task.task_type}")
