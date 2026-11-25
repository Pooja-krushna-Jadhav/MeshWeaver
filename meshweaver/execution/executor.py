"""Asynchronous task execution engine and registry."""

import asyncio
import importlib
import inspect
import logging
import multiprocessing
import os
import threading
import time
import traceback
import types
from typing import Any, Callable
from meshweaver.execution.results import TaskResult
from meshweaver.execution.serialization import deserialize_callable
from meshweaver.execution.task import Task, TaskState, TaskType
from meshweaver.utils.exceptions import SecurityError, TaskExecutionError, TaskTimeoutError
from meshweaver.utils.helpers import json_dumps, json_loads, utc_now

logger = logging.getLogger("meshweaver.executor")


class TaskRegistry:
    """Registry of pre-approved safe task handlers."""

    def __init__(self) -> None:
        self._handlers: dict[str, Callable[..., Any]] = {}
        self._builtin_handlers: dict[str, Callable[..., Any]] = {}
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

    def is_builtin(self, name: str, fn: Callable[..., Any] | None = None) -> bool:
        builtin = self._builtin_handlers.get(name)
        if builtin is None:
            return False
        return fn is None or fn is builtin

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

        for builtin_name, builtin_fn in (
            ("system.ping", ping),
            ("math.add", add),
            ("math.multiply", multiply),
            ("math.vector_norm", vector_norm),
            ("text.word_count", word_count),
        ):
            self.register(builtin_name, builtin_fn)
            self._builtin_handlers[builtin_name] = builtin_fn


def _get_import_path(fn: Callable[..., Any]) -> tuple[str, str] | None:
    """Return (module, qualname) if fn is top-level importable, else None."""
    self_obj = getattr(fn, "__self__", None)
    if self_obj is not None and not isinstance(self_obj, types.ModuleType):
        return None
    mod = getattr(fn, "__module__", None)
    qualname = getattr(fn, "__qualname__", None)
    if mod and qualname and "<locals>" not in qualname:
        return (mod, qualname)
    return None


def _resolve_import_path(fn_import_path: tuple[str, str]) -> Callable[..., Any]:
    """Import and resolve a top-level callable from (module, qualname)."""
    mod_name, qualname = fn_import_path
    if "<locals>" in qualname:
        raise TaskExecutionError(f"Cannot import local function '{qualname}' in isolated worker")
    obj: Any = importlib.import_module(mod_name)
    for attr in qualname.split("."):
        obj = getattr(obj, attr)
    if not callable(obj):
        raise TaskExecutionError(f"Imported object '{mod_name}.{qualname}' is not callable")
    return obj


def _send_worker_result(pipe_conn: Any, success: bool, payload: Any) -> None:
    """Serialize worker result strictly as JSON UTF-8 bytes over the pipe."""
    pipe_conn.send_bytes(json_dumps([success, payload]).encode("utf-8"))


def _isolated_worker_entry(
    pipe_conn: Any,
    fn_b64: str | None,
    fn_name: str | None,
    args: list[Any],
    kwargs: dict[str, Any],
    fn_import_path: tuple[str, str] | None = None,
) -> None:
    """Entry point executed inside an isolated worker subprocess."""
    try:
        # Scrub sensitive MeshWeaver configuration/secret env vars before executing task code
        for key in [k for k in os.environ if k.startswith("MESHWEAVER_")]:
            os.environ.pop(key, None)

        if fn_b64:
            fn = deserialize_callable(fn_b64)
        elif fn_import_path:
            fn = _resolve_import_path(fn_import_path)
        elif fn_name:
            reg = TaskRegistry()
            fn = reg.get(fn_name)
            if not fn:
                _send_worker_result(
                    pipe_conn,
                    False,
                    ["TaskExecutionError", f"Task handler '{fn_name}' not found in registry"],
                )
                return
        else:
            _send_worker_result(pipe_conn, False, ["TaskExecutionError", "No executable payload provided"])
            return

        res = fn(*args, **kwargs)
        if inspect.iscoroutine(res):
            res = asyncio.run(res)
        _send_worker_result(pipe_conn, True, res)
    except Exception as e:
        try:
            _send_worker_result(pipe_conn, False, [type(e).__name__, str(e)])
        except Exception:
            pass
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
            task.transition_to(TaskState.RUNNING, reason="Execution started")
            logger.info("Executing task %s (%s) on node %s", task.task_id[:8], task.name, self.node_id[:8])

            try:
                self._active_tasks[task.task_id] = task
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
        fn_import_path: tuple[str, str] | None = None,
    ) -> Any:
        args = args or []
        kwargs = kwargs or {}

        ctx = multiprocessing.get_context("spawn")
        parent_conn, child_conn = ctx.Pipe()
        p = ctx.Process(
            target=_isolated_worker_entry,
            args=(child_conn, fn_b64, fn_name, args, kwargs, fn_import_path),
        )
        p.daemon = True
        p.start()
        child_conn.close()

        stop_event = threading.Event()

        def _poll_conn() -> Any:
            deadline = time.monotonic() + timeout
            while not stop_event.is_set():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError()
                if parent_conn.poll(min(0.05, remaining)):
                    try:
                        raw = parent_conn.recv_bytes()
                    except EOFError:
                        raise TaskExecutionError("Worker process closed connection unexpectedly")
                    try:
                        decoded = json_loads(raw)
                        success, val = decoded
                    except Exception as e:
                        raise TaskExecutionError(f"Malformed worker response: {e}") from e
                    if success:
                        return val
                    if isinstance(val, (list, tuple)) and len(val) == 2:
                        exc_type, exc_msg = val
                        raise TaskExecutionError(f"{exc_type}: {exc_msg}")
                    raise TaskExecutionError(f"Worker execution failed: {val}")
            raise asyncio.CancelledError()

        try:
            return await asyncio.to_thread(_poll_conn)
        except TimeoutError:
            raise TaskTimeoutError(f"Task exceeded timeout limit of {timeout}s (worker process terminated)")
        finally:
            stop_event.set()
            try:
                if p.is_alive():
                    p.terminate()
                    await asyncio.to_thread(p.join, 0.5)
                    if p.is_alive():
                        p.kill()
                await asyncio.to_thread(p.join, 1.0)
            finally:
                try:
                    parent_conn.close()
                except Exception:
                    pass

    async def _run_task_payload(self, task: Task) -> Any:
        args = task.payload.get("args", [])
        kwargs = task.payload.get("kwargs", {})
        isolate = self.process_isolation or bool(task.payload.get("isolate", False))

        if task.task_type == TaskType.REGISTERED:
            handler = self.registry.get(task.name)
            if not handler:
                raise TaskExecutionError(f"Task handler '{task.name}' not found in registry")

            fn_import_path = _get_import_path(handler)
            if inspect.iscoroutinefunction(handler):
                if isolate:
                    if fn_import_path:
                        return await self._run_in_process(
                            fn_name=task.name,
                            args=args,
                            kwargs=kwargs,
                            timeout=task.timeout,
                            fn_import_path=fn_import_path,
                        )
                    raise TaskExecutionError(
                        f"Async handler '{task.name}' cannot run with process_isolation=True unless top-level importable"
                    )
                return await handler(*args, **kwargs)
            elif isolate:
                if not fn_import_path and not self.registry.is_builtin(task.name, handler):
                    raise TaskExecutionError(
                        f"Task handler '{task.name}' cannot run with process_isolation=True because it is not a top-level importable function"
                    )
                return await self._run_in_process(
                    fn_name=task.name,
                    args=args,
                    kwargs=kwargs,
                    timeout=task.timeout,
                    fn_import_path=fn_import_path,
                )
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
