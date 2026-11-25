"""Retry policies and task deduplication registry."""

import asyncio
import logging
import time
from typing import Any, Callable, Coroutine
from meshweaver.utils.helpers import utc_now

logger = logging.getLogger("meshweaver.retry")


class RetryPolicy:
    """Configurable exponential backoff retry policy."""

    def __init__(
        self,
        max_retries: int = 3,
        initial_delay: float = 0.5,
        multiplier: float = 2.0,
        max_delay: float = 5.0,
    ) -> None:
        self.max_retries = max_retries
        self.initial_delay = initial_delay
        self.multiplier = multiplier
        self.max_delay = max_delay

    def get_delay(self, attempt: int) -> float:
        """Calculate exponential backoff delay for the given retry attempt (1-indexed)."""
        delay = self.initial_delay * (self.multiplier ** (attempt - 1))
        return min(delay, self.max_delay)


class DeduplicationRegistry:
    """Tracks recently processed task IDs to prevent duplicate execution (idempotency)."""

    def __init__(self, ttl_sec: float = 3600.0, max_entries: int = 10000) -> None:
        self.ttl_sec = ttl_sec
        self.max_entries = max_entries
        # task_id -> (result_dict, completed_at)
        self._cache: dict[str, tuple[dict[str, Any] | None, float]] = {}

    def is_processed(self, task_id: str) -> bool:
        """Check if task_id has already been processed or is currently running."""
        self._cleanup()
        return task_id in self._cache

    def record_started(self, task_id: str) -> bool:
        """Record task start. Returns False if already seen (duplicate!)."""
        self._cleanup()
        if task_id in self._cache:
            return False
        self._cache[task_id] = (None, utc_now())
        self._cleanup()
        return True

    def clear_started(self, task_id: str) -> None:
        """Remove an in-flight entry if execution aborted before completion."""
        entry = self._cache.get(task_id)
        if entry is not None and entry[0] is None:
            del self._cache[task_id]

    def record_completed(self, task_id: str, result_dict: dict[str, Any]) -> None:
        """Cache completed task result for idempotency reconciliation."""
        self._cache[task_id] = (result_dict, utc_now())
        self._cleanup()

    def get_result(self, task_id: str) -> dict[str, Any] | None:
        self._cleanup()
        val = self._cache.get(task_id)
        return val[0] if val else None

    def _cleanup(self) -> None:
        now = utc_now()
        expired = [tid for tid, (_, t) in self._cache.items() if now - t > self.ttl_sec]
        for tid in expired:
            del self._cache[tid]

        if self.max_entries <= 0:
            self._cache.clear()
        elif len(self._cache) > self.max_entries:
            overflow = len(self._cache) - self.max_entries
            oldest = sorted(self._cache.items(), key=lambda item: item[1][1])[:overflow]
            for tid, _ in oldest:
                del self._cache[tid]
