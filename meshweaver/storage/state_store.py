"""Lightweight SQLite-backed persistent state store for crash durability."""

import asyncio
import json
import logging
import sqlite3
import threading
from typing import Any
from meshweaver.execution.task import Task, TaskPriority, TaskState, TaskType
from meshweaver.utils.helpers import json_dumps, json_loads, utc_now

logger = logging.getLogger("meshweaver.storage")


class StateStore:
    """SQLite persistence layer for tasks and node identity across reboots."""

    def __init__(self, db_path: str = ":memory:") -> None:
        self.db_path = db_path
        self._lock = threading.Lock()
        if self.db_path == ":memory:":
            self._mem_conn: sqlite3.Connection | None = sqlite3.connect(":memory:", check_same_thread=False)
            self._mem_conn.row_factory = sqlite3.Row
        else:
            self._mem_conn = None
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        if self._mem_conn is not None:
            return self._mem_conn
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        """Create tables if they do not exist."""
        with self._lock:
            conn = self._get_connection()
            conn.execute("""
                CREATE TABLE IF NOT EXISTS node_identity (
                    node_id TEXT PRIMARY KEY,
                    name TEXT,
                    public_key TEXT,
                    private_key TEXT,
                    created_at REAL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    payload_json TEXT,
                    task_type TEXT,
                    origin_node_id TEXT,
                    assigned_node_id TEXT,
                    priority INTEGER,
                    state TEXT NOT NULL,
                    timeout REAL,
                    retry_count INTEGER,
                    max_retries INTEGER,
                    result_json TEXT,
                    error TEXT,
                    created_at REAL,
                    started_at REAL,
                    completed_at REAL,
                    execution_time_sec REAL
                )
            """)
            conn.commit()
            if self._mem_conn is None:
                conn.close()

    async def save_task(self, task: Task) -> None:
        """Persist or update a task record asynchronously."""
        await asyncio.to_thread(self._sync_save_task, task)

    def _sync_save_task(self, task: Task) -> None:
        with self._lock:
            conn = self._get_connection()
            try:
                conn.execute("""
                    INSERT INTO tasks (
                        task_id, name, payload_json, task_type, origin_node_id,
                        assigned_node_id, priority, state, timeout, retry_count,
                        max_retries, result_json, error, created_at, started_at,
                        completed_at, execution_time_sec
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(task_id) DO UPDATE SET
                        assigned_node_id=excluded.assigned_node_id,
                        state=excluded.state,
                        retry_count=excluded.retry_count,
                        result_json=excluded.result_json,
                        error=excluded.error,
                        started_at=excluded.started_at,
                        completed_at=excluded.completed_at,
                        execution_time_sec=excluded.execution_time_sec
                """, (
                    task.task_id,
                    task.name,
                    json_dumps(task.payload),
                    task.task_type.value,
                    task.origin_node_id,
                    task.assigned_node_id,
                    int(task.priority),
                    task.state.value,
                    task.timeout,
                    task.retry_count,
                    task.max_retries,
                    json_dumps(task.result) if task.result is not None else None,
                    task.error,
                    task.created_at,
                    task.started_at,
                    task.completed_at,
                    task.execution_time_sec,
                ))
                conn.commit()
            finally:
                if self._mem_conn is None:
                    conn.close()

    async def get_task(self, task_id: str) -> Task | None:
        return await asyncio.to_thread(self._sync_get_task, task_id)

    def _sync_get_task(self, task_id: str) -> Task | None:
        with self._lock:
            conn = self._get_connection()
            try:
                cur = conn.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,))
                row = cur.fetchone()
                if not row:
                    return None
                return self._row_to_task(row)
            finally:
                if self._mem_conn is None:
                    conn.close()

    async def list_tasks(self, limit: int = 50) -> list[Task]:
        return await asyncio.to_thread(self._sync_list_tasks, limit)

    def _sync_list_tasks(self, limit: int) -> list[Task]:
        with self._lock:
            conn = self._get_connection()
            try:
                cur = conn.execute("SELECT * FROM tasks ORDER BY created_at DESC LIMIT ?", (limit,))
                return [self._row_to_task(row) for row in cur.fetchall()]
            finally:
                if self._mem_conn is None:
                    conn.close()

    def _row_to_task(self, row: sqlite3.Row) -> Task:
        payload = json_loads(row["payload_json"]) if row["payload_json"] else {}
        result = json_loads(row["result_json"]) if row["result_json"] else None
        return Task(
            task_id=row["task_id"],
            name=row["name"],
            payload=payload,
            task_type=TaskType(row["task_type"]),
            origin_node_id=row["origin_node_id"] or "",
            assigned_node_id=row["assigned_node_id"],
            priority=TaskPriority(row["priority"]),
            state=TaskState(row["state"]),
            timeout=row["timeout"] or 60.0,
            retry_count=row["retry_count"] or 0,
            max_retries=row["max_retries"] or 3,
            created_at=row["created_at"],
            started_at=row["started_at"],
            completed_at=row["completed_at"],
            execution_time_sec=row["execution_time_sec"],
            result=result,
            error=row["error"],
        )

    async def save_identity(self, node_id: str, name: str, public_key: str | None = None, private_key: str | None = None) -> None:
        await asyncio.to_thread(self._sync_save_identity, node_id, name, public_key, private_key)

    def _sync_save_identity(self, node_id: str, name: str, public_key: str | None, private_key: str | None) -> None:
        with self._lock:
            conn = self._get_connection()
            try:
                conn.execute("""
                    INSERT OR REPLACE INTO node_identity (node_id, name, public_key, private_key, created_at)
                    VALUES (?, ?, ?, ?, ?)
                """, (node_id, name, public_key, private_key, utc_now()))
                conn.commit()
            finally:
                if self._mem_conn is None:
                    conn.close()

    async def load_identity(self) -> dict[str, Any] | None:
        return await asyncio.to_thread(self._sync_load_identity)

    def _sync_load_identity(self) -> dict[str, Any] | None:
        with self._lock:
            conn = self._get_connection()
            try:
                cur = conn.execute("SELECT * FROM node_identity LIMIT 1")
                row = cur.fetchone()
                if row:
                    return dict(row)
                return None
            finally:
                if self._mem_conn is None:
                    conn.close()
