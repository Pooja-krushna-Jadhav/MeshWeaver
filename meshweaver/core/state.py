"""State definitions and models for MeshWeaver nodes and peers."""

from dataclasses import dataclass, field
from enum import Enum
import time
from typing import Any
from meshweaver.utils.helpers import utc_now


class NodeStatus(str, Enum):
    INITIALIZING = "INITIALIZING"
    ONLINE = "ONLINE"
    BUSY = "BUSY"
    DRAINING = "DRAINING"
    STOPPED = "STOPPED"


class PeerHealth(str, Enum):
    HEALTHY = "HEALTHY"
    SUSPECT = "SUSPECT"
    DEAD = "DEAD"


@dataclass
class NodeInfo:
    node_id: str
    name: str = "mesh-node"
    host: str = "127.0.0.1"
    tcp_port: int = 8765
    udp_port: int = 8766
    public_key: str | None = None
    version: str = "0.1.0"
    started_at: float = field(default_factory=utc_now)

    @property
    def tcp_address(self) -> tuple[str, int]:
        return (self.host, self.tcp_port)

    @property
    def udp_address(self) -> tuple[str, int]:
        return (self.host, self.udp_port)

    def to_dict(self) -> dict[str, Any]:
        return {
            "node_id": self.node_id,
            "name": self.name,
            "host": self.host,
            "tcp_port": self.tcp_port,
            "udp_port": self.udp_port,
            "public_key": self.public_key,
            "version": self.version,
            "started_at": self.started_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "NodeInfo":
        return cls(**data)


@dataclass
class ResourceMetrics:
    cpu_percent: float = 0.0
    memory_total_mb: float = 0.0
    memory_available_mb: float = 0.0
    memory_percent: float = 0.0
    active_tasks: int = 0
    max_tasks: int = 8
    timestamp: float = 0.0

    @property
    def available_capacity(self) -> int:
        return max(0, self.max_tasks - self.active_tasks)

    @property
    def is_overloaded(self) -> bool:
        return self.cpu_percent > 90.0 or self.memory_percent > 92.0 or self.available_capacity <= 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "cpu_percent": round(self.cpu_percent, 2),
            "memory_total_mb": round(self.memory_total_mb, 2),
            "memory_available_mb": round(self.memory_available_mb, 2),
            "memory_percent": round(self.memory_percent, 2),
            "active_tasks": self.active_tasks,
            "max_tasks": self.max_tasks,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ResourceMetrics":
        return cls(**data)


@dataclass
class PeerState:
    info: NodeInfo
    metrics: ResourceMetrics = field(default_factory=ResourceMetrics)
    health: PeerHealth = PeerHealth.HEALTHY
    last_seen: float = field(default_factory=utc_now)
    rtt_ms: float = 0.0
    fail_count: int = 0

    def mark_alive(self, rtt_ms: float | None = None) -> None:
        self.last_seen = utc_now()
        self.health = PeerHealth.HEALTHY
        self.fail_count = 0
        if rtt_ms is not None:
            # Exponential moving average for smooth RTT tracking
            self.rtt_ms = rtt_ms if self.rtt_ms == 0.0 else (0.7 * self.rtt_ms + 0.3 * rtt_ms)

    def mark_failed(self) -> None:
        self.fail_count += 1
        if self.fail_count >= 3:
            self.health = PeerHealth.DEAD
        else:
            self.health = PeerHealth.SUSPECT

    def to_dict(self) -> dict[str, Any]:
        return {
            "info": self.info.to_dict(),
            "metrics": self.metrics.to_dict(),
            "health": self.health.value,
            "last_seen": self.last_seen,
            "rtt_ms": round(self.rtt_ms, 2),
            "fail_count": self.fail_count,
        }
