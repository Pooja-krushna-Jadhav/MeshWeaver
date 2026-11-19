"""Configuration models for MeshWeaver nodes and subsystems."""

from dataclasses import dataclass, field
import os
from pathlib import Path
from typing import Any
from meshweaver.utils.helpers import generate_node_id


@dataclass
class NetworkConfig:
    host: str = "127.0.0.1"
    tcp_port: int = 8765
    udp_port: int = 8766
    advertise_host: str | None = None
    advertise_tcp_port: int | None = None
    advertise_udp_port: int | None = None
    connect_timeout: float = 5.0
    read_timeout: float = 30.0
    max_frame_size: int = 16 * 1024 * 1024  # 16 MB max frame
    connection_pool_size: int = 32
    tls_enabled: bool = False
    tls_cert_path: str | None = None
    tls_key_path: str | None = None
    tls_ca_path: str | None = None

    @property
    def effective_advertise_host(self) -> str:
        return self.advertise_host or self.host

    @property
    def effective_advertise_tcp_port(self) -> int:
        return self.advertise_tcp_port or self.tcp_port

    @property
    def effective_advertise_udp_port(self) -> int:
        return self.advertise_udp_port or self.udp_port


@dataclass
class DiscoveryConfig:
    enabled: bool = True
    bootstrap_peers: list[str] = field(default_factory=list)  # e.g. ["127.0.0.1:8765"]
    k_bucket_size: int = 20
    alpha: int = 3  # Parallel lookup concurrency
    refresh_interval: float = 3600.0  # 1 hour bucket refresh
    ping_interval: float = 30.0
    enable_lan_broadcast: bool = True
    broadcast_port: int = 52520


@dataclass
class GossipConfig:
    enabled: bool = True
    interval: float = 5.0  # Periodic broadcast every 5s
    fanout: int = 3  # Number of random neighbors to gossip with
    max_hops: int = 3
    stale_threshold: float = 15.0  # Mark peer suspect after 15s without update
    dead_threshold: float = 30.0   # Mark peer dead after 30s
    purge_threshold: float = 90.0  # Evict peer after 90s


@dataclass
class SchedulerConfig:
    strategy: str = "multi_metric"  # "least_loaded", "multi_metric", "round_robin"
    max_concurrent_tasks: int = 8
    cpu_weight: float = 0.5
    ram_weight: float = 0.3
    queue_weight: float = 0.2
    max_task_timeout: float = 300.0  # Default 5 minutes
    process_isolation: bool = False


@dataclass
class SecurityConfig:
    enabled: bool = False
    require_signatures: bool = False
    shared_secret: str | None = None
    private_key_path: str | None = None
    public_key_path: str | None = None
    allowed_nodes: list[str] = field(default_factory=list)
    allow_cloudpickle: bool = False  # Disabled by default for defense against RCE
    trusted_public_keys: dict[str, str] = field(default_factory=dict)  # node_id -> public_key_hex
    replay_window_sec: float = 30.0


@dataclass
class StorageConfig:
    enabled: bool = True
    db_path: str = ":memory:"  # In-memory by default, or path like "~/.meshweaver/state.db"


@dataclass
class MonitoringConfig:
    log_level: str = "INFO"
    enable_dashboard: bool = False
    metrics_history_size: int = 120


@dataclass
class NodeConfig:
    node_id: str = field(default_factory=generate_node_id)
    name: str = "mesh-node"
    data_dir: str = field(default_factory=lambda: str(Path.home() / ".meshweaver"))
    network: NetworkConfig = field(default_factory=NetworkConfig)
    discovery: DiscoveryConfig = field(default_factory=DiscoveryConfig)
    gossip: GossipConfig = field(default_factory=GossipConfig)
    scheduler: SchedulerConfig = field(default_factory=SchedulerConfig)
    security: SecurityConfig = field(default_factory=SecurityConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    monitoring: MonitoringConfig = field(default_factory=MonitoringConfig)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "NodeConfig":
        data = data.copy()
        network_data = data.pop("network", {})
        discovery_data = data.pop("discovery", {})
        gossip_data = data.pop("gossip", {})
        scheduler_data = data.pop("scheduler", {})
        security_data = data.pop("security", {})
        storage_data = data.pop("storage", {})
        monitoring_data = data.pop("monitoring", {})

        return cls(
            network=NetworkConfig(**network_data),
            discovery=DiscoveryConfig(**discovery_data),
            gossip=GossipConfig(**gossip_data),
            scheduler=SchedulerConfig(**scheduler_data),
            security=SecurityConfig(**security_data),
            storage=StorageConfig(**storage_data),
            monitoring=MonitoringConfig(**monitoring_data),
            **data,
        )

    @classmethod
    def from_env(cls) -> "NodeConfig":
        """Load configuration with environment variable overrides."""
        cfg = cls()
        if os.getenv("MESHWEAVER_NODE_ID"):
            cfg.node_id = os.environ["MESHWEAVER_NODE_ID"]
        if os.getenv("MESHWEAVER_NAME"):
            cfg.name = os.environ["MESHWEAVER_NAME"]
        if os.getenv("MESHWEAVER_HOST"):
            cfg.network.host = os.environ["MESHWEAVER_HOST"]
        if os.getenv("MESHWEAVER_TCP_PORT"):
            cfg.network.tcp_port = int(os.environ["MESHWEAVER_TCP_PORT"])
        if os.getenv("MESHWEAVER_UDP_PORT"):
            cfg.network.udp_port = int(os.environ["MESHWEAVER_UDP_PORT"])
        if os.getenv("MESHWEAVER_BOOTSTRAP_PEERS"):
            cfg.discovery.bootstrap_peers = [
                p.strip() for p in os.environ["MESHWEAVER_BOOTSTRAP_PEERS"].split(",") if p.strip()
            ]
        if os.getenv("MESHWEAVER_SHARED_SECRET"):
            cfg.security.shared_secret = os.environ["MESHWEAVER_SHARED_SECRET"]
            cfg.security.enabled = True
        if os.getenv("MESHWEAVER_ALLOW_CLOUDPICKLE"):
            cfg.security.allow_cloudpickle = os.environ["MESHWEAVER_ALLOW_CLOUDPICKLE"].lower() in ("1", "true", "yes")
        if os.getenv("MESHWEAVER_DB_PATH"):
            cfg.storage.db_path = os.environ["MESHWEAVER_DB_PATH"]
        return cfg
