"""Core MeshWeaver Node runtime."""

import asyncio
import hashlib
import logging
import os
from pathlib import Path
from typing import Any, Callable, Coroutine
from meshweaver.core.config import NodeConfig
from meshweaver.core.events import (
    EVENT_NODE_STATE_CHANGED,
    EVENT_TASK_ASSIGNED,
    EVENT_TASK_COMPLETED,
    EVENT_TASK_CREATED,
    EVENT_TASK_FAILED,
    EVENT_TASK_RETRIED,
    EVENT_TASK_STARTED,
    EventBus,
)
from meshweaver.core.state import NodeInfo, NodeStatus, PeerState
from meshweaver.discovery.kademlia import KademliaNode
from meshweaver.discovery.peer_discovery import PeerDiscoveryService
from meshweaver.execution.executor import TaskExecutor, TaskRegistry
from meshweaver.execution.results import TaskResult
from meshweaver.execution.task import Task, TaskState, TaskType
from meshweaver.gossip.gossip_protocol import GossipProtocol
from meshweaver.gossip.peer_state import PeerTable
from meshweaver.gossip.resource_monitor import ResourceMonitor
from meshweaver.networking.protocol import Message
from meshweaver.networking.tcp import AsyncTCPClient, AsyncTCPServer
from meshweaver.networking.udp import AsyncUDPEndpoint
from meshweaver.reliability.failure_detector import FailureDetector
from meshweaver.reliability.heartbeat import HeartbeatMonitor
from meshweaver.reliability.recovery import RecoveryManager
from meshweaver.reliability.retry import DeduplicationRegistry
from meshweaver.scheduler.scheduler import TaskScheduler
from meshweaver.security.authentication import KeyManager, ReplayProtector
from meshweaver.security.permissions import AuthorizationPolicy
from meshweaver.storage.state_store import StateStore
from meshweaver.utils.exceptions import (
    AuthenticationError,
    ConfigurationError,
    NetworkError,
    SchedulingError,
    SecurityError,
    UnauthorizedError,
)
from meshweaver.utils.helpers import json_dumps, utc_now

logger = logging.getLogger("meshweaver.node")


class Node:
    """A decentralized MeshWeaver peer node participating in the mesh."""

    _MAX_TASKS_HISTORY = 1000

    def __init__(self, config: NodeConfig | None = None, server_tls: bool = True) -> None:
        self.config = config or NodeConfig()
        self.config.validate(require_server_tls=server_tls)

        self.node_id = self.config.node_id
        self.status = NodeStatus.INITIALIZING
        self.events = EventBus()

        # Security & Cryptography
        private_key_pem: str | None = None
        if self.config.security.private_key_path:
            try:
                private_key_pem = Path(self.config.security.private_key_path).read_text(encoding="utf-8")
            except OSError as e:
                raise ConfigurationError(
                    f"Failed to read private key from '{self.config.security.private_key_path}': {e}"
                ) from e

        self.keys = KeyManager(
            private_key_pem=private_key_pem,
            shared_secret=self.config.security.shared_secret,
        )
        self.replay_protector = ReplayProtector(window_sec=self.config.security.replay_window_sec)
        self.auth_policy = AuthorizationPolicy(
            allowed_nodes=self.config.security.allowed_nodes,
            allow_cloudpickle=self.config.security.allow_cloudpickle,
            admin_nodes=self.config.security.admin_nodes,
        )
        self._verified_public_keys: dict[str, str] = {}

        # In asymmetric mode, bind node_id to SHA-1 hash of public key
        if (
            self._security_enabled
            and not self.config.security.shared_secret
            and self.keys.public_key_hex not in ("none", "hmac")
        ):
            self.node_id = hashlib.sha1(bytes.fromhex(self.keys.public_key_hex)).hexdigest()

        self.info = NodeInfo(
            node_id=self.node_id,
            name=self.config.name,
            host=self.config.network.effective_advertise_host,
            tcp_port=self.config.network.effective_advertise_tcp_port,
            udp_port=self.config.network.effective_advertise_udp_port,
            public_key=self.keys.public_key_hex,
        )

        # Networking components
        server_ssl = None
        client_ssl = None
        if self.config.network.tls_enabled:
            from meshweaver.networking.tcp import create_ssl_context
            if server_tls:
                server_ssl = create_ssl_context(
                    cert_path=self.config.network.tls_cert_path,
                    key_path=self.config.network.tls_key_path,
                    ca_path=self.config.network.tls_ca_path,
                    is_server=True,
                )
            client_ssl = create_ssl_context(
                cert_path=self.config.network.tls_cert_path,
                key_path=self.config.network.tls_key_path,
                ca_path=self.config.network.tls_ca_path,
                is_server=False,
            )

        self.tcp_server = AsyncTCPServer(
            host=self.config.network.host,
            port=self.config.network.tcp_port,
            handler=self._handle_tcp_message,
            max_frame_size=self.config.network.max_frame_size,
            ssl_context=server_ssl,
            read_timeout=self.config.network.read_timeout,
        )
        self.tcp_client = AsyncTCPClient(
            max_frame_size=self.config.network.max_frame_size,
            max_idle=self.config.network.connection_pool_size,
            ssl_context=client_ssl,
            connect_timeout=self.config.network.connect_timeout,
        )
        self.udp_endpoint = AsyncUDPEndpoint(
            host=self.config.network.host,
            port=self.config.network.udp_port,
            handler=self._handle_udp_message,
        )

        # Task execution subsystem
        self.registry = TaskRegistry()
        self.executor = TaskExecutor(
            node_id=self.node_id,
            registry=self.registry,
            max_concurrent_tasks=self.config.scheduler.max_concurrent_tasks,
            allow_cloudpickle=self.config.security.allow_cloudpickle,
            process_isolation=self.config.scheduler.process_isolation,
        )

        # Resource monitoring and peer state
        self.resource_monitor = ResourceMonitor(max_tasks=self.config.scheduler.max_concurrent_tasks)
        self.peer_table = PeerTable()

        # Kademlia DHT
        self.kademlia = KademliaNode(
            local_node_id=self.node_id,
            host=self.info.host,
            tcp_port=self.info.tcp_port,
            udp_port=self.info.udp_port,
            send_rpc_fn=self._send_rpc,
            k_size=self.config.discovery.k_bucket_size,
            alpha=self.config.discovery.alpha,
        )

        # Peer Discovery (LAN broadcast + bootstrap)
        self.discovery = PeerDiscoveryService(
            node_id=self.node_id,
            host=self.info.host,
            tcp_port=self.info.tcp_port,
            udp_port=self.info.udp_port,
            kademlia=self.kademlia,
            bootstrap_peers=self.config.discovery.bootstrap_peers,
            enable_lan_broadcast=self.config.discovery.enable_lan_broadcast,
            broadcast_port=self.config.discovery.broadcast_port,
            refresh_interval=self.config.discovery.refresh_interval,
            sign_fn=self._sign_beacon_dict,
            verify_fn=self._verify_beacon_dict,
        )

        # Failure detector (wired into gossip purge lifecycle)
        self.failure_detector = FailureDetector()

        # Gossip Protocol Engine
        self.gossip = GossipProtocol(
            node_id=self.node_id,
            node_info=self.info,
            peer_table=self.peer_table,
            resource_monitor=self.resource_monitor,
            kademlia=self.kademlia,
            events=self.events,
            send_udp_fn=self.udp_endpoint.send_message,
            interval=self.config.gossip.interval,
            fanout=self.config.gossip.fanout,
            stale_threshold=self.config.gossip.stale_threshold,
            dead_threshold=self.config.gossip.dead_threshold,
            purge_threshold=self.config.gossip.purge_threshold,
            active_tasks_fn=lambda: self.executor.active_task_count,
            sign_fn=self._sign_message_if_enabled,
            on_purge_fn=self.failure_detector.remove_peer,
        )

        # Distributed Task Scheduler
        self.scheduler = TaskScheduler(
            node_id=self.node_id,
            node_info=self.info,
            peer_table=self.peer_table,
            resource_monitor=self.resource_monitor,
            events=self.events,
            config=self.config.scheduler,
        )

        # Task tracking & coordination
        self.tasks: dict[str, Task] = {}
        self._task_events: dict[str, asyncio.Event] = {}
        self._running_tasks: set[asyncio.Task[Any]] = set()
        self._inflight_rpc_tasks: dict[str, asyncio.Task[Any]] = {}
        self._task_futures: dict[str, asyncio.Future[TaskResult]] = {}
        self._excluded_peers: dict[str, set[str]] = {}

        # Persistence & Reliability
        self.store = StateStore(db_path=self.config.storage.db_path) if self.config.storage.enabled else None
        self.dedup = DeduplicationRegistry()
        self.heartbeat = HeartbeatMonitor(
            node_id=self.node_id,
            peer_table=self.peer_table,
            failure_detector=self.failure_detector,
            events=self.events,
            send_fn=self.ping_tcp,
            interval=2.0,
        )
        self.recovery = RecoveryManager(
            node_id=self.node_id,
            tasks_map=self.tasks,
            events=self.events,
            reschedule_task_fn=self.submit_task,
            cancel_inflight_fn=self._cancel_inflight_task,
            resolve_task_fn=self._resolve_task_future,
        )

        # Message dispatch routing: msg_type -> async handler
        self._custom_handlers: dict[str, Callable[[Message], Coroutine[Any, Any, Message | None]]] = {
            "kad_rpc": self.kademlia.handle_rpc,
        }

    @property
    def _security_enabled(self) -> bool:
        return bool(self.config.security.enabled or self.config.security.require_signatures)

    @property
    def peers(self) -> dict[str, PeerState]:
        """Synchronized view of known peers backed by peer_table."""
        return self.peer_table._peers

    @property
    def tcp_address(self) -> tuple[str, int]:
        return (self.config.network.host, self.tcp_server.actual_port)

    @property
    def udp_address(self) -> tuple[str, int]:
        return (self.config.network.host, self.udp_endpoint.actual_port)

    def _apply_identity(self, node_id: str, keys: KeyManager | None = None) -> None:
        """Propagate node identity and optional KeyManager across all subsystems."""
        if keys is not None:
            self.keys = keys
        self.node_id = node_id
        self.info.node_id = node_id
        self.info.public_key = self.keys.public_key_hex
        self.executor.node_id = node_id
        self.kademlia.local_node_id = node_id
        self.kademlia.routing_table.local_node_id = node_id
        self.discovery.node_id = node_id
        self.gossip.node_id = node_id
        self.scheduler.node_id = node_id
        self.heartbeat.node_id = node_id
        self.recovery.node_id = node_id

    def _store_task(self, task: Task) -> None:
        """Insert or update a task in self.tasks while bounding history to _MAX_TASKS_HISTORY."""
        self.tasks[task.task_id] = task
        if len(self.tasks) > self._MAX_TASKS_HISTORY:
            for tid in list(self.tasks.keys()):
                if len(self.tasks) <= self._MAX_TASKS_HISTORY:
                    break
                if tid != task.task_id and self.tasks[tid].is_terminal:
                    del self.tasks[tid]

    def _sign_message_if_enabled(self, message: Message) -> None:
        """Sign an outbound Message when security is enabled and a signing key exists."""
        if self._security_enabled and self.keys.has_signing_key and not message.signature:
            self.keys.sign_message(message)

    def _sign_beacon_dict(self, beacon: dict[str, Any]) -> dict[str, Any]:
        """Sign an outbound LAN discovery beacon dict when security is enabled."""
        if not self._security_enabled or not self.keys.has_signing_key:
            return beacon
        signed = dict(beacon)
        if self.keys.public_key_hex not in ("none", "hmac"):
            signed["public_key"] = self.keys.public_key_hex
        canon = json_dumps(signed).encode("utf-8")
        signed["signature"] = self.keys.sign_bytes(canon)
        return signed

    def _verify_beacon_dict(self, beacon: dict[str, Any]) -> bool:
        """Verify an inbound LAN discovery beacon dict when security is enabled."""
        if not self._security_enabled:
            return True
        sig = beacon.get("signature")
        sender_id = beacon.get("node_id")
        ts = beacon.get("timestamp")
        if not isinstance(sig, str) or not sig or not isinstance(sender_id, str) or not sender_id:
            return False
        if sender_id == self.node_id:
            return False
        if isinstance(ts, bool) or not isinstance(ts, (int, float)):
            return False
        if not (abs(utc_now() - float(ts)) <= self.replay_protector.window_sec):
            return False
        try:
            self.auth_policy.check_node_allowed(sender_id)
        except UnauthorizedError:
            return False

        try:
            unsigned = {k: v for k, v in beacon.items() if k != "signature"}
            canon = json_dumps(unsigned).encode("utf-8")
            if self.keys.shared_secret:
                return self.keys.verify_bytes(canon, sig)

            pub_key = self._resolve_sender_public_key(sender_id, beacon.get("public_key"))
            if not pub_key or not self.keys.verify_bytes(canon, sig, public_key_hex=pub_key):
                return False
            if (
                sender_id not in self._verified_public_keys
                and sender_id not in self.config.security.trusted_public_keys
            ):
                self._verified_public_keys[sender_id] = pub_key
            return True
        except (AuthenticationError, Exception) as e:
            logger.debug("Beacon verification failed for %s: %s", sender_id[:8], e)
            return False

    def _resolve_sender_public_key(self, sender_id: str, advertised_key: Any) -> str:
        """Resolve and validate sender's Ed25519 public key from known sources or TOFU."""
        peer_state = self.peer_table.get(sender_id)
        pub_key = peer_state.info.public_key if peer_state and peer_state.info else None
        if pub_key in (None, "none", "hmac"):
            pub_key = self.config.security.trusted_public_keys.get(sender_id)
        if not pub_key:
            pub_key = self._verified_public_keys.get(sender_id)
        if not pub_key and advertised_key:
            if not self.config.security.allow_tofu:
                raise AuthenticationError("Unknown sender public key and TOFU is disabled")
            if not isinstance(advertised_key, str):
                raise AuthenticationError("Invalid public key encoding")
            try:
                pub_bytes = bytes.fromhex(advertised_key)
            except ValueError as e:
                raise AuthenticationError(f"Invalid public key encoding: {e}") from e
            expected_id = hashlib.sha1(pub_bytes).hexdigest()
            if expected_id != sender_id:
                raise AuthenticationError(
                    f"Sender ID {sender_id} does not match public key hash {expected_id}"
                )
            pub_key = advertised_key
        if not pub_key:
            raise AuthenticationError("Invalid or missing cryptographic signature")
        return pub_key

    def _verify_incoming_message(self, message: Message, allow_unauthenticated_ping: bool = False) -> None:
        """Validate sender identity, timestamps, anti-reflection, signature, authorization, and replay nonce."""
        if not isinstance(message.sender_id, str) or not message.sender_id.strip():
            raise AuthenticationError("Missing or invalid sender_id")
        if message.sender_id == self.node_id:
            raise AuthenticationError("Rejected reflected message from self")
        if message.receiver_id not in (None, self.node_id):
            raise AuthenticationError("Message receiver_id does not match local node")

        if not self._security_enabled:
            return
        if allow_unauthenticated_ping and message.msg_type == "ping" and not message.signature:
            return

        # 1. Timestamp window check before recording nonce (blocks stale DoS against nonce cache)
        self.replay_protector.check_timestamp(message)

        # 2. Cryptographic signature verification
        if self.keys.shared_secret:
            if not self.keys.verify_message(message):
                raise AuthenticationError("Invalid or missing cryptographic signature")
        else:
            pub_key = self._resolve_sender_public_key(message.sender_id, message.sender_public_key)
            if not self.keys.verify_message(message, public_key_hex=pub_key):
                raise AuthenticationError("Invalid or missing cryptographic signature")
            if (
                message.sender_public_key
                and message.sender_id not in self._verified_public_keys
                and message.sender_id not in self.config.security.trusted_public_keys
            ):
                self._verified_public_keys[message.sender_id] = pub_key

        # 3. Node authorization policy
        self.auth_policy.check_node_allowed(message.sender_id)

        # 4. Post-authentication replay nonce recording
        self.replay_protector.validate(message)

    def register_handler(
        self,
        msg_type: str,
        handler: Callable[[Message], Coroutine[Any, Any, Message | None]],
    ) -> None:
        """Register a subsystem handler for incoming message types."""
        self._custom_handlers[msg_type] = handler

    async def start(self) -> None:
        """Start the node: bind TCP and UDP servers, initialize subsystems."""
        if self.status == NodeStatus.ONLINE:
            return

        logger.info("Starting MeshWeaver node %s (%s)...", self.node_id[:8], self.config.name)
        restored_retrying_tasks: list[Task] = []

        try:
            # 0. Restore durable state if persistent storage is configured
            if self.store:
                stored_identity = await self.store.load_identity()
                is_asymmetric = self._security_enabled and not self.config.security.shared_secret
                persist_priv = None if self.config.security.private_key_path else self.keys.private_key_pem

                if stored_identity and not os.getenv("MESHWEAVER_NODE_ID") and not self.config.security.private_key_path:
                    restored_id = stored_identity["node_id"]
                    restored_keys: KeyManager | None = None
                    stored_priv_pem = stored_identity.get("private_key")
                    can_restore = True

                    if is_asymmetric:
                        can_restore = False
                        if stored_priv_pem:
                            try:
                                candidate_keys = KeyManager(private_key_pem=stored_priv_pem)
                                if candidate_keys.public_key_hex not in ("none", "hmac"):
                                    expected_id = hashlib.sha1(
                                        bytes.fromhex(candidate_keys.public_key_hex)
                                    ).hexdigest()
                                    if expected_id == restored_id:
                                        restored_keys = candidate_keys
                                        can_restore = True
                            except Exception as e:
                                logger.warning("Ignoring invalid persisted private key in store: %s", e)
                    elif not self.config.security.shared_secret and stored_priv_pem:
                        try:
                            restored_keys = KeyManager(private_key_pem=stored_priv_pem)
                        except Exception as e:
                            logger.warning("Ignoring invalid persisted private key in store: %s", e)

                    if can_restore:
                        self._apply_identity(restored_id, keys=restored_keys)
                        logger.info("Restored persisted node identity %s from store", self.node_id[:8])
                    else:
                        logger.warning(
                            "Stored identity %s is incompatible with current security mode; saving current identity %s",
                            restored_id[:8],
                            self.node_id[:8],
                        )
                        await self.store.save_identity(
                            self.node_id,
                            self.config.name,
                            self.info.public_key,
                            private_key=persist_priv,
                        )
                else:
                    await self.store.save_identity(
                        self.node_id,
                        self.config.name,
                        self.info.public_key,
                        private_key=persist_priv,
                    )

                # Restore incomplete tasks from database
                saved_tasks = await self.store.list_incomplete_tasks()
                for t in saved_tasks:
                    if t.task_id not in self.tasks:
                        if t.state in (TaskState.ASSIGNED, TaskState.RUNNING):
                            t.transition_to(TaskState.RETRYING, reason="Restored from persistent store after reboot")
                            await self.store.save_task(t)
                        self._store_task(t)
                        if t.state == TaskState.RETRYING:
                            restored_retrying_tasks.append(t)

            # 1. Start TCP Server
            await self.tcp_server.start()
            if self.config.network.tcp_port == 0:
                self.info.tcp_port = self.tcp_server.actual_port
                self.kademlia.tcp_port = self.tcp_server.actual_port
                self.discovery.tcp_port = self.tcp_server.actual_port

            # 2. Start UDP Endpoint
            await self.udp_endpoint.start()
            if self.config.network.udp_port == 0:
                self.info.udp_port = self.udp_endpoint.actual_port
                self.kademlia.udp_port = self.udp_endpoint.actual_port
                self.discovery.udp_port = self.udp_endpoint.actual_port

            # 3. Start Discovery & Gossip Subsystems
            if self.config.discovery.enabled:
                await self.discovery.start()

            if self.config.gossip.enabled:
                await self.gossip.start()

            # 4. Start Heartbeat Monitor
            await self.heartbeat.start()

            self.status = NodeStatus.ONLINE
            await self.events.emit(EVENT_NODE_STATE_CHANGED, {"status": self.status.value, "node_id": self.node_id})

            if restored_retrying_tasks:
                resume_task = asyncio.create_task(self._resume_restored_tasks(restored_retrying_tasks))
                self._running_tasks.add(resume_task)
                resume_task.add_done_callback(self._running_tasks.discard)

            logger.info(
                "MeshWeaver node %s ONLINE (TCP: %s:%d, UDP: %s:%d)",
                self.node_id[:8],
                self.info.host,
                self.info.tcp_port,
                self.info.host,
                self.info.udp_port,
            )
        except Exception:
            await self.stop()
            raise

    async def _resume_restored_tasks(self, tasks_to_resume: list[Task]) -> None:
        """Resume RETRYING tasks restored from persistent storage shortly after startup."""
        await asyncio.sleep(0.2)
        for task in tasks_to_resume:
            if self.status != NodeStatus.ONLINE:
                break
            if task.state != TaskState.RETRYING:
                continue
            try:
                await self.submit_task(task)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("Failed to resume restored task %s: %s", task.task_id[:8], e)

    async def stop(self) -> None:
        """Gracefully shut down the node and networking subsystems."""
        if self.status == NodeStatus.STOPPED:
            return

        logger.info("Stopping MeshWeaver node %s...", self.node_id[:8])
        self.status = NodeStatus.STOPPED

        # Stop recovery manager before cancelling background tasks/futures
        await self.recovery.stop()

        # Cancel background tasks and in-flight RPCs/futures
        for rpc_task in list(self._inflight_rpc_tasks.values()):
            if not rpc_task.done():
                rpc_task.cancel()
        self._inflight_rpc_tasks.clear()

        for fut in list(self._task_futures.values()):
            if not fut.done():
                fut.cancel()
        self._task_futures.clear()

        for task in list(self._running_tasks):
            task.cancel()
        if self._running_tasks:
            await asyncio.gather(*self._running_tasks, return_exceptions=True)
            self._running_tasks.clear()

        # Stop subsystems
        await self.heartbeat.stop()
        await self.gossip.stop()
        await self.discovery.stop()

        # Stop networking
        await self.tcp_server.stop()
        await self.tcp_client.close()
        await self.udp_endpoint.stop()

        await self.events.emit(EVENT_NODE_STATE_CHANGED, {"status": self.status.value, "node_id": self.node_id})
        await self.events.cancel_pending()

        if self.store:
            await self.store.aclose()

        logger.info("MeshWeaver node %s stopped cleanly.", self.node_id[:8])

    async def ping_tcp(
        self,
        host: str,
        port: int,
        message: Message | float | None = None,
        timeout: float = 5.0,
    ) -> Message:
        """Send a PING or custom heartbeat message to a peer over TCP and measure round-trip response."""
        if isinstance(message, (int, float)):
            timeout = float(message)
            message = None

        start_time = utc_now()
        ping_msg = message or Message.ping(
            sender_id=self.node_id,
            payload={"node_info": self.info.to_dict(), "sent_at": start_time},
        )
        self._sign_message_if_enabled(ping_msg)

        response = await self.tcp_client.send_and_receive(host, port, ping_msg, timeout=timeout)
        if self._security_enabled and response.msg_type == "pong":
            self._verify_incoming_message(response)

        rtt_ms = (utc_now() - start_time) * 1000.0

        # Update peer table if response includes peer info
        sender_id = response.sender_id
        if sender_id and sender_id != self.node_id and response.msg_type != "error":
            peer_info_data = response.payload.get("node_info")
            if peer_info_data:
                peer_info = NodeInfo.from_dict(peer_info_data)
                self.peer_table.update_peer(peer_info, rtt_ms=rtt_ms)

        return response

    def _cancel_inflight_task(self, task_id: str, dead_node_id: str) -> None:
        """Cancel an in-flight RPC for a task assigned to a dead peer and exclude that peer."""
        if dead_node_id:
            self._excluded_peers.setdefault(task_id, set()).add(dead_node_id)
        rpc_task = self._inflight_rpc_tasks.get(task_id)
        if rpc_task is not None and not rpc_task.done():
            rpc_task.cancel()

    def _resolve_task_future(
        self,
        task_id: str,
        result: TaskResult | None,
        exc: Exception | None,
    ) -> None:
        """Resolve an awaiting caller future when RecoveryManager finishes recovering a task."""
        fut = self._task_futures.pop(task_id, None)
        if fut is not None and not fut.done():
            if exc is not None:
                fut.set_exception(exc)
            elif result is not None:
                fut.set_result(result)
            else:
                fut.set_exception(NetworkError(f"Task {task_id} recovery completed without result"))

        task = self.tasks.get(task_id)
        if task is not None and task.is_terminal:
            self._excluded_peers.pop(task_id, None)
            if self.store:
                bg = asyncio.create_task(self.store.save_task(task))
                self._running_tasks.add(bg)

                def _on_save_done(t: asyncio.Task[Any], tid: str = task_id) -> None:
                    self._running_tasks.discard(t)
                    if t.cancelled():
                        return
                    err = t.exception()
                    if err is not None:
                        logger.warning("Failed to persist terminal task %s: %s", tid[:8], err)

                bg.add_done_callback(_on_save_done)

    async def _persist_task(self, task: Task) -> None:
        if self.store:
            await self.store.save_task(task)

    async def submit_task(self, task: Task) -> TaskResult:
        """Intelligently schedule a task across the mesh to the optimal peer."""
        task.origin_node_id = self.node_id
        is_new = task.task_id not in self.tasks
        self._store_task(task)
        if is_new:
            await self._persist_task(task)
            await self.events.emit(EVENT_TASK_CREATED, {"task_id": task.task_id, "name": task.name})

        excluded = self._excluded_peers.get(task.task_id)
        selected_peer, is_local = self.scheduler.select_peer_for_task(
            task,
            active_local_tasks=self.executor.active_task_count,
            excluded_peers=excluded,
        )

        if not selected_peer:
            err_msg = "No available peer with compute capacity in the mesh"
            task.error = err_msg
            task.transition_to(TaskState.FAILED, reason=err_msg)
            await self._persist_task(task)
            await self.events.emit(EVENT_TASK_FAILED, {"task_id": task.task_id, "error": err_msg})
            self._excluded_peers.pop(task.task_id, None)
            raise SchedulingError(err_msg)

        reserved_peer_id = selected_peer.info.node_id
        self.scheduler.reserve_peer(reserved_peer_id)

        try:
            task.assigned_node_id = reserved_peer_id
            if task.state in (TaskState.CREATED, TaskState.QUEUED, TaskState.RETRYING):
                task.transition_to(TaskState.ASSIGNED, reason=f"Scheduled on {reserved_peer_id[:8]}")
            await self._persist_task(task)
            await self.events.emit(
                EVENT_TASK_ASSIGNED,
                {"task_id": task.task_id, "assigned_node_id": task.assigned_node_id, "is_local": is_local},
            )

            if is_local:
                logger.info("Executing task %s locally on %s", task.task_id[:8], self.node_id[:8])
                await self.events.emit(EVENT_TASK_STARTED, {"task_id": task.task_id, "name": task.name})
                result = await self.executor.execute_task(task)
                await self._persist_task(task)
                if result.is_success:
                    await self.events.emit(EVENT_TASK_COMPLETED, {"task_id": task.task_id, "result": result.result})
                else:
                    await self.events.emit(EVENT_TASK_FAILED, {"task_id": task.task_id, "error": result.error})
                self._excluded_peers.pop(task.task_id, None)
                return result

            logger.info(
                "Routing task %s to remote peer %s (%s:%d)",
                task.task_id[:8],
                selected_peer.info.node_id[:8],
                selected_peer.info.host,
                selected_peer.info.tcp_port,
            )
            return await self.submit_task_to_peer(
                host=selected_peer.info.host,
                port=selected_peer.info.tcp_port,
                task=task,
            )
        finally:
            self.scheduler.release_peer(reserved_peer_id)

    async def submit_task_to_peer(
        self,
        host: str,
        port: int,
        task: Task,
        timeout: float | None = None,
    ) -> TaskResult:
        """Submit a task to a remote peer over TCP and await the result."""
        task.origin_node_id = self.node_id
        effective_timeout = timeout if timeout is not None else (task.timeout + 10.0)

        is_new = task.task_id not in self.tasks
        self._store_task(task)
        if is_new:
            await self._persist_task(task)
            await self.events.emit(EVENT_TASK_CREATED, {"task_id": task.task_id, "name": task.name})

        if task.state in (TaskState.CREATED, TaskState.QUEUED, TaskState.RETRYING):
            task.transition_to(TaskState.ASSIGNED, reason=f"Dispatched to {host}:{port}")
            await self._persist_task(task)

        loop = asyncio.get_running_loop()
        is_future_owner = task.task_id not in self._task_futures
        caller_future: asyncio.Future[TaskResult]
        if is_future_owner:
            caller_future = loop.create_future()
            self._task_futures[task.task_id] = caller_future
        else:
            caller_future = self._task_futures[task.task_id]

        req_msg = Message(
            sender_id=self.node_id,
            receiver_id=task.assigned_node_id,
            msg_type="task_request",
            payload=task.to_dict(),
        )
        self._sign_message_if_enabled(req_msg)

        rpc_task = asyncio.create_task(
            self.tcp_client.send_and_receive(
                host,
                port,
                req_msg,
                timeout=effective_timeout,
            )
        )
        self._inflight_rpc_tasks[task.task_id] = rpc_task

        try:
            try:
                response = await rpc_task
            except asyncio.CancelledError:
                if (
                    is_future_owner
                    and self.status != NodeStatus.STOPPED
                    and (task.task_id in self.recovery._recovering_task_ids or not caller_future.done())
                ):
                    if task.task_id in self.recovery._recovering_task_ids:
                        return await caller_future
                raise
            except Exception as e:
                if is_future_owner and task.task_id in self.recovery._recovering_task_ids:
                    return await caller_future

                logger.warning(
                    "Task %s dispatch to %s:%d failed (%s: %s)",
                    task.task_id[:8],
                    host,
                    port,
                    type(e).__name__,
                    e,
                )
                failed_peer_id = task.assigned_node_id
                if failed_peer_id:
                    self._excluded_peers.setdefault(task.task_id, set()).add(failed_peer_id)
                    peer_state = self.peer_table.get(failed_peer_id)
                    if peer_state:
                        peer_state.mark_failed()

                if task.is_terminal:
                    if is_future_owner:
                        self._task_futures.pop(task.task_id, None)
                    return TaskResult(
                        task_id=task.task_id,
                        state=task.state,
                        result=task.result,
                        error=task.error,
                        execution_time_sec=task.execution_time_sec,
                        worker_node_id=task.assigned_node_id or "",
                    )

                if (
                    not is_future_owner
                    and failed_peer_id
                    and self.recovery._recovering_tasks.get(task.task_id) == failed_peer_id
                ):
                    raise asyncio.CancelledError

                if task.retry_count < task.max_retries:
                    task.retry_count += 1
                    task.assigned_node_id = None
                    delay = self.recovery.retry_policy.get_delay(task.retry_count)
                    logger.info(
                        "Retrying task %s (attempt %d/%d) in %.2fs...",
                        task.task_id[:8],
                        task.retry_count,
                        task.max_retries,
                        delay,
                    )
                    task.transition_to(
                        TaskState.RETRYING,
                        reason=f"Peer {host}:{port} communication error: {e}",
                    )
                    await self._persist_task(task)
                    await self.events.emit(EVENT_TASK_RETRIED, {"task_id": task.task_id, "attempt": task.retry_count})
                    await asyncio.sleep(delay)
                    return await self.submit_task(task)

                err_text = f"Connection to peer {host}:{port} failed: {e}"
                task.error = err_text
                task.transition_to(TaskState.FAILED, reason=err_text)
                await self._persist_task(task)
                await self.events.emit(EVENT_TASK_FAILED, {"task_id": task.task_id, "error": err_text})
                self._excluded_peers.pop(task.task_id, None)
                if is_future_owner:
                    self._task_futures.pop(task.task_id, None)
                return TaskResult(
                    task_id=task.task_id,
                    state=TaskState.FAILED,
                    error=err_text,
                    worker_node_id=failed_peer_id or "",
                )
        finally:
            if self._inflight_rpc_tasks.get(task.task_id) is rpc_task:
                self._inflight_rpc_tasks.pop(task.task_id, None)
            if is_future_owner and task.task_id not in self.recovery._recovering_task_ids:
                self._task_futures.pop(task.task_id, None)

        if task.is_terminal:
            if is_future_owner:
                self._task_futures.pop(task.task_id, None)
            return TaskResult(
                task_id=task.task_id,
                state=task.state,
                result=task.result,
                error=task.error,
                execution_time_sec=task.execution_time_sec,
                worker_node_id=task.assigned_node_id or "",
            )

        if task.state == TaskState.RETRYING:
            if is_future_owner and task.task_id in self.recovery._recovering_task_ids:
                return await caller_future
            if is_future_owner:
                self._task_futures.pop(task.task_id, None)
            return TaskResult(
                task_id=task.task_id,
                state=task.state,
                result=task.result,
                error=task.error or "Task state moved to RETRYING while awaiting response",
                worker_node_id=task.assigned_node_id or "",
            )

        if response.msg_type == "error":
            err_text = response.payload.get("error", "Unknown remote execution error")
            task.error = err_text
            task.transition_to(TaskState.FAILED, reason=err_text)
            await self._persist_task(task)
            await self.events.emit(EVENT_TASK_FAILED, {"task_id": task.task_id, "error": err_text})
            self._excluded_peers.pop(task.task_id, None)
            if is_future_owner:
                self._task_futures.pop(task.task_id, None)
            return TaskResult(
                task_id=task.task_id,
                state=TaskState.FAILED,
                error=err_text,
                worker_node_id=response.sender_id,
            )

        if self._security_enabled and response.msg_type == "task_response":
            try:
                self._verify_incoming_message(response)
            except SecurityError as e:
                err_text = str(e)
                task.error = err_text
                task.transition_to(TaskState.FAILED, reason=err_text)
                await self._persist_task(task)
                await self.events.emit(EVENT_TASK_FAILED, {"task_id": task.task_id, "error": err_text})
                self._excluded_peers.pop(task.task_id, None)
                if is_future_owner:
                    self._task_futures.pop(task.task_id, None)
                return TaskResult(
                    task_id=task.task_id,
                    state=TaskState.FAILED,
                    error=err_text,
                    worker_node_id=response.sender_id,
                )

        result_data = response.payload.get("result")
        if not result_data:
            result_data = response.payload

        task_result = TaskResult.from_dict(result_data)
        task.result = task_result.result
        task.error = task_result.error
        task.assigned_node_id = task_result.worker_node_id

        if task_result.is_success:
            task.transition_to(TaskState.COMPLETED, reason="Remote execution completed")
            task.execution_time_sec = task_result.execution_time_sec
            await self._persist_task(task)
            await self.events.emit(EVENT_TASK_COMPLETED, {"task_id": task.task_id, "result": task.result})
        else:
            task.transition_to(TaskState.FAILED, reason=task_result.error or "Remote execution failed")
            task.execution_time_sec = task_result.execution_time_sec
            await self._persist_task(task)
            await self.events.emit(EVENT_TASK_FAILED, {"task_id": task.task_id, "error": task.error})

        self._excluded_peers.pop(task.task_id, None)
        if is_future_owner:
            self._task_futures.pop(task.task_id, None)
        return task_result

    async def _send_rpc(self, host: str, port: int, msg: Message, timeout: float = 3.0) -> Message | None:
        """Send a Kademlia RPC over TCP and await response."""
        try:
            self._sign_message_if_enabled(msg)
            resp = await self.tcp_client.send_and_receive(host, port, msg, timeout=timeout)
            if resp is not None and self._security_enabled and resp.msg_type == "kad_rpc":
                self._verify_incoming_message(resp)
            return resp
        except Exception as e:
            logger.debug("RPC to %s:%d failed: %s", host, port, e)
            return None

    async def _handle_tcp_message(self, message: Message, writer: asyncio.StreamWriter) -> Message | None:
        """Handle incoming framed TCP message."""
        logger.debug(
            "Received TCP message %s from %s",
            message.msg_type,
            message.sender_id[:8] if message.sender_id else "unknown",
        )

        try:
            self._verify_incoming_message(message)
        except Exception as e:
            sender_preview = message.sender_id[:8] if isinstance(message.sender_id, str) else "unknown"
            logger.warning("Security check failed for message %s from %s: %s", message.msg_type, sender_preview, e)
            err_reply = Message.error(
                sender_id=self.node_id,
                receiver_id=message.sender_id if isinstance(message.sender_id, str) else None,
                code="UNAUTHORIZED",
                message=str(e),
            )
            self._sign_message_if_enabled(err_reply)
            return err_reply

        reply: Message | None = None
        if message.msg_type == "ping":
            reply = Message.pong(
                sender_id=self.node_id,
                receiver_id=message.sender_id,
                payload={"node_info": self.info.to_dict(), "ack": True},
            )
        elif message.msg_type == "heartbeat":
            reply = Message.pong(
                sender_id=self.node_id,
                receiver_id=message.sender_id,
                payload={"ack": True},
            )
        elif message.msg_type == "task_request":
            reply = await self._handle_task_request(message)
        elif message.msg_type in self._custom_handlers:
            reply = await self._custom_handlers[message.msg_type](message)
        else:
            logger.debug("Unhandled TCP message type: %s", message.msg_type)

        if reply is not None:
            self._sign_message_if_enabled(reply)
        return reply

    async def _handle_task_request(self, message: Message) -> Message:
        """Handle remote task request and execute on this node."""
        try:
            task_dict = message.payload
            task = Task.from_network_dict(
                task_dict,
                max_timeout=self.config.scheduler.max_task_timeout,
            )
            if task.task_type == TaskType.FUNCTION and not self.config.security.allow_cloudpickle:
                raise SecurityError(
                    "Rejecting incoming FUNCTION task: cloudpickle execution is disabled on this node for security"
                )

            # Check task execution permissions
            self.auth_policy.check_task_permission(message.sender_id, task)

            # Deduplication check for at-most-once / idempotent execution
            cached_result = self.dedup.get_result(task.task_id)
            if cached_result:
                logger.info("Returning deduplicated cached result for task %s", task.task_id[:8])
                return Message(
                    sender_id=self.node_id,
                    receiver_id=message.sender_id,
                    msg_type="task_response",
                    payload={"result": cached_result},
                )

            # Check if task is already in-flight (concurrent duplicate)
            if not self.dedup.record_started(task.task_id):
                logger.info("Task %s is already in-flight; awaiting existing execution", task.task_id[:8])
                evt = self._task_events.get(task.task_id)
                if evt:
                    try:
                        await asyncio.wait_for(evt.wait(), timeout=task.timeout)
                        cached_result = self.dedup.get_result(task.task_id)
                        if cached_result:
                            return Message(
                                sender_id=self.node_id,
                                receiver_id=message.sender_id,
                                msg_type="task_response",
                                payload={"result": cached_result},
                            )
                    except (asyncio.TimeoutError, Exception) as e:
                        logger.warning("Timed out or failed while awaiting in-flight task %s: %s", task.task_id[:8], e)
                return Message.error(
                    sender_id=self.node_id,
                    receiver_id=message.sender_id,
                    code="TASK_IN_PROGRESS",
                    message=f"Task {task.task_id} is already in-flight",
                )

            evt = asyncio.Event()
            self._task_events[task.task_id] = evt
            self._store_task(task)
            recorded_completed = False

            try:
                await self.events.emit(EVENT_TASK_STARTED, {"task_id": task.task_id, "name": task.name})
                result = await self.executor.execute_task(task)

                result_dict = result.to_dict()
                reply = Message(
                    sender_id=self.node_id,
                    receiver_id=message.sender_id,
                    msg_type="task_response",
                    payload={"result": result_dict},
                )
                self._sign_message_if_enabled(reply)
                # Validate wire serialization before caching/persisting
                reply.to_frame()

                if result.is_success:
                    self.dedup.record_completed(task.task_id, result_dict)
                    recorded_completed = True

                await self._persist_task(task)

                if result.is_success:
                    await self.events.emit(EVENT_TASK_COMPLETED, {"task_id": task.task_id, "result": result.result})
                else:
                    await self.events.emit(EVENT_TASK_FAILED, {"task_id": task.task_id, "error": result.error})

                return reply
            finally:
                if not recorded_completed:
                    self.dedup.clear_started(task.task_id)
                evt.set()
                self._task_events.pop(task.task_id, None)
        except Exception as e:
            logger.error("Failed to process task request: %s", e, exc_info=True)
            return Message.error(
                sender_id=self.node_id,
                receiver_id=message.sender_id,
                code="TASK_EXECUTION_ERROR",
                message=str(e),
            )

    async def _handle_udp_message(self, message: Message, addr: tuple[str, int]) -> Message | None:
        """Handle incoming UDP datagram message."""
        logger.debug(
            "Received UDP message %s from %s (%s)",
            message.msg_type,
            message.sender_id[:8] if message.sender_id else "unknown",
            addr,
        )

        try:
            self._verify_incoming_message(message)
        except Exception as e:
            sender_preview = message.sender_id[:8] if isinstance(message.sender_id, str) else "unknown"
            logger.warning("UDP security check failed for %s from %s (%s): %s", message.msg_type, sender_preview, addr, e)
            return None

        reply: Message | None = None
        if message.msg_type == "ping":
            reply = Message.pong(
                sender_id=self.node_id,
                receiver_id=message.sender_id,
                payload={"node_info": self.info.to_dict(), "ack": True},
            )
        elif message.msg_type == "gossip":
            await self.gossip.handle_gossip_message(message)
            return None
        elif message.msg_type in self._custom_handlers:
            reply = await self._custom_handlers[message.msg_type](message)

        if reply is not None:
            self._sign_message_if_enabled(reply)
        return reply
