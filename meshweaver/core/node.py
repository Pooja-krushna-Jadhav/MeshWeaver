"""Core MeshWeaver Node runtime."""

import asyncio
import hashlib
import logging
import os
from typing import Any, Callable, Coroutine
from meshweaver.core.config import NodeConfig
from meshweaver.core.events import (
    EVENT_GOSSIP_RECEIVED,
    EVENT_NODE_STATE_CHANGED,
    EVENT_PEER_CONNECTED,
    EVENT_PEER_DISCONNECTED,
    EVENT_TASK_ASSIGNED,
    EVENT_TASK_COMPLETED,
    EVENT_TASK_FAILED,
    EVENT_TASK_RETRIED,
    EVENT_TASK_STARTED,
    EventBus,
)
from meshweaver.core.state import NodeInfo, NodeStatus, PeerState
from meshweaver.discovery.kademlia import KademliaNode
from meshweaver.discovery.peer_discovery import PeerDiscoveryService
from meshweaver.discovery.routing_table import Contact
from meshweaver.execution.executor import TaskExecutor, TaskRegistry
from meshweaver.execution.results import TaskResult
from meshweaver.execution.serialization import deserialize_task, serialize_task
from meshweaver.execution.task import Task, TaskState
from meshweaver.gossip.gossip_protocol import GossipProtocol
from meshweaver.gossip.peer_state import PeerTable
from meshweaver.gossip.resource_monitor import ResourceMonitor
from meshweaver.networking.framing import FrameFlags
from meshweaver.reliability.failure_detector import FailureDetector
from meshweaver.reliability.heartbeat import HeartbeatMonitor
from meshweaver.reliability.recovery import RecoveryManager
from meshweaver.reliability.retry import DeduplicationRegistry
from meshweaver.scheduler.scheduler import TaskScheduler
from meshweaver.security.authentication import KeyManager, ReplayProtector
from meshweaver.security.permissions import AuthorizationPolicy
from meshweaver.storage.state_store import StateStore
from meshweaver.utils.exceptions import AuthenticationError, ReplayAttackError, SchedulingError, UnauthorizedError
from meshweaver.networking.protocol import Message
from meshweaver.networking.tcp import AsyncTCPClient, AsyncTCPServer
from meshweaver.networking.udp import AsyncUDPEndpoint
from meshweaver.utils.helpers import utc_now

logger = logging.getLogger("meshweaver.node")


class Node:
    """A decentralized MeshWeaver peer node participating in the mesh."""

    def __init__(self, config: NodeConfig | None = None) -> None:
        self.config = config or NodeConfig()
        self.node_id = self.config.node_id
        self.status = NodeStatus.INITIALIZING
        self.events = EventBus()

        # Security & Cryptography
        self.keys = KeyManager(shared_secret=self.config.security.shared_secret)
        self.replay_protector = ReplayProtector(window_sec=self.config.security.replay_window_sec)
        self.auth_policy = AuthorizationPolicy(
            allowed_nodes=self.config.security.allowed_nodes,
            allow_cloudpickle=self.config.security.allow_cloudpickle,
        )

        # In asymmetric mode, bind node_id to SHA-1 hash of public key if not explicitly set
        if self.config.security.require_signatures and not self.config.security.shared_secret and self.keys.public_key_hex != "none":
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
        )
        self.tcp_client = AsyncTCPClient(
            max_frame_size=self.config.network.max_frame_size,
            max_idle=self.config.network.connection_pool_size,
            ssl_context=client_ssl,
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
        )

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

        # Known peers and local tasks tables
        self.peers: dict[str, PeerState] = {}
        self.tasks: dict[str, Task] = {}
        self._task_events: dict[str, asyncio.Event] = {}
        self._running_tasks: set[asyncio.Task] = set()

        # Persistence & Reliability
        self.store = StateStore(db_path=self.config.storage.db_path) if self.config.storage.enabled else None
        self.failure_detector = FailureDetector()
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
        )

        # Message dispatch routing: msg_type -> async handler
        self._custom_handlers: dict[str, Callable[[Message], Coroutine[Any, Any, Message | None]]] = {
            "kad_rpc": self.kademlia.handle_rpc,
        }

    @property
    def tcp_address(self) -> tuple[str, int]:
        return (self.config.network.host, self.tcp_server.actual_port)

    @property
    def udp_address(self) -> tuple[str, int]:
        return (self.config.network.host, self.udp_endpoint.actual_port)

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

        # 0. Restore durable state if persistent storage is configured
        if self.store:
            stored_identity = await self.store.load_identity()
            if stored_identity and not os.getenv("MESHWEAVER_NODE_ID"):
                self.node_id = stored_identity["node_id"]
                self.info.node_id = self.node_id
                self.kademlia.local_node_id = self.node_id
                logger.info("Restored persisted node identity %s from store", self.node_id[:8])
            else:
                await self.store.save_identity(self.node_id, self.config.name, self.info.public_key)

            # Restore non-terminal tasks from database
            saved_tasks = await self.store.list_tasks(limit=100)
            for t in saved_tasks:
                if t.task_id not in self.tasks:
                    self.tasks[t.task_id] = t
                    if t.state in (TaskState.ASSIGNED, TaskState.RUNNING):
                        t.transition_to(TaskState.RETRYING, reason="Restored from persistent store after reboot")

        # 1. Start TCP Server
        await self.tcp_server.start()
        # Update advertised port if dynamic port 0 was specified
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
        logger.info(
            "MeshWeaver node %s ONLINE (TCP: %s:%d, UDP: %s:%d)",
            self.node_id[:8],
            self.info.host,
            self.info.tcp_port,
            self.info.host,
            self.info.udp_port,
        )

    async def stop(self) -> None:
        """Gracefully shut down the node and networking subsystems."""
        if self.status == NodeStatus.STOPPED:
            return

        logger.info("Stopping MeshWeaver node %s...", self.node_id[:8])
        self.status = NodeStatus.STOPPED

        # Cancel background tasks
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
        if (self.config.security.enabled or self.config.security.require_signatures) and not ping_msg.signature:
            self.keys.sign_message(ping_msg)

        response = await self.tcp_client.send_and_receive(host, port, ping_msg, timeout=timeout)
        rtt_ms = (utc_now() - start_time) * 1000.0

        # Update peer table if response includes peer info
        sender_id = response.sender_id
        if sender_id and sender_id != self.node_id:
            peer_info_data = response.payload.get("node_info")
            if peer_info_data:
                peer_info = NodeInfo.from_dict(peer_info_data)
                self.peer_table.update_peer(peer_info, rtt_ms=rtt_ms)
                p = self.peer_table.get(sender_id)
                if p:
                    self.peers[sender_id] = p

        return response

    async def submit_task(self, task: Task) -> TaskResult:
        """Intelligently schedule a task across the mesh to the optimal peer."""
        task.origin_node_id = self.node_id
        self.tasks[task.task_id] = task

        selected_peer, is_local = self.scheduler.select_peer_for_task(
            task,
            active_local_tasks=self.executor.active_task_count,
        )

        if not selected_peer:
            err_msg = "No available peer with compute capacity in the mesh"
            task.error = err_msg
            task.transition_to(TaskState.FAILED, reason=err_msg)
            raise SchedulingError(err_msg)

        task.assigned_node_id = selected_peer.info.node_id
        await self.events.emit(
            EVENT_TASK_ASSIGNED,
            {"task_id": task.task_id, "assigned_node_id": task.assigned_node_id, "is_local": is_local},
        )

        if is_local:
            logger.info("Executing task %s locally on %s", task.task_id[:8], self.node_id[:8])
            return await self.executor.execute_task(task)
        else:
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

    async def submit_task_to_peer(
        self,
        host: str,
        port: int,
        task: Task,
        timeout: float | None = None,
    ) -> TaskResult:
        """Submit a task to a remote peer over TCP and await the result."""
        task.origin_node_id = self.node_id
        effective_timeout = timeout or (task.timeout + 10.0)

        # Store task locally
        self.tasks[task.task_id] = task
        if task.state == TaskState.CREATED:
            task.transition_to(TaskState.ASSIGNED, reason=f"Dispatched to {host}:{port}")

        req_msg = Message(
            sender_id=self.node_id,
            receiver_id=task.assigned_node_id,
            msg_type="task_request",
            payload=task.to_dict(),
        )
        if self.config.security.enabled or self.config.security.require_signatures:
            self.keys.sign_message(req_msg)

        try:
            response = await self.tcp_client.send_and_receive(
                host,
                port,
                req_msg,
                timeout=effective_timeout,
            )
        except Exception as e:
            logger.warning(
                "Task %s dispatch to %s:%d failed (%s: %s)",
                task.task_id[:8], host, port, type(e).__name__, e
            )
            if task.assigned_node_id:
                peer_state = self.peer_table.get(task.assigned_node_id)
                if peer_state:
                    peer_state.mark_failed()

            if task.retry_count < task.max_retries:
                task.retry_count += 1
                task.assigned_node_id = None
                delay = self.recovery.retry_policy.get_delay(task.retry_count)
                logger.info(
                    "Retrying task %s (attempt %d/%d) in %.2fs...",
                    task.task_id[:8], task.retry_count, task.max_retries, delay
                )
                task.transition_to(
                    TaskState.RETRYING,
                    reason=f"Peer {host}:{port} communication error: {e}",
                )
                await self.events.emit(EVENT_TASK_RETRIED, {"task_id": task.task_id, "attempt": task.retry_count})
                await asyncio.sleep(delay)
                return await self.submit_task(task)

            err_text = f"Connection to peer {host}:{port} failed: {e}"
            task.error = err_text
            task.transition_to(TaskState.FAILED, reason=err_text)
            return TaskResult(
                task_id=task.task_id,
                state=TaskState.FAILED,
                error=err_text,
                worker_node_id=task.assigned_node_id or "",
            )

        if response.msg_type == "error":
            err_text = response.payload.get("error", "Unknown remote execution error")
            task.error = err_text
            task.transition_to(TaskState.FAILED, reason=err_text)
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
        else:
            task.transition_to(TaskState.FAILED, reason=task_result.error or "Remote execution failed")

        return task_result

    async def _send_rpc(self, host: str, port: int, msg: Message, timeout: float = 3.0) -> Message | None:
        """Send a Kademlia RPC over TCP and await response."""
        try:
            return await self.tcp_client.send_and_receive(host, port, msg, timeout=timeout)
        except Exception as e:
            logger.debug("RPC to %s:%d failed: %s", host, port, e)
            return None

    async def _handle_tcp_message(self, message: Message, writer: asyncio.StreamWriter) -> Message | None:
        """Handle incoming framed TCP message."""
        logger.debug("Received TCP message %s from %s", message.msg_type, message.sender_id[:8] if message.sender_id else "unknown")

        # Security validation when enabled
        if self.config.security.enabled or self.config.security.require_signatures:
            try:
                self.replay_protector.validate(message)
                if self.keys.shared_secret:
                    if not self.keys.verify_message(message):
                        raise AuthenticationError("Invalid or missing cryptographic signature")
                else:
                    peer_state = self.peer_table.get(message.sender_id)
                    pub_key = peer_state.info.public_key if peer_state and peer_state.info else None
                    if not pub_key:
                        pub_key = self.config.security.trusted_public_keys.get(message.sender_id)
                    if not pub_key and message.sender_public_key:
                        # Validate that public key hashes to sender_id to prevent identity spoofing
                        try:
                            pub_bytes = bytes.fromhex(message.sender_public_key)
                            expected_id = hashlib.sha1(pub_bytes).hexdigest()
                            if expected_id == message.sender_id:
                                pub_key = message.sender_public_key
                            else:
                                raise AuthenticationError(
                                    f"Sender ID {message.sender_id} does not match public key hash {expected_id}"
                                )
                        except Exception as e:
                            if isinstance(e, AuthenticationError):
                                raise
                            raise AuthenticationError(f"Invalid public key encoding: {e}")

                    if not pub_key or not self.keys.verify_message(message, public_key_hex=pub_key):
                        raise AuthenticationError("Invalid or missing cryptographic signature")

                    if message.sender_public_key and not peer_state:
                        sender_info = NodeInfo(node_id=message.sender_id, public_key=message.sender_public_key)
                        self.peer_table.update_peer(sender_info)

                self.auth_policy.check_node_allowed(message.sender_id)
            except Exception as e:
                logger.warning("Security check failed for message %s from %s: %s", message.msg_type, message.sender_id[:8], e)
                return Message.error(
                    sender_id=self.node_id,
                    receiver_id=message.sender_id,
                    code="UNAUTHORIZED",
                    message=str(e),
                )

        if message.msg_type == "ping":
            return Message.pong(
                sender_id=self.node_id,
                receiver_id=message.sender_id,
                payload={"node_info": self.info.to_dict(), "ack": True},
            )

        if message.msg_type == "heartbeat":
            return Message.pong(
                sender_id=self.node_id,
                receiver_id=message.sender_id,
                payload={"ack": True},
            )

        if message.msg_type == "task_request":
            return await self._handle_task_request(message)

        if message.msg_type in self._custom_handlers:
            return await self._custom_handlers[message.msg_type](message)

        logger.debug("Unhandled TCP message type: %s", message.msg_type)
        return None

    async def _handle_task_request(self, message: Message) -> Message:
        """Handle remote task request and execute on this node."""
        try:
            task_dict = message.payload
            task = deserialize_task(task_dict, allow_cloudpickle=self.config.security.allow_cloudpickle)

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
            self.tasks[task.task_id] = task

            try:
                await self.events.emit(EVENT_TASK_STARTED, {"task_id": task.task_id, "name": task.name})
                result = await self.executor.execute_task(task)

                # Record in deduplication cache
                self.dedup.record_completed(task.task_id, result.to_dict())

                # Persist to local durable store if enabled
                if self.store:
                    await self.store.save_task(task)

                if result.is_success:
                    await self.events.emit(EVENT_TASK_COMPLETED, {"task_id": task.task_id, "result": result.result})
                else:
                    await self.events.emit(EVENT_TASK_FAILED, {"task_id": task.task_id, "error": result.error})

                return Message(
                    sender_id=self.node_id,
                    receiver_id=message.sender_id,
                    msg_type="task_response",
                    payload={"result": result.to_dict()},
                )
            finally:
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
        logger.debug("Received UDP message %s from %s (%s)", message.msg_type, message.sender_id[:8] if message.sender_id else "unknown", addr)

        if message.msg_type == "ping":
            return Message.pong(
                sender_id=self.node_id,
                receiver_id=message.sender_id,
                payload={"node_info": self.info.to_dict(), "ack": True},
            )

        if message.msg_type == "gossip":
            await self.gossip.handle_gossip_message(message)
            return None

        if message.msg_type in self._custom_handlers:
            return await self._custom_handlers[message.msg_type](message)

        return None
