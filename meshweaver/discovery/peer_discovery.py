"""Peer discovery coordinator: bootstrap list, LAN broadcast beacon, and DHT."""

import asyncio
import hashlib
import logging
import socket
from typing import Any, Callable
from meshweaver.discovery.kademlia import KademliaNode
from meshweaver.discovery.routing_table import Contact, _validate_node_id
from meshweaver.networking.protocol import Message
from meshweaver.utils.exceptions import ProtocolError
from meshweaver.utils.helpers import json_dumps, json_loads, utc_now

logger = logging.getLogger("meshweaver.discovery")


class PeerDiscoveryService:
    """Manages multi-modal peer discovery: LAN beacon, bootstrap, and DHT lookups."""

    def __init__(
        self,
        node_id: str,
        host: str,
        tcp_port: int,
        udp_port: int,
        kademlia: KademliaNode,
        bootstrap_peers: list[str] | None = None,
        enable_lan_broadcast: bool = True,
        broadcast_port: int = 52520,
        refresh_interval: float = 3600.0,
        sign_fn: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
        verify_fn: Callable[[dict[str, Any]], bool] | None = None,
    ) -> None:
        self.node_id = node_id
        self.host = host
        self.tcp_port = tcp_port
        self.udp_port = udp_port
        self.kademlia = kademlia
        self.bootstrap_peers = bootstrap_peers or []
        self.enable_lan_broadcast = enable_lan_broadcast
        self.broadcast_port = broadcast_port
        self.refresh_interval = refresh_interval
        self.sign_fn = sign_fn
        self.verify_fn = verify_fn

        self._is_running = False
        self._tasks: list[asyncio.Task] = []
        self._pending_pings: set[str] = set()
        self._beacon_sock: socket.socket | None = None

    async def start(self) -> None:
        """Start discovery background tasks."""
        if self._is_running:
            return
        self._is_running = True

        # 1. Connect to static bootstrap peers if configured
        if self.bootstrap_peers:
            self._tasks.append(asyncio.create_task(self._bootstrap_loop()))

        # 2. Start LAN broadcast listener and sender if enabled
        if self.enable_lan_broadcast:
            self._start_broadcast_socket()
            self._tasks.append(asyncio.create_task(self._lan_beacon_sender()))
            self._tasks.append(asyncio.create_task(self._lan_beacon_listener()))

        logger.info("Peer discovery service started (LAN beacon: %s)", self.enable_lan_broadcast)

    async def stop(self) -> None:
        """Stop discovery background tasks."""
        self._is_running = False
        for t in self._tasks:
            t.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
            self._tasks.clear()

        if self._beacon_sock:
            try:
                self._beacon_sock.close()
            except Exception:
                pass
            self._beacon_sock = None
        self._pending_pings.clear()
        logger.info("Peer discovery service stopped.")

    async def _bootstrap_loop(self) -> None:
        """Perform initial bootstrap with exponential backoff and periodic bucket refresh."""
        contacts: list[Contact] = []
        for peer_str in self.bootstrap_peers:
            try:
                parts = peer_str.split(":")
                host = parts[0]
                tcp_port = int(parts[1])
                udp_port = int(parts[2]) if len(parts) > 2 else tcp_port + 1
                seed_id = hashlib.sha1(f"seed:{host}:{tcp_port}".encode("utf-8")).hexdigest()
                contacts.append(
                    Contact(
                        node_id=seed_id,
                        host=host,
                        tcp_port=tcp_port,
                        udp_port=udp_port,
                        is_seed=True,
                    )
                )
            except Exception as e:
                logger.warning("Invalid bootstrap peer %s: %s", peer_str, e)

        if not contacts:
            return

        attempt = 0
        while self._is_running:
            try:
                successful = await self.kademlia.bootstrap(contacts)
                if successful == 0:
                    backoff = min(2.0 * (2 ** attempt), 60.0)
                    attempt += 1
                    logger.debug("Bootstrap found 0 peers; retrying in %.1fs", backoff)
                    await asyncio.sleep(backoff)
                    continue

                attempt = 0
                await asyncio.sleep(self.refresh_interval)
                if self._is_running:
                    await self.kademlia.iterative_find_node(self.node_id)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug("Bootstrap/refresh loop error: %s", e)
                await asyncio.sleep(5.0)

    def _start_broadcast_socket(self) -> None:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            if hasattr(socket, "SO_REUSEADDR"):
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if hasattr(socket, "SO_REUSEPORT"):
                try:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
                except OSError:
                    pass
            sock.setblocking(False)
            try:
                sock.bind(("0.0.0.0", self.broadcast_port))
                self._beacon_sock = sock
            except OSError as e:
                logger.warning("Could not bind broadcast port %d: %s (beacon listening disabled)", self.broadcast_port, e)
                sock.close()
                self._beacon_sock = None
        except Exception as e:
            logger.warning("Failed to initialize broadcast socket: %s", e)

    async def _lan_beacon_sender(self) -> None:
        """Periodically broadcast discovery beacon to local subnet."""
        loop = asyncio.get_running_loop()

        send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        send_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        send_sock.setblocking(False)

        try:
            while self._is_running:
                beacon_dict: dict[str, Any] = {
                    "type": "MSHW_BEACON",
                    "node_id": self.node_id,
                    "host": self.host,
                    "tcp_port": self.tcp_port,
                    "udp_port": self.udp_port,
                    "timestamp": utc_now(),
                }
                if self.sign_fn:
                    beacon_dict = self.sign_fn(beacon_dict)
                beacon_payload = json_dumps(beacon_dict).encode("utf-8")

                try:
                    await loop.sock_sendto(send_sock, beacon_payload, ("<broadcast>", self.broadcast_port))
                except OSError:
                    # Fallback to local subnet broadcast if <broadcast> fails
                    try:
                        await loop.sock_sendto(send_sock, beacon_payload, ("255.255.255.255", self.broadcast_port))
                    except Exception:
                        pass
                except Exception as e:
                    logger.debug("Beacon broadcast error: %s", e)
                await asyncio.sleep(10.0)
        finally:
            send_sock.close()

    def _schedule_beacon_ping(self, contact: Contact) -> None:
        """Verify an unauthenticated LAN beacon endpoint via Kademlia PING."""
        if contact.node_id in self._pending_pings:
            return
        self._pending_pings.add(contact.node_id)

        async def _verify() -> None:
            try:
                await self.kademlia.ping(contact)
            except Exception as e:
                logger.debug("Beacon verification ping to %s failed: %s", contact.node_id[:8], e)

        task = asyncio.create_task(_verify())
        self._tasks.append(task)

        def _cleanup(t: asyncio.Task[Any]) -> None:
            self._pending_pings.discard(contact.node_id)
            try:
                self._tasks.remove(t)
            except ValueError:
                pass

        task.add_done_callback(_cleanup)

    async def _lan_beacon_listener(self) -> None:
        """Listen for beacons from other nodes on the local subnet."""
        if not self._beacon_sock:
            return

        loop = asyncio.get_running_loop()
        while self._is_running:
            try:
                data, addr = await loop.sock_recvfrom(self._beacon_sock, 4096)
                msg = json_loads(data)
                if not isinstance(msg, dict) or msg.get("type") != "MSHW_BEACON":
                    continue
                if self.verify_fn and not self.verify_fn(msg):
                    logger.debug("Rejected unverified LAN beacon from %s", addr)
                    continue

                peer_node_id = msg.get("node_id")
                if not peer_node_id or peer_node_id == self.node_id:
                    continue
                try:
                    _validate_node_id(peer_node_id)
                except ProtocolError:
                    continue

                peer_host = addr[0] if msg.get("host") in ("0.0.0.0", "127.0.0.1") else msg.get("host", addr[0])
                tcp_port = int(msg["tcp_port"])
                udp_port = int(msg["udp_port"])
                if not (1 <= tcp_port <= 65535 and 1 <= udp_port <= 65535):
                    continue

                contact = Contact(
                    node_id=peer_node_id,
                    host=str(peer_host),
                    tcp_port=tcp_port,
                    udp_port=udp_port,
                )
                existing = self.kademlia.routing_table.get_contact(peer_node_id)
                if existing is None or (existing.host, existing.tcp_port, existing.udp_port) != (
                    contact.host,
                    contact.tcp_port,
                    contact.udp_port,
                ):
                    self._schedule_beacon_ping(contact)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug("Beacon listener error: %s", e)
                await asyncio.sleep(1.0)
