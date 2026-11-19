"""Peer discovery coordinator: bootstrap list, LAN broadcast beacon, and DHT."""

import asyncio
import logging
import socket
from typing import Any
from meshweaver.discovery.kademlia import KademliaNode
from meshweaver.discovery.routing_table import Contact
from meshweaver.networking.protocol import Message
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
    ) -> None:
        self.node_id = node_id
        self.host = host
        self.tcp_port = tcp_port
        self.udp_port = udp_port
        self.kademlia = kademlia
        self.bootstrap_peers = bootstrap_peers or []
        self.enable_lan_broadcast = enable_lan_broadcast
        self.broadcast_port = broadcast_port

        self._is_running = False
        self._tasks: list[asyncio.Task] = []
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
        logger.info("Peer discovery service stopped.")

    async def _bootstrap_loop(self) -> None:
        """Perform initial bootstrap and periodic refresh."""
        contacts: list[Contact] = []
        for peer_str in self.bootstrap_peers:
            try:
                parts = peer_str.split(":")
                host = parts[0]
                tcp_port = int(parts[1])
                udp_port = int(parts[2]) if len(parts) > 2 else tcp_port + 1
                import hashlib
                seed_id = hashlib.sha1(f"seed:{host}:{tcp_port}".encode("utf-8")).hexdigest()
                contacts.append(
                    Contact(
                        node_id=seed_id,
                        host=host,
                        tcp_port=tcp_port,
                        udp_port=udp_port,
                    )
                )
            except Exception as e:
                logger.warning("Invalid bootstrap peer %s: %s", peer_str, e)

        if contacts:
            await self.kademlia.bootstrap(contacts)

    def _start_broadcast_socket(self) -> None:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            if hasattr(socket, "SO_REUSEADDR"):
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
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
        beacon_payload = json_dumps({
            "type": "MSHW_BEACON",
            "node_id": self.node_id,
            "host": self.host,
            "tcp_port": self.tcp_port,
            "udp_port": self.udp_port,
        }).encode("utf-8")

        send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        send_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        send_sock.setblocking(False)

        try:
            while self._is_running:
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

    async def _lan_beacon_listener(self) -> None:
        """Listen for beacons from other nodes on the local subnet."""
        if not self._beacon_sock:
            return

        loop = asyncio.get_running_loop()
        while self._is_running:
            try:
                data, addr = await loop.sock_recvfrom(self._beacon_sock, 4096)
                msg = json_loads(data)
                if msg.get("type") == "MSHW_BEACON":
                    peer_node_id = msg.get("node_id")
                    if peer_node_id and peer_node_id != self.node_id:
                        peer_host = addr[0] if msg.get("host") in ("0.0.0.0", "127.0.0.1") else msg.get("host", addr[0])
                        contact = Contact(
                            node_id=peer_node_id,
                            host=peer_host,
                            tcp_port=int(msg["tcp_port"]),
                            udp_port=int(msg["udp_port"]),
                        )
                        self.kademlia.routing_table.add_contact(contact)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug("Beacon listener error: %s", e)
                await asyncio.sleep(1.0)
