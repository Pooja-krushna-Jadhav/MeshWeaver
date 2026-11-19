"""Epidemic Gossip Protocol for exchanging CPU and RAM load vectors."""

import asyncio
import hashlib
import logging
import random
from typing import Any, Callable, Coroutine
from meshweaver.core.events import EVENT_GOSSIP_RECEIVED, EVENT_PEER_DISCONNECTED, EventBus
from meshweaver.core.state import NodeInfo, PeerHealth, ResourceMetrics
from meshweaver.discovery.kademlia import KademliaNode
from meshweaver.discovery.routing_table import Contact
from meshweaver.gossip.peer_state import PeerTable
from meshweaver.gossip.resource_monitor import ResourceMonitor
from meshweaver.networking.protocol import Message
from meshweaver.utils.helpers import utc_now

logger = logging.getLogger("meshweaver.gossip")


class GossipProtocol:
    """Manages randomized epidemic gossip rounds to propagate resource metrics."""

    def __init__(
        self,
        node_id: str,
        node_info: NodeInfo,
        peer_table: PeerTable,
        resource_monitor: ResourceMonitor,
        kademlia: KademliaNode,
        events: EventBus,
        send_udp_fn: Callable[[Message, tuple[str, int]], None],
        interval: float = 5.0,
        fanout: int = 3,
        stale_threshold: float = 15.0,
        dead_threshold: float = 30.0,
        purge_threshold: float = 90.0,
    ) -> None:
        self.node_id = node_id
        self.node_info = node_info
        self.peer_table = peer_table
        self.resource_monitor = resource_monitor
        self.kademlia = kademlia
        self.events = events
        self.send_udp = send_udp_fn
        self.interval = interval
        self.fanout = fanout
        self.stale_threshold = stale_threshold
        self.dead_threshold = dead_threshold
        self.purge_threshold = purge_threshold

        self._is_running = False
        self._gossip_task: asyncio.Task | None = None

    async def start(self) -> None:
        """Start the periodic gossip loop."""
        if self._is_running:
            return
        self._is_running = True
        self._gossip_task = asyncio.create_task(self._run_loop())
        logger.info("Gossip protocol engine started (interval: %.1fs, fanout: %d)", self.interval, self.fanout)

    async def stop(self) -> None:
        """Stop the periodic gossip loop."""
        self._is_running = False
        if self._gossip_task:
            self._gossip_task.cancel()
            try:
                await self._gossip_task
            except asyncio.CancelledError:
                pass
            self._gossip_task = None
        logger.info("Gossip protocol engine stopped.")

    async def _run_loop(self) -> None:
        while self._is_running:
            try:
                await self.do_gossip_round()
                # Prune stale/dead peers
                suspects, dead, purged = self.peer_table.check_stale(
                    stale_threshold=self.stale_threshold,
                    dead_threshold=self.dead_threshold,
                    purge_threshold=self.purge_threshold,
                )
                for dead_id in dead:
                    await self.events.emit(EVENT_PEER_DISCONNECTED, {"node_id": dead_id, "reason": "Dead heartbeat"})
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in gossip round: %s", e, exc_info=True)

            await asyncio.sleep(self.interval)

    async def do_gossip_round(self) -> None:
        """Execute one gossip cycle: collect local metrics and send to random peers."""
        # 1. Sample current local metrics
        local_metrics = self.resource_monitor.sample()

        # Update our entry in local peer table
        self.peer_table.update_peer(self.node_info, local_metrics)

        # 2. Gather candidates: all known peers from peer table + Kademlia contacts
        all_candidates: dict[str, tuple[str, int]] = {}
        for p in self.peer_table.all_peers():
            if p.info.node_id != self.node_id:
                all_candidates[p.info.node_id] = (p.info.host, p.info.udp_port)

        for c in self.kademlia.routing_table.all_contacts():
            if c.node_id != self.node_id:
                all_candidates[c.node_id] = (c.host, c.udp_port)

        if not all_candidates:
            return

        # 3. Select random fanout targets
        target_ids = list(all_candidates.keys())
        selected_ids = random.sample(target_ids, min(self.fanout, len(target_ids)))

        # 4. Pack peer states to gossip (self + up to 5 other known peers)
        peers_to_send: list[dict[str, Any]] = [
            {"info": self.node_info.to_dict(), "metrics": local_metrics.to_dict()}
        ]
        other_peers = [p for p in self.peer_table.all_peers() if p.info.node_id != self.node_id]
        if other_peers:
            sample_size = min(5, len(other_peers))
            for p in random.sample(other_peers, sample_size):
                peers_to_send.append({"info": p.info.to_dict(), "metrics": p.metrics.to_dict()})

        gossip_msg = Message.gossip(sender_id=self.node_id, peer_states=peers_to_send)

        for tid in selected_ids:
            addr = all_candidates[tid]
            try:
                self.send_udp(gossip_msg, addr)
            except Exception as e:
                logger.debug("Failed to send gossip to %s (%s): %s", tid[:8], addr, e)

    async def handle_gossip_message(self, message: Message) -> None:
        """Process an incoming gossip payload and update peer table."""
        peers_data = message.payload.get("peers", [])
        updated_nodes: list[str] = []

        for p_data in peers_data:
            info_data = p_data.get("info")
            metrics_data = p_data.get("metrics")
            if not info_data:
                continue

            peer_info = NodeInfo.from_dict(info_data)
            if peer_info.node_id == self.node_id:
                continue

            # Validate that peer public key cryptographically binds to node_id
            if peer_info.public_key:
                try:
                    pub_bytes = bytes.fromhex(peer_info.public_key)
                    expected_id = hashlib.sha1(pub_bytes).hexdigest()
                    if expected_id != peer_info.node_id:
                        logger.debug(
                            "Dropping unverified public key for peer %s (hash mismatch %s)",
                            peer_info.node_id[:8], expected_id[:8]
                        )
                        peer_info.public_key = None
                except Exception:
                    peer_info.public_key = None

            # Prevent overwriting an existing verified public key
            existing = self.peer_table.get(peer_info.node_id)
            if existing and existing.info.public_key and existing.info.public_key != peer_info.public_key:
                peer_info.public_key = existing.info.public_key

            metrics = ResourceMetrics.from_dict(metrics_data) if metrics_data else None
            is_direct = (peer_info.node_id == message.sender_id)
            self.peer_table.update_peer(peer_info, metrics=metrics, direct_contact=is_direct)

            # Also ensure Kademlia routing table knows about this peer
            contact = Contact(
                node_id=peer_info.node_id,
                host=peer_info.host,
                tcp_port=peer_info.tcp_port,
                udp_port=peer_info.udp_port,
            )
            self.kademlia.routing_table.add_contact(contact)
            updated_nodes.append(peer_info.node_id)

        if updated_nodes:
            await self.events.emit(EVENT_GOSSIP_RECEIVED, {"sender": message.sender_id, "nodes": updated_nodes})
