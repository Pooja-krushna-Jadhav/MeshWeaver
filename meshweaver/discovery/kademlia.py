"""Kademlia Distributed Hash Table (DHT) protocol implementation."""

import asyncio
import hashlib
import logging
from typing import Any, Callable, Coroutine
from meshweaver.discovery.routing_table import Contact, RoutingTable
from meshweaver.networking.protocol import Message
from meshweaver.utils.exceptions import ProtocolError
from meshweaver.utils.helpers import generate_nonce, utc_now, xor_distance

logger = logging.getLogger("meshweaver.kademlia")

ALPHA = 3  # Parallel lookup concurrency


class KademliaNode:
    """Kademlia DHT participant managing routing table and distributed key-value storage."""

    def __init__(
        self,
        local_node_id: str,
        host: str,
        tcp_port: int,
        udp_port: int,
        send_rpc_fn: Callable[[str, int, Message, float], Coroutine[Any, Any, Message | None]],
        k_size: int = 20,
        alpha: int = ALPHA,
    ) -> None:
        self.local_node_id = local_node_id
        self.host = host
        self.tcp_port = tcp_port
        self.udp_port = udp_port
        self.send_rpc = send_rpc_fn
        self.alpha = max(1, alpha)
        self.routing_table = RoutingTable(local_node_id, k_size=k_size)
        self.storage: dict[str, tuple[Any, float]] = {}  # key -> (value, stored_at)

    def self_contact(self) -> Contact:
        return Contact(
            node_id=self.local_node_id,
            host=self.host,
            tcp_port=self.tcp_port,
            udp_port=self.udp_port,
        )

    async def handle_rpc(self, message: Message) -> Message:
        """Handle incoming Kademlia RPC message (PING, STORE, FIND_NODE, FIND_VALUE)."""
        payload = message.payload
        rpc_type = payload.get("rpc")
        sender_contact_data = payload.get("contact")

        if sender_contact_data:
            if not isinstance(sender_contact_data, dict):
                return Message.error(
                    sender_id=self.local_node_id,
                    receiver_id=message.sender_id,
                    code="INVALID_CONTACT",
                    message="Malformed sender contact payload",
                )
            try:
                contact = Contact.from_dict(sender_contact_data)
            except (KeyError, TypeError, ValueError) as e:
                return Message.error(
                    sender_id=self.local_node_id,
                    receiver_id=message.sender_id,
                    code="INVALID_CONTACT",
                    message=f"Invalid sender contact: {e}",
                )
            if contact.node_id != message.sender_id:
                return Message.error(
                    sender_id=self.local_node_id,
                    receiver_id=message.sender_id,
                    code="INVALID_CONTACT",
                    message="Sender contact node_id does not match message sender_id",
                )
            try:
                self.routing_table.add_contact(contact)
            except ProtocolError as e:
                return Message.error(
                    sender_id=self.local_node_id,
                    receiver_id=message.sender_id,
                    code="INVALID_CONTACT",
                    message=str(e),
                )

        if rpc_type == "PING":
            return Message(
                sender_id=self.local_node_id,
                receiver_id=message.sender_id,
                msg_type="kad_rpc",
                payload={"rpc": "PONG", "contact": self.self_contact().to_dict()},
            )

        elif rpc_type == "STORE":
            key = str(payload.get("key"))
            value = payload.get("value")
            self.storage[key] = (value, utc_now())
            return Message(
                sender_id=self.local_node_id,
                receiver_id=message.sender_id,
                msg_type="kad_rpc",
                payload={"rpc": "STORE_ACK", "status": "OK"},
            )

        elif rpc_type == "FIND_NODE":
            target_id = str(payload.get("target_id", ""))
            try:
                closest = self.routing_table.find_closest_contacts(target_id, count=self.routing_table.k_size)
            except ProtocolError as e:
                return Message.error(
                    sender_id=self.local_node_id,
                    receiver_id=message.sender_id,
                    code="INVALID_TARGET",
                    message=str(e),
                )
            return Message(
                sender_id=self.local_node_id,
                receiver_id=message.sender_id,
                msg_type="kad_rpc",
                payload={
                    "rpc": "FIND_NODE_RESP",
                    "contacts": [c.to_dict() for c in closest],
                },
            )

        elif rpc_type == "FIND_VALUE":
            key = str(payload.get("key"))
            if key in self.storage:
                val, _ = self.storage[key]
                return Message(
                    sender_id=self.local_node_id,
                    receiver_id=message.sender_id,
                    msg_type="kad_rpc",
                    payload={"rpc": "FIND_VALUE_RESP", "value": val, "found": True},
                )
            else:
                key_hash = hashlib.sha1(key.encode("utf-8")).hexdigest()
                closest = self.routing_table.find_closest_contacts(key_hash, count=self.routing_table.k_size)
                return Message(
                    sender_id=self.local_node_id,
                    receiver_id=message.sender_id,
                    msg_type="kad_rpc",
                    payload={
                        "rpc": "FIND_VALUE_RESP",
                        "contacts": [c.to_dict() for c in closest],
                        "found": False,
                    },
                )

        return Message.error(
            sender_id=self.local_node_id,
            code="UNKNOWN_RPC",
            message=f"Unknown Kademlia RPC: {rpc_type}",
        )

    def _evict_contact(self, node_id: str) -> None:
        """Safely evict an unreachable contact from the routing table."""
        try:
            self.routing_table.remove_contact(node_id)
        except ProtocolError:
            pass

    async def ping(self, contact: Contact, timeout: float = 3.0) -> bool:
        """Send PING RPC to contact."""
        msg = Message(
            sender_id=self.local_node_id,
            receiver_id=None if contact.is_seed else contact.node_id,
            msg_type="kad_rpc",
            payload={"rpc": "PING", "contact": self.self_contact().to_dict()},
        )
        try:
            resp = await self.send_rpc(contact.host, contact.tcp_port, msg, timeout)
            if resp and resp.payload.get("rpc") == "PONG":
                peer_contact_data = resp.payload.get("contact")
                if peer_contact_data:
                    real_contact = Contact.from_dict(peer_contact_data)
                    real_contact.last_seen = utc_now()
                    if contact.node_id != real_contact.node_id:
                        self._evict_contact(contact.node_id)
                    self.routing_table.add_contact(real_contact)
                elif not contact.is_seed:
                    contact.last_seen = utc_now()
                    self.routing_table.add_contact(contact)
                elif resp.sender_id:
                    real_contact = Contact(
                        node_id=resp.sender_id,
                        host=contact.host,
                        tcp_port=contact.tcp_port,
                        udp_port=contact.udp_port,
                        last_seen=utc_now(),
                    )
                    self.routing_table.add_contact(real_contact)
                return True
        except Exception as e:
            logger.debug("PING to %s failed: %s", contact.node_id[:8], e)
        self._evict_contact(contact.node_id)
        return False

    async def find_node(self, contact: Contact, target_id: str, timeout: float = 3.0) -> list[Contact]:
        """Send FIND_NODE RPC to contact and return discovered neighbors."""
        msg = Message(
            sender_id=self.local_node_id,
            receiver_id=contact.node_id,
            msg_type="kad_rpc",
            payload={
                "rpc": "FIND_NODE",
                "target_id": target_id,
                "contact": self.self_contact().to_dict(),
            },
        )
        try:
            resp = await self.send_rpc(contact.host, contact.tcp_port, msg, timeout)
            if resp and resp.payload.get("rpc") == "FIND_NODE_RESP":
                contacts_data = resp.payload.get("contacts", [])
                contacts: list[Contact] = []
                for cd in contacts_data:
                    try:
                        c = Contact.from_dict(cd)
                        if c.node_id != self.local_node_id:
                            self.routing_table.add_contact(c)
                            contacts.append(c)
                    except (KeyError, TypeError, ValueError, ProtocolError) as ce:
                        logger.debug("Ignoring malformed contact in FIND_NODE_RESP: %s", ce)
                return contacts
        except Exception as e:
            logger.debug("FIND_NODE to %s failed: %s", contact.node_id[:8], e)
        self._evict_contact(contact.node_id)
        return []

    async def store(self, contact: Contact, key: str, value: Any, timeout: float = 3.0) -> bool:
        """Send STORE RPC to contact."""
        msg = Message(
            sender_id=self.local_node_id,
            receiver_id=contact.node_id,
            msg_type="kad_rpc",
            payload={
                "rpc": "STORE",
                "key": key,
                "value": value,
                "contact": self.self_contact().to_dict(),
            },
        )
        try:
            resp = await self.send_rpc(contact.host, contact.tcp_port, msg, timeout)
            return resp is not None and resp.payload.get("rpc") == "STORE_ACK"
        except Exception as e:
            logger.debug("STORE to %s failed: %s", contact.node_id[:8], e)
            return False

    async def iterative_find_node(self, target_id: str) -> list[Contact]:
        """Perform Kademlia alpha-parallel iterative lookup for target_id."""
        queried: set[str] = set()
        closest = self.routing_table.find_closest_contacts(target_id, count=self.routing_table.k_size)
        if not closest:
            return []

        seen_ids: set[str] = {c.node_id for c in closest}

        while True:
            # Select unqueried contacts among closest
            candidates = [c for c in closest if c.node_id not in queried][:self.alpha]
            if not candidates:
                break

            tasks = [self.find_node(c, target_id) for c in candidates]
            for c in candidates:
                queried.add(c.node_id)

            results = await asyncio.gather(*tasks, return_exceptions=True)
            for res in results:
                if isinstance(res, list):
                    for new_c in res:
                        if new_c.node_id != self.local_node_id and new_c.node_id not in seen_ids:
                            seen_ids.add(new_c.node_id)
                            closest.append(new_c)

            closest.sort(key=lambda c: xor_distance(c.node_id, target_id))
            closest = closest[: self.routing_table.k_size]

        return closest

    async def bootstrap(self, bootstrap_peers: list[Contact]) -> int:
        """Bootstrap node by contacting seed peers and finding nearest nodes."""
        successful = 0
        for peer in bootstrap_peers:
            if await self.ping(peer):
                successful += 1

        if successful > 0:
            # Query the network for ourselves to populate routing table
            await self.iterative_find_node(self.local_node_id)

        return successful
