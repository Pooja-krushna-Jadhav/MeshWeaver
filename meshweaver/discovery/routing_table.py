"""Kademlia routing table with 160-bit XOR metric and k-buckets."""

from dataclasses import dataclass, field
import heapq
import logging
from typing import Iterator
from meshweaver.utils.helpers import node_id_to_int, utc_now, xor_distance

logger = logging.getLogger("meshweaver.routing_table")

K_BUCKET_SIZE = 20
NUM_BUCKETS = 160


@dataclass
class Contact:
    node_id: str
    host: str
    tcp_port: int
    udp_port: int
    last_seen: float = field(default_factory=utc_now)

    def to_dict(self) -> dict[str, str | int | float]:
        return {
            "node_id": self.node_id,
            "host": self.host,
            "tcp_port": self.tcp_port,
            "udp_port": self.udp_port,
            "last_seen": self.last_seen,
        }

    @classmethod
    def from_dict(cls, data: dict[str, str | int | float]) -> "Contact":
        return cls(
            node_id=str(data["node_id"]),
            host=str(data["host"]),
            tcp_port=int(data["tcp_port"]),
            udp_port=int(data["udp_port"]),
            last_seen=float(data.get("last_seen", utc_now())),
        )


class KBucket:
    """A single k-bucket holding up to k contacts with LRU replacement queue."""

    def __init__(self, k_size: int = K_BUCKET_SIZE) -> None:
        self.k_size = k_size
        self.contacts: list[Contact] = []
        self.replacement_cache: list[Contact] = []

    def __len__(self) -> int:
        return len(self.contacts)

    def get_contact(self, node_id: str) -> Contact | None:
        for c in self.contacts:
            if c.node_id == node_id:
                return c
        return None

    def head(self) -> Contact | None:
        """Return least-recently seen contact at the head of the bucket."""
        return self.contacts[0] if self.contacts else None

    def add(self, contact: Contact) -> bool:
        """Add or update contact following LRU order. Returns True if accepted, False if cached for replacement."""
        for i, c in enumerate(self.contacts):
            if c.node_id == contact.node_id:
                # Update and move to tail (most recently seen)
                self.contacts.pop(i)
                self.contacts.append(contact)
                return True

        if len(self.contacts) < self.k_size:
            self.contacts.append(contact)
            return True

        # Bucket is full; record in replacement cache
        for i, c in enumerate(self.replacement_cache):
            if c.node_id == contact.node_id:
                self.replacement_cache.pop(i)
                break
        self.replacement_cache.append(contact)
        if len(self.replacement_cache) > self.k_size:
            self.replacement_cache.pop(0)
        return False

    def remove(self, node_id: str) -> bool:
        for i, c in enumerate(self.contacts):
            if c.node_id == node_id:
                self.contacts.pop(i)
                # Promote contact from replacement cache if available
                if self.replacement_cache:
                    promoted = self.replacement_cache.pop()
                    self.contacts.append(promoted)
                return True
        for i, c in enumerate(self.replacement_cache):
            if c.node_id == node_id:
                self.replacement_cache.pop(i)
                return True
        return False


class RoutingTable:
    """160-bit Kademlia Routing Table."""

    def __init__(self, local_node_id: str, k_size: int = K_BUCKET_SIZE) -> None:
        self.local_node_id = local_node_id
        self.k_size = k_size
        self.buckets: list[KBucket] = [KBucket(k_size) for _ in range(NUM_BUCKETS)]

    def get_bucket_index(self, node_id: str) -> int:
        """Calculate bucket index based on leading zeros of XOR distance."""
        dist = xor_distance(self.local_node_id, node_id)
        if dist == 0:
            return 0
        bit_length = dist.bit_length()  # 1 to 160
        return min(NUM_BUCKETS - 1, bit_length - 1)

    def add_contact(self, contact: Contact) -> bool:
        """Insert or refresh a contact in the appropriate k-bucket."""
        if contact.node_id == self.local_node_id:
            return False
        idx = self.get_bucket_index(contact.node_id)
        return self.buckets[idx].add(contact)

    def remove_contact(self, node_id: str) -> bool:
        idx = self.get_bucket_index(node_id)
        return self.buckets[idx].remove(node_id)

    def get_contact(self, node_id: str) -> Contact | None:
        idx = self.get_bucket_index(node_id)
        return self.buckets[idx].get_contact(node_id)

    def find_closest_contacts(self, target_id: str, count: int = K_BUCKET_SIZE) -> list[Contact]:
        """Return up to `count` contacts closest to `target_id` sorted by XOR distance."""
        all_contacts: list[Contact] = []
        for bucket in self.buckets:
            if bucket.contacts:
                all_contacts.extend(bucket.contacts)

        return heapq.nsmallest(count, all_contacts, key=lambda c: xor_distance(c.node_id, target_id))

    def all_contacts(self) -> list[Contact]:
        """Return all active contacts across all buckets."""
        result: list[Contact] = []
        for b in self.buckets:
            result.extend(b.contacts)
        return result

    @property
    def total_contacts(self) -> int:
        return sum(len(b) for b in self.buckets)
