"""Peer state management, metrics caching, and stale peer detection."""

import logging
from typing import Iterator
from meshweaver.core.state import NodeInfo, PeerHealth, PeerState, ResourceMetrics
from meshweaver.utils.helpers import utc_now

logger = logging.getLogger("meshweaver.peer_state")


class PeerTable:
    """Thread-safe / async-friendly table of known peers and their live metrics."""

    def __init__(self) -> None:
        self._peers: dict[str, PeerState] = {}

    def __len__(self) -> int:
        return len(self._peers)

    def __iter__(self) -> Iterator[PeerState]:
        return iter(list(self._peers.values()))

    def get(self, node_id: str) -> PeerState | None:
        return self._peers.get(node_id)

    def update_peer(
        self,
        info: NodeInfo,
        metrics: ResourceMetrics | None = None,
        rtt_ms: float | None = None,
        direct_contact: bool = True,
    ) -> PeerState:
        """Add or update a peer entry with latest metrics."""
        now = utc_now()
        peer = self._peers.get(info.node_id)
        if not peer:
            peer = PeerState(info=info, metrics=metrics if metrics else ResourceMetrics())
            self._peers[info.node_id] = peer
        elif metrics:
            # Only update metrics if they are newer than what we have
            if metrics.timestamp >= peer.metrics.timestamp:
                peer.metrics = metrics

        peer.info = info
        if direct_contact:
            peer.mark_alive(rtt_ms=rtt_ms)
        return peer

    def check_stale(
        self,
        stale_threshold: float = 15.0,
        dead_threshold: float = 30.0,
        purge_threshold: float = 90.0,
    ) -> tuple[list[str], list[str], list[str]]:
        """Inspect peers and flag stale/dead peers or purge expired ones."""
        now = utc_now()
        suspected: list[str] = []
        dead: list[str] = []
        purged: list[str] = []

        for node_id, peer in list(self._peers.items()):
            elapsed = now - peer.last_seen

            if elapsed >= purge_threshold:
                purged.append(node_id)
                del self._peers[node_id]
                logger.info("Purged expired peer %s (unseen for %.1fs)", node_id[:8], elapsed)
            elif elapsed >= dead_threshold:
                if peer.health != PeerHealth.DEAD:
                    peer.health = PeerHealth.DEAD
                    dead.append(node_id)
                    logger.warning("Peer %s marked DEAD (unseen for %.1fs)", node_id[:8], elapsed)
            elif elapsed >= stale_threshold:
                if peer.health == PeerHealth.HEALTHY:
                    peer.health = PeerHealth.SUSPECT
                    suspected.append(node_id)
                    logger.info("Peer %s marked SUSPECT (unseen for %.1fs)", node_id[:8], elapsed)

        return (suspected, dead, purged)

    def available_peers(self) -> list[PeerState]:
        """Return non-dead, non-overloaded peers available for task assignment."""
        return [
            p for p in self._peers.values()
            if p.health != PeerHealth.DEAD and not p.metrics.is_overloaded
        ]

    def all_peers(self) -> list[PeerState]:
        return list(self._peers.values())
