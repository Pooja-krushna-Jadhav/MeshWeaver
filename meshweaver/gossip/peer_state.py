"""Peer state management, metrics caching, and stale peer detection."""

import logging
from typing import Iterator
from meshweaver.core.state import NodeInfo, PeerHealth, PeerState, ResourceMetrics
from meshweaver.utils.helpers import utc_now

logger = logging.getLogger("meshweaver.peer_state")


class PeerTable:
    """Thread-safe / async-friendly table of known peers and their live metrics."""

    _DEFAULT_MAX_TOMBSTONES = 10000
    _DEFAULT_TOMBSTONE_MAX_AGE = 3600.0

    def __init__(
        self,
        max_tombstones: int = _DEFAULT_MAX_TOMBSTONES,
        tombstone_max_age: float = _DEFAULT_TOMBSTONE_MAX_AGE,
    ) -> None:
        self._peers: dict[str, PeerState] = {}
        self._tombstones: dict[str, float] = {}
        self._max_tombstones = max(1, max_tombstones)
        self._tombstone_max_age = max(0.0, tombstone_max_age)

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
    ) -> PeerState | None:
        """Add or update a peer entry with latest metrics."""
        tombstone_ts = self._tombstones.get(info.node_id)
        if tombstone_ts is not None:
            metrics_ts = metrics.timestamp if metrics else 0.0
            if not direct_contact and metrics_ts <= tombstone_ts:
                return None
            self._tombstones.pop(info.node_id, None)

        peer = self._peers.get(info.node_id)
        if not peer:
            peer = PeerState(info=info, metrics=metrics if metrics else ResourceMetrics())
            self._peers[info.node_id] = peer
        else:
            if metrics and metrics.timestamp >= peer.metrics.timestamp:
                peer.metrics = metrics
            if direct_contact:
                peer.info = info

        if direct_contact:
            peer.mark_alive(rtt_ms=rtt_ms)
        return peer

    def _prune_tombstones(self, now: float) -> None:
        """Evict expired tombstones and cap size at _max_tombstones."""
        expired = [nid for nid, ts in self._tombstones.items() if now - ts > self._tombstone_max_age]
        for nid in expired:
            del self._tombstones[nid]
        while len(self._tombstones) > self._max_tombstones:
            oldest_id = next(iter(self._tombstones))
            del self._tombstones[oldest_id]

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
                self._tombstones.pop(node_id, None)
                self._tombstones[node_id] = now
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

        self._prune_tombstones(now)
        return (suspected, dead, purged)

    def available_peers(self) -> list[PeerState]:
        """Return non-dead, non-overloaded peers available for task assignment."""
        return [
            p for p in self._peers.values()
            if p.health != PeerHealth.DEAD and not p.metrics.is_overloaded
        ]

    def all_peers(self) -> list[PeerState]:
        return list(self._peers.values())
