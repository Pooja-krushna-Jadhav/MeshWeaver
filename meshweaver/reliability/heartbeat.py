"""Heartbeat monitoring service actively probing peer liveness."""

import asyncio
import logging
from typing import Any, Callable, Coroutine
from meshweaver.core.events import EVENT_HEARTBEAT_LOST, EVENT_PEER_UPDATED, EventBus
from meshweaver.core.state import PeerHealth
from meshweaver.gossip.peer_state import PeerTable
from meshweaver.networking.protocol import Message
from meshweaver.reliability.failure_detector import FailureDetector
from meshweaver.utils.helpers import utc_now

logger = logging.getLogger("meshweaver.heartbeat")


class HeartbeatMonitor:
    """Actively probes known peers with periodic heartbeats to maintain failure detector."""

    def __init__(
        self,
        node_id: str,
        peer_table: PeerTable,
        failure_detector: FailureDetector,
        events: EventBus,
        send_fn: Callable[[str, int, Message, float], Coroutine[Any, Any, Message | None]],
        interval: float = 2.0,
    ) -> None:
        self.node_id = node_id
        self.peer_table = peer_table
        self.failure_detector = failure_detector
        self.events = events
        self.send_fn = send_fn
        self.interval = interval

        self._is_running = False
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        if self._is_running:
            return
        self._is_running = True
        self._task = asyncio.create_task(self._run_loop())
        logger.info("Heartbeat monitor started (interval: %.1fs)", self.interval)

    async def stop(self) -> None:
        self._is_running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info("Heartbeat monitor stopped.")

    async def _run_loop(self) -> None:
        while self._is_running:
            try:
                await self.probe_all_peers()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in heartbeat probe cycle: %s", e, exc_info=True)

            await asyncio.sleep(self.interval)

    async def probe_all_peers(self) -> None:
        """Probe all known peers in parallel."""
        peers = [p for p in self.peer_table.all_peers() if p.info.node_id != self.node_id]
        if not peers:
            return

        for p in peers:
            self.failure_detector.register_peer(p.info.node_id)

        tasks = [self._probe_single_peer(p.info.node_id, p.info.host, p.info.tcp_port) for p in peers]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _probe_single_peer(self, peer_id: str, host: str, port: int) -> None:
        msg = Message.heartbeat(sender_id=self.node_id, receiver_id=peer_id)
        start_t = utc_now()
        peer_state = self.peer_table.get(peer_id)

        try:
            resp = await self.send_fn(host, port, msg, self.interval)
            if resp is not None and resp.msg_type != "error":
                rtt_ms = (utc_now() - start_t) * 1000.0
                self.failure_detector.record_heartbeat(peer_id)
                if peer_state:
                    peer_state.mark_alive(rtt_ms=rtt_ms)
            else:
                self._handle_probe_failure(peer_id)
        except Exception:
            self._handle_probe_failure(peer_id)

    def _handle_probe_failure(self, peer_id: str) -> None:
        self.failure_detector.record_failure(peer_id)
        health = self.failure_detector.get_health(peer_id)
        peer_state = self.peer_table.get(peer_id)
        if peer_state:
            old_health = peer_state.health
            peer_state.health = health
            if health == PeerHealth.DEAD and old_health != PeerHealth.DEAD:
                logger.warning("Peer %s confirmed DEAD by heartbeat monitor", peer_id[:8])
                self.events.emit_nowait(EVENT_HEARTBEAT_LOST, {"node_id": peer_id})
