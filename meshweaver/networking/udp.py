"""Asynchronous UDP transport for lightweight discovery and gossip."""

import asyncio
import logging
import socket
from typing import Any, Callable, Coroutine
from meshweaver.networking.framing import decode_frame_bytes
from meshweaver.networking.protocol import Message
from meshweaver.utils.exceptions import FramingError, NetworkError

logger = logging.getLogger("meshweaver.udp")

UDPMessageHandler = Callable[[Message, tuple[str, int]], Coroutine[Any, Any, Message | None]]


class _UDPProtocol(asyncio.DatagramProtocol):
    def __init__(self, endpoint: "AsyncUDPEndpoint") -> None:
        self.endpoint = endpoint
        self.transport: asyncio.DatagramTransport | None = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport  # type: ignore

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        self.endpoint._on_datagram_received(data, addr)

    def error_received(self, exc: Exception) -> None:
        logger.warning("UDP error received: %s", exc)

    def connection_lost(self, exc: Exception | None) -> None:
        logger.debug("UDP transport connection lost: %s", exc)


class AsyncUDPEndpoint:
    """Asynchronous UDP endpoint for lightweight RPC, LAN beacons, and gossip."""

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 0,
        handler: UDPMessageHandler | None = None,
        broadcast: bool = False,
        max_queue_size: int = 2000,
        worker_count: int = 4,
        max_datagram_size: int = 65507,
    ) -> None:
        self.host = host
        self.port = port
        self.handler = handler
        self.broadcast = broadcast
        self.max_queue_size = max_queue_size
        self.worker_count = worker_count
        self.max_datagram_size = max_datagram_size
        self._queue: asyncio.Queue[tuple[Message, tuple[str, int]]] | None = None
        self._worker_tasks: list[asyncio.Task] = []
        self._transport: asyncio.DatagramTransport | None = None
        self._protocol: _UDPProtocol | None = None
        self._is_running = False

    @property
    def is_running(self) -> bool:
        return self._is_running

    @property
    def actual_port(self) -> int:
        if self._transport:
            sock = self._transport.get_extra_info("socket")
            if sock:
                return sock.getsockname()[1]
        return self.port

    async def start(self) -> None:
        """Bind and start listening for UDP datagrams."""
        if self._is_running:
            return

        loop = asyncio.get_running_loop()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.setblocking(False)

            # Allow port reuse if supported
            if hasattr(socket, "SO_REUSEADDR"):
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            if self.broadcast:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

            sock.bind((self.host, self.port))

            transport, protocol = await loop.create_datagram_endpoint(
                lambda: _UDPProtocol(self),
                sock=sock,
            )
        except Exception:
            sock.close()
            raise
        self._transport = transport
        self._protocol = protocol
        self._is_running = True

        # Initialize bounded worker queue and workers
        self._queue = asyncio.Queue(maxsize=self.max_queue_size)
        self._worker_tasks = [
            asyncio.create_task(self._worker_loop())
            for _ in range(self.worker_count)
        ]

        logger.info("UDP endpoint listening on %s:%d (broadcast=%s)", self.host, self.actual_port, self.broadcast)

    async def _worker_loop(self) -> None:
        while self._is_running:
            try:
                if self._queue is None:
                    break
                message, addr = await self._queue.get()
                try:
                    await self._process_message(message, addr)
                finally:
                    self._queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in UDP worker loop: %s", e, exc_info=True)

    def _on_datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        if not self._is_running or not self.handler or self._queue is None:
            return

        try:
            frame = decode_frame_bytes(data, max_size=self.max_datagram_size)
            message = Message.from_frame(frame)

            try:
                self._queue.put_nowait((message, addr))
            except asyncio.QueueFull:
                logger.warning("UDP receive queue full (%d items); dropping packet from %s", self.max_queue_size, addr)
        except Exception as e:
            logger.debug("Failed to parse incoming UDP datagram from %s: %s", addr, e)

    async def _process_message(self, message: Message, addr: tuple[str, int]) -> None:
        if not self.handler:
            return
        try:
            reply = await self.handler(message, addr)
            if reply is not None:
                self.send_to(reply.to_frame(), addr)
        except Exception as e:
            logger.error("Error in UDP handler for %s from %s: %s", message.msg_type, addr, e, exc_info=True)

    def send_to(self, data: bytes, addr: tuple[str, int]) -> None:
        """Send raw bytes to a target UDP address."""
        if len(data) > self.max_datagram_size:
            raise FramingError(
                f"UDP datagram size {len(data)} exceeds maximum allowed {self.max_datagram_size} bytes"
            )
        if self._transport and not self._transport.is_closing():
            self._transport.sendto(data, addr)
        else:
            raise NetworkError("Cannot send UDP datagram: transport is not active")

    def send_message(self, message: Message, addr: tuple[str, int]) -> None:
        """Send a Message as framed bytes to a target UDP address."""
        self.send_to(message.to_frame(), addr)

    async def stop(self) -> None:
        """Close UDP transport and terminate worker tasks."""
        self._is_running = False
        for task in self._worker_tasks:
            task.cancel()
        if self._worker_tasks:
            await asyncio.gather(*self._worker_tasks, return_exceptions=True)
            self._worker_tasks.clear()

        if self._transport:
            self._transport.close()
            self._transport = None
            self._protocol = None
            await asyncio.sleep(0)
        self._queue = None
        logger.info("UDP endpoint stopped.")
