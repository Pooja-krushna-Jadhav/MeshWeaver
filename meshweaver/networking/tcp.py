"""Asynchronous TCP networking server, client, and connection pool."""

import asyncio
import logging
from typing import Any, Callable, Coroutine
from meshweaver.networking.framing import Frame, FrameFlags, encode_frame, read_frame
from meshweaver.networking.protocol import Message
from meshweaver.utils.exceptions import ConnectionFailedError, ConnectionTimeoutError, FramingError, NetworkError

logger = logging.getLogger("meshweaver.tcp")

MessageHandler = Callable[[Message, asyncio.StreamWriter], Coroutine[Any, Any, Message | None]]


def create_ssl_context(
    cert_path: str | None = None,
    key_path: str | None = None,
    ca_path: str | None = None,
    is_server: bool = False,
) -> Any:
    """Create an SSLContext from given certificates/keys, or return None if not configured."""
    if not (cert_path or ca_path):
        return None
    import ssl
    if is_server:
        ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        if cert_path and key_path:
            ctx.load_cert_chain(certfile=cert_path, keyfile=key_path)
        if ca_path:
            ctx.load_verify_locations(cafile=ca_path)
            ctx.verify_mode = ssl.CERT_REQUIRED
    else:
        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        if ca_path:
            ctx.load_verify_locations(cafile=ca_path)
        else:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        if cert_path and key_path:
            ctx.load_cert_chain(certfile=cert_path, keyfile=key_path)
    return ctx


class AsyncTCPServer:
    """Non-blocking TCP server for bidirectional framed message transport."""

    def __init__(
        self,
        host: str,
        port: int,
        handler: MessageHandler | None = None,
        max_frame_size: int = 16 * 1024 * 1024,
        ssl_context: Any = None,
    ) -> None:
        self.host = host
        self.port = port
        self.handler = handler
        self.max_frame_size = max_frame_size
        self.ssl_context = ssl_context
        self._server: asyncio.Server | None = None
        self._active_connections: set[asyncio.StreamWriter] = set()
        self._is_running = False

    @property
    def is_running(self) -> bool:
        return self._is_running

    @property
    def actual_port(self) -> int:
        """Return the actual bound port (useful when bound to port 0)."""
        if self._server and self._server.sockets:
            return self._server.sockets[0].getsockname()[1]
        return self.port

    async def start(self) -> None:
        """Start listening for incoming TCP client connections."""
        if self._is_running:
            return

        self._server = await asyncio.start_server(
            self._handle_client,
            self.host,
            self.port,
            ssl=self.ssl_context,
        )
        self._is_running = True
        logger.info("TCP server listening on %s:%d (TLS=%s)", self.host, self.actual_port, self.ssl_context is not None)

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        addr = writer.get_extra_info("peername")
        self._active_connections.add(writer)
        logger.debug("TCP client connected: %s", addr)

        try:
            while self._is_running:
                try:
                    frame = await read_frame(reader, max_size=self.max_frame_size)
                except ConnectionResetError:
                    break
                except (asyncio.IncompleteReadError, FramingError) as e:
                    logger.warning("Framing error from %s: %s", addr, e)
                    break

                try:
                    message = Message.from_frame(frame)
                except Exception as e:
                    logger.error("Failed to decode message from %s: %s", addr, e)
                    continue

                if self.handler:
                    try:
                        reply = await self.handler(message, writer)
                        if reply is not None:
                            reply_frame = reply.to_frame()
                            writer.write(reply_frame)
                            await writer.drain()
                    except Exception as e:
                        logger.error("Error handling message %s from %s: %s", message.msg_type, addr, e, exc_info=True)
                        err_msg = Message.error(sender_id="server", code="HANDLER_ERROR", message=str(e))
                        writer.write(err_msg.to_frame())
                        await writer.drain()

        except asyncio.CancelledError:
            pass
        finally:
            self._active_connections.discard(writer)
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            logger.debug("TCP client disconnected: %s", addr)

    async def stop(self) -> None:
        """Gracefully stop server and close all active connections."""
        self._is_running = False

        # Close all active client connections first so waiting read_frame loops unblock
        writers = list(self._active_connections)
        self._active_connections.clear()
        for w in writers:
            try:
                w.close()
            except Exception:
                pass

        if self._server:
            self._server.close()
            try:
                await asyncio.wait_for(self._server.wait_closed(), timeout=1.0)
            except Exception:
                pass
            self._server = None

        logger.info("TCP server stopped.")


class ConnectionPool:
    """Manages pooled, reusable TCP connections to peer nodes for low latency."""

    def __init__(
        self,
        max_idle: int = 16,
        timeout: float = 10.0,
        ssl_context: Any = None,
    ) -> None:
        self.max_idle = max_idle
        self.timeout = timeout
        self.ssl_context = ssl_context
        self._pool: dict[tuple[str, int], list[tuple[asyncio.StreamReader, asyncio.StreamWriter]]] = {}
        self._lock = asyncio.Lock()

    async def acquire(self, host: str, port: int) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        key = (host, port)
        async with self._lock:
            conns = self._pool.setdefault(key, [])
            while conns:
                r, w = conns.pop()
                if not w.is_closing():
                    return r, w

        # Create new connection
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port, ssl=self.ssl_context),
                timeout=self.timeout,
            )
            return reader, writer
        except asyncio.TimeoutError:
            raise ConnectionTimeoutError(f"Connection pool to {host}:{port} timed out")
        except OSError as e:
            raise ConnectionFailedError(f"Connection pool failed to reach {host}:{port}: {e}")

    async def release(self, host: str, port: int, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        key = (host, port)
        if writer.is_closing():
            return
        async with self._lock:
            conns = self._pool.setdefault(key, [])
            if len(conns) < self.max_idle:
                conns.append((reader, writer))
            else:
                try:
                    writer.close()
                    await writer.wait_closed()
                except Exception:
                    pass

    async def close_all(self) -> None:
        async with self._lock:
            for conns in self._pool.values():
                for _, writer in conns:
                    try:
                        writer.close()
                        await writer.wait_closed()
                    except Exception:
                        pass
            self._pool.clear()


class AsyncTCPClient:
    """Asynchronous TCP client for point-to-point message transmission with optional connection pooling."""

    def __init__(
        self,
        max_frame_size: int = 16 * 1024 * 1024,
        pool_connections: bool = True,
        max_idle: int = 16,
        timeout: float = 10.0,
        ssl_context: Any = None,
    ) -> None:
        self.max_frame_size = max_frame_size
        self.ssl_context = ssl_context
        self.timeout = timeout
        self.pool = ConnectionPool(max_idle=max_idle, timeout=timeout, ssl_context=ssl_context) if pool_connections else None

    async def close(self) -> None:
        """Close all pooled idle connections."""
        if self.pool:
            await self.pool.close_all()

    async def send_message(
        self,
        host: str,
        port: int,
        message: Message,
        timeout: float = 10.0,
        flags: int = FrameFlags.NONE,
    ) -> None:
        """Connect, send one framed message, and close connection (fire-and-forget)."""
        frame_data = message.to_frame(flags=flags)
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port, ssl=self.ssl_context),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            raise ConnectionTimeoutError(f"Connection to {host}:{port} timed out after {timeout}s")
        except OSError as e:
            raise ConnectionFailedError(f"Failed to connect to {host}:{port}: {e}")

        try:
            writer.write(frame_data)
            await asyncio.wait_for(writer.drain(), timeout=timeout)
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def send_and_receive(
        self,
        host: str,
        port: int,
        message: Message,
        timeout: float = 10.0,
        flags: int = FrameFlags.NONE,
    ) -> Message:
        """Send one framed message and wait for framed response, reusing pooled connections when enabled."""
        frame_data = message.to_frame(flags=flags)

        if self.pool is not None:
            # Try once on pooled connection; if stale, retry once with a fresh socket
            for attempt in range(2):
                reader, writer = await self.pool.acquire(host, port)
                try:
                    writer.write(frame_data)
                    await asyncio.wait_for(writer.drain(), timeout=timeout)

                    resp_frame = await asyncio.wait_for(
                        read_frame(reader, max_size=self.max_frame_size),
                        timeout=timeout,
                    )
                    resp_msg = Message.from_frame(resp_frame)
                    await self.pool.release(host, port, reader, writer)
                    return resp_msg
                except (ConnectionResetError, asyncio.IncompleteReadError, BrokenPipeError, OSError):
                    try:
                        writer.close()
                        await writer.wait_closed()
                    except Exception:
                        pass
                    if attempt == 1:
                        raise ConnectionFailedError(f"Connection to {host}:{port} failed during transmission")
                except asyncio.TimeoutError:
                    try:
                        writer.close()
                        await writer.wait_closed()
                    except Exception:
                        pass
                    raise ConnectionTimeoutError(f"Response from {host}:{port} timed out after {timeout}s")
                except Exception:
                    try:
                        writer.close()
                        await writer.wait_closed()
                    except Exception:
                        pass
                    raise

        # Unpooled fallback
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port, ssl=self.ssl_context),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            raise ConnectionTimeoutError(f"Connection to {host}:{port} timed out after {timeout}s")
        except OSError as e:
            raise ConnectionFailedError(f"Failed to connect to {host}:{port}: {e}")

        try:
            writer.write(frame_data)
            await asyncio.wait_for(writer.drain(), timeout=timeout)

            resp_frame = await asyncio.wait_for(
                read_frame(reader, max_size=self.max_frame_size),
                timeout=timeout,
            )
            return Message.from_frame(resp_frame)
        except asyncio.TimeoutError:
            raise ConnectionTimeoutError(f"Response from {host}:{port} timed out after {timeout}s")
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
