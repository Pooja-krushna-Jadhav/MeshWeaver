"""Asynchronous TCP networking server, client, and connection pool."""

import asyncio
import logging
from typing import Any, Callable, Coroutine
from meshweaver.networking.framing import Frame, FrameFlags, encode_frame, read_frame
from meshweaver.networking.protocol import Message
from meshweaver.utils.exceptions import (
    ConfigurationError,
    ConnectionFailedError,
    ConnectionTimeoutError,
    FramingError,
    NetworkError,
)

logger = logging.getLogger("meshweaver.tcp")

MessageHandler = Callable[[Message, asyncio.StreamWriter], Coroutine[Any, Any, Message | None]]


def create_ssl_context(
    cert_path: str | None = None,
    key_path: str | None = None,
    ca_path: str | None = None,
    is_server: bool = False,
    allow_insecure_client: bool = False,
) -> Any:
    """Create an SSLContext from given certificates/keys, or return None if not configured."""
    if not (cert_path or key_path or ca_path):
        return None
    if bool(cert_path) != bool(key_path):
        raise ConfigurationError("Both cert_path and key_path must be provided together")

    import ssl
    if is_server:
        if not (cert_path and key_path):
            raise ConfigurationError("Server SSL context requires both cert_path and key_path")
        ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ctx.load_cert_chain(certfile=cert_path, keyfile=key_path)
        if ca_path:
            ctx.load_verify_locations(cafile=ca_path)
            ctx.verify_mode = ssl.CERT_REQUIRED
    else:
        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        if ca_path:
            ctx.load_verify_locations(cafile=ca_path)
        elif allow_insecure_client:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        else:
            raise ConfigurationError("Client SSL context requires ca_path unless allow_insecure_client=True")
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
        read_timeout: float = 30.0,
        max_connections: int = 256,
    ) -> None:
        self.host = host
        self.port = port
        self.handler = handler
        self.max_frame_size = max_frame_size
        self.ssl_context = ssl_context
        self.read_timeout = read_timeout
        self.max_connections = max_connections
        self._server: asyncio.Server | None = None
        self._active_connections: set[asyncio.StreamWriter] = set()
        self._client_tasks: set[asyncio.Task[Any]] = set()
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
        current_task = asyncio.current_task()
        if current_task is not None:
            self._client_tasks.add(current_task)

        if len(self._active_connections) >= self.max_connections:
            logger.warning("Max connections (%d) reached; rejecting %s", self.max_connections, addr)
            try:
                writer.close()
                await asyncio.wait_for(writer.wait_closed(), timeout=1.0)
            except Exception:
                pass
            finally:
                if current_task is not None:
                    self._client_tasks.discard(current_task)
            return

        self._active_connections.add(writer)
        logger.debug("TCP client connected: %s", addr)

        try:
            while self._is_running:
                try:
                    frame = await asyncio.wait_for(
                        read_frame(reader, max_size=self.max_frame_size),
                        timeout=self.read_timeout,
                    )
                except asyncio.TimeoutError:
                    logger.debug("TCP read timeout (%.1fs) from %s", self.read_timeout, addr)
                    break
                except ConnectionResetError:
                    break
                except (asyncio.IncompleteReadError, FramingError) as e:
                    logger.warning("Framing error from %s: %s", addr, e)
                    break

                try:
                    message = Message.from_frame(frame)
                except Exception as e:
                    logger.warning("Failed to decode message from %s: %s", addr, e)
                    err_msg = Message.error(sender_id="server", code="INVALID_MESSAGE", message="Invalid message format")
                    try:
                        writer.write(err_msg.to_frame())
                        await writer.drain()
                    except (OSError, ConnectionError):
                        pass
                    break

                if self.handler:
                    try:
                        reply = await self.handler(message, writer)
                        if reply is not None:
                            reply_frame = reply.to_frame()
                            writer.write(reply_frame)
                            await writer.drain()
                    except (OSError, ConnectionError) as e:
                        logger.debug("Connection error handling message %s from %s: %s", message.msg_type, addr, e)
                        break
                    except Exception as e:
                        logger.error("Error handling message %s from %s: %s", message.msg_type, addr, e, exc_info=True)
                        err_msg = Message.error(sender_id="server", code="HANDLER_ERROR", message="Internal server error")
                        try:
                            writer.write(err_msg.to_frame())
                            await writer.drain()
                        except (OSError, ConnectionError):
                            break

        except asyncio.CancelledError:
            pass
        finally:
            self._active_connections.discard(writer)
            if current_task is not None:
                self._client_tasks.discard(current_task)
            try:
                writer.close()
                await asyncio.wait_for(writer.wait_closed(), timeout=1.0)
            except Exception:
                pass
            logger.debug("TCP client disconnected: %s", addr)

    async def stop(self) -> None:
        """Gracefully stop server and close all active connections."""
        self._is_running = False

        if self._server:
            self._server.close()

        # Close active writers first so waiting read_frame loops unblock promptly
        writers = list(self._active_connections)
        self._active_connections.clear()
        for w in writers:
            try:
                w.close()
            except Exception:
                pass

        tasks = list(self._client_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._client_tasks.clear()

        for w in writers:
            try:
                await asyncio.wait_for(w.wait_closed(), timeout=1.0)
            except Exception:
                pass

        if self._server:
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
        max_total_idle: int = 128,
    ) -> None:
        self.max_idle = max_idle
        self.timeout = timeout
        self.ssl_context = ssl_context
        self.max_total_idle = max_total_idle
        self._pool: dict[tuple[str, int], list[tuple[asyncio.StreamReader, asyncio.StreamWriter]]] = {}
        self._lock = asyncio.Lock()
        self._closed = False

    async def _pop_idle(self, host: str, port: int) -> tuple[asyncio.StreamReader, asyncio.StreamWriter] | None:
        if self._closed:
            return None
        key = (host, port)
        stale_writers: list[asyncio.StreamWriter] = []
        valid_conn: tuple[asyncio.StreamReader, asyncio.StreamWriter] | None = None
        async with self._lock:
            conns = self._pool.get(key, [])
            while conns:
                r, w = conns.pop()
                if not w.is_closing() and not r.at_eof():
                    valid_conn = (r, w)
                    break
                stale_writers.append(w)
            if key in self._pool and not conns:
                del self._pool[key]

        for sw in stale_writers:
            try:
                sw.close()
                await asyncio.wait_for(sw.wait_closed(), timeout=1.0)
            except Exception:
                pass

        return valid_conn

    async def acquire(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        if self._closed:
            raise ConnectionFailedError("Connection pool is closed")

        pooled = await self._pop_idle(host, port)
        if pooled is not None:
            return pooled

        effective_timeout = self.timeout if timeout is None else timeout
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port, ssl=self.ssl_context),
                timeout=effective_timeout,
            )
            return reader, writer
        except asyncio.TimeoutError:
            raise ConnectionTimeoutError(f"Connection pool to {host}:{port} timed out")
        except OSError as e:
            raise ConnectionFailedError(f"Connection pool failed to reach {host}:{port}: {e}")

    async def release(self, host: str, port: int, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        key = (host, port)
        if self._closed or writer.is_closing() or reader.at_eof():
            try:
                writer.close()
                await asyncio.wait_for(writer.wait_closed(), timeout=1.0)
            except Exception:
                pass
            return

        close_needed = False
        async with self._lock:
            if self._closed:
                close_needed = True
            else:
                total_idle = sum(len(v) for v in self._pool.values())
                conns = self._pool.setdefault(key, [])
                if len(conns) < self.max_idle and total_idle < self.max_total_idle:
                    conns.append((reader, writer))
                else:
                    close_needed = True

        if close_needed:
            try:
                writer.close()
                await asyncio.wait_for(writer.wait_closed(), timeout=1.0)
            except Exception:
                pass

    async def close_all(self) -> None:
        async with self._lock:
            self._closed = True
            writers = [w for conns in self._pool.values() for _, w in conns]
            self._pool.clear()
        for writer in writers:
            try:
                writer.close()
                await asyncio.wait_for(writer.wait_closed(), timeout=1.0)
            except Exception:
                pass


class AsyncTCPClient:
    """Asynchronous TCP client for point-to-point message transmission with optional connection pooling."""

    def __init__(
        self,
        max_frame_size: int = 16 * 1024 * 1024,
        pool_connections: bool = True,
        max_idle: int = 16,
        timeout: float = 10.0,
        ssl_context: Any = None,
        connect_timeout: float = 5.0,
    ) -> None:
        self.max_frame_size = max_frame_size
        self.ssl_context = ssl_context
        self.timeout = timeout
        self.connect_timeout = connect_timeout
        self.pool = ConnectionPool(max_idle=max_idle, timeout=timeout, ssl_context=ssl_context) if pool_connections else None

    async def close(self) -> None:
        """Close all pooled idle connections."""
        if self.pool:
            await self.pool.close_all()

    @staticmethod
    async def _close_writer(writer: asyncio.StreamWriter) -> None:
        try:
            writer.close()
            await asyncio.wait_for(writer.wait_closed(), timeout=1.0)
        except Exception:
            pass

    async def _open_fresh(self, host: str, port: int, conn_timeout: float) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        try:
            return await asyncio.wait_for(
                asyncio.open_connection(host, port, ssl=self.ssl_context),
                timeout=conn_timeout,
            )
        except asyncio.TimeoutError:
            raise ConnectionTimeoutError(f"Connection to {host}:{port} timed out after {conn_timeout}s")
        except OSError as e:
            raise ConnectionFailedError(f"Failed to connect to {host}:{port}: {e}")

    async def _exchange_on_connection(
        self,
        host: str,
        port: int,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        frame_data: bytes,
        timeout: float,
        release_to_pool: bool,
    ) -> Message:
        try:
            writer.write(frame_data)
            await asyncio.wait_for(writer.drain(), timeout=timeout)

            resp_frame = await asyncio.wait_for(
                read_frame(reader, max_size=self.max_frame_size),
                timeout=timeout,
            )
            resp_msg = Message.from_frame(resp_frame)
        except asyncio.TimeoutError:
            await self._close_writer(writer)
            raise ConnectionTimeoutError(f"Response from {host}:{port} timed out after {timeout}s")
        except Exception:
            await self._close_writer(writer)
            raise
        except BaseException:
            try:
                writer.close()
            except Exception:
                pass
            raise

        if release_to_pool and self.pool is not None:
            await self.pool.release(host, port, reader, writer)
        else:
            await self._close_writer(writer)
        return resp_msg

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
        conn_timeout = min(timeout, self.connect_timeout)
        _, writer = await self._open_fresh(host, port, conn_timeout)

        try:
            writer.write(frame_data)
            await asyncio.wait_for(writer.drain(), timeout=timeout)
        finally:
            await self._close_writer(writer)

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
        conn_timeout = min(timeout, self.connect_timeout)

        if self.pool is not None:
            pooled = await self.pool._pop_idle(host, port)
            if pooled is not None:
                reader, writer = pooled
                from_pool = True
            else:
                reader, writer = await self.pool.acquire(host, port, timeout=conn_timeout)
                from_pool = False

            try:
                return await self._exchange_on_connection(
                    host, port, reader, writer, frame_data, timeout, release_to_pool=True
                )
            except (ConnectionResetError, asyncio.IncompleteReadError, BrokenPipeError, OSError) as e:
                if not from_pool:
                    raise ConnectionFailedError(f"Connection to {host}:{port} failed during transmission: {e}") from e
                # Pooled socket was stale; retry once with a fresh connection
                reader, writer = await self._open_fresh(host, port, conn_timeout)
                try:
                    return await self._exchange_on_connection(
                        host, port, reader, writer, frame_data, timeout, release_to_pool=True
                    )
                except (ConnectionResetError, asyncio.IncompleteReadError, BrokenPipeError, OSError) as retry_err:
                    raise ConnectionFailedError(
                        f"Connection to {host}:{port} failed during transmission: {retry_err}"
                    ) from retry_err

        # Unpooled fallback
        reader, writer = await self._open_fresh(host, port, conn_timeout)
        try:
            return await self._exchange_on_connection(
                host, port, reader, writer, frame_data, timeout, release_to_pool=False
            )
        except (ConnectionResetError, asyncio.IncompleteReadError, BrokenPipeError, OSError) as e:
            raise ConnectionFailedError(f"Connection to {host}:{port} failed during transmission: {e}") from e
