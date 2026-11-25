"""Unit tests for ConnectionPool, AsyncTCPServer, and AsyncTCPClient edge cases."""

import asyncio
import pytest

from meshweaver.networking.framing import FrameFlags, Opcode, encode_frame, read_frame
from meshweaver.networking.protocol import Message
from meshweaver.networking.tcp import AsyncTCPClient, AsyncTCPServer, ConnectionPool
from meshweaver.utils.exceptions import ConnectionFailedError, ConnectionTimeoutError


@pytest.mark.asyncio
async def test_connection_pool_evicts_stale_at_eof_socket():
    conn_counter = 0

    async def handler(msg: Message, writer: asyncio.StreamWriter) -> Message | None:
        nonlocal conn_counter
        conn_counter += 1
        reply = Message.pong(sender_id="server", receiver_id=msg.sender_id, payload={"seq": conn_counter})
        writer.write(reply.to_frame())
        await writer.drain()
        if conn_counter == 1:
            # Close server side right after first reply so pooled socket reaches EOF
            writer.close()
        return None

    server = AsyncTCPServer("127.0.0.1", 0, handler=handler)
    await server.start()
    pool = ConnectionPool(max_idle=4, timeout=2.0)

    try:
        port = server.actual_port
        r1, w1 = await pool.acquire("127.0.0.1", port)
        w1.write(Message.ping(sender_id="c1").to_frame())
        await w1.drain()
        frame1 = await read_frame(r1)
        assert Message.from_frame(frame1).payload["seq"] == 1

        # Wait briefly for FIN to arrive on r1 so r1.at_eof() becomes True
        await r1.read(1)
        assert r1.at_eof()
        # Even if released when not yet closed, or tested directly via _pool:
        # Force stale entry into pool to specifically exercise _pop_idle at_eof() eviction
        pool._pool[("127.0.0.1", port)] = [(r1, w1)]

        r2, w2 = await pool.acquire("127.0.0.1", port)
        assert (r2, w2) != (r1, w1)
        assert w1.is_closing()

        w2.write(Message.ping(sender_id="c1").to_frame())
        await w2.drain()
        frame2 = await read_frame(r2)
        assert Message.from_frame(frame2).payload["seq"] == 2
        await pool.release("127.0.0.1", port, r2, w2)
    finally:
        await pool.close_all()
        await server.stop()


@pytest.mark.asyncio
async def test_connection_pool_close_all_release_race_no_leak():
    async def handler(msg: Message, writer: asyncio.StreamWriter) -> Message | None:
        return Message.pong(sender_id="server")

    server = AsyncTCPServer("127.0.0.1", 0, handler=handler)
    await server.start()
    pool = ConnectionPool(max_idle=4, timeout=2.0)

    try:
        port = server.actual_port
        r1, w1 = await pool.acquire("127.0.0.1", port)
        r2, w2 = await pool.acquire("127.0.0.1", port)

        # Race close_all() and release() concurrently
        await asyncio.gather(
            pool.close_all(),
            pool.release("127.0.0.1", port, r1, w1),
            pool.release("127.0.0.1", port, r2, w2),
        )

        # Release after close_all must close writer and never repopulate pool
        assert pool._closed is True
        assert pool._pool == {}
        assert w1.is_closing()
        assert w2.is_closing()

        with pytest.raises(ConnectionFailedError, match="closed"):
            await pool.acquire("127.0.0.1", port)
    finally:
        await pool.close_all()
        await server.stop()


@pytest.mark.asyncio
async def test_connection_pool_per_call_timeout_in_acquire(monkeypatch):
    pool = ConnectionPool(max_idle=2, timeout=10.0)

    async def stalled_open_connection(*args, **kwargs):
        await asyncio.sleep(5.0)

    monkeypatch.setattr(asyncio, "open_connection", stalled_open_connection)

    try:
        with pytest.raises(ConnectionTimeoutError, match="timed out"):
            await asyncio.wait_for(pool.acquire("127.0.0.1", 9999, timeout=0.05), timeout=1.0)
    finally:
        await pool.close_all()


@pytest.mark.asyncio
async def test_tcp_server_read_timeout_slowloris_closure():
    server = AsyncTCPServer("127.0.0.1", 0, read_timeout=0.1)
    await server.start()

    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", server.actual_port)
        # Send partial header only (Slowloris) and wait
        writer.write(b"MSHW")
        await writer.drain()

        eof_data = await asyncio.wait_for(reader.read(64), timeout=2.0)
        assert eof_data == b""
        writer.close()
        await writer.wait_closed()
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_tcp_server_malformed_message_returns_invalid_message():
    server = AsyncTCPServer("127.0.0.1", 0, read_timeout=2.0)
    await server.start()

    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", server.actual_port)
        # Valid binary frame, invalid Message JSON payload
        bad_frame = encode_frame(Opcode.PING, b'{"missing_required_fields": true}', flags=FrameFlags.JSON)
        writer.write(bad_frame)
        await writer.drain()

        resp_frame = await asyncio.wait_for(read_frame(reader), timeout=2.0)
        resp_msg = Message.from_frame(resp_frame)
        assert resp_msg.msg_type == "error"
        assert resp_msg.payload["code"] == "INVALID_MESSAGE"

        # Server terminates the malformed connection after replying
        assert await asyncio.wait_for(reader.read(64), timeout=2.0) == b""
        writer.close()
        await writer.wait_closed()
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_tcp_server_handler_error_and_broken_pipe_safety():
    client_disconnected = asyncio.Event()

    async def flaky_handler(msg: Message, writer: asyncio.StreamWriter) -> Message | None:
        if msg.payload.get("mode") == "raise":
            raise RuntimeError("sensitive internal traceback detail")
        if msg.payload.get("mode") == "slow_reply":
            await asyncio.sleep(0.1)
            client_disconnected.set()
            return Message.pong(sender_id="server")
        return Message.pong(sender_id="server")

    server = AsyncTCPServer("127.0.0.1", 0, handler=flaky_handler, read_timeout=2.0)
    await server.start()
    client = AsyncTCPClient(pool_connections=False)

    try:
        port = server.actual_port

        # 1. Handler exception returns sanitized HANDLER_ERROR
        err_resp = await client.send_and_receive(
            "127.0.0.1",
            port,
            Message.ping(sender_id="c1", payload={"mode": "raise"}),
        )
        assert err_resp.msg_type == "error"
        assert err_resp.payload["code"] == "HANDLER_ERROR"
        assert "sensitive internal" not in err_resp.payload["error"]

        # 2. Client drops connection mid-handler -> server absorbs broken pipe cleanly
        _, raw_writer = await asyncio.open_connection("127.0.0.1", port)
        raw_writer.write(Message.ping(sender_id="c2", payload={"mode": "slow_reply"}).to_frame())
        await raw_writer.drain()
        raw_writer.close()
        await raw_writer.wait_closed()
        await asyncio.wait_for(client_disconnected.wait(), timeout=2.0)
        await asyncio.sleep(0.05)

        # 3. Server remains healthy after both error paths
        ok_resp = await client.send_and_receive(
            "127.0.0.1",
            port,
            Message.ping(sender_id="c3"),
        )
        assert ok_resp.msg_type == "pong"
    finally:
        await client.close()
        await server.stop()


@pytest.mark.asyncio
async def test_tcp_server_stop_cancels_active_handlers():
    handler_started = asyncio.Event()
    handler_cancelled = asyncio.Event()

    async def blocking_handler(msg: Message, writer: asyncio.StreamWriter) -> Message | None:
        handler_started.set()
        try:
            await asyncio.sleep(30.0)
        except asyncio.CancelledError:
            handler_cancelled.set()
            raise
        return None

    server = AsyncTCPServer("127.0.0.1", 0, handler=blocking_handler, read_timeout=5.0)
    await server.start()

    reader, writer = await asyncio.open_connection("127.0.0.1", server.actual_port)
    try:
        writer.write(Message.ping(sender_id="c1").to_frame())
        await writer.drain()
        await asyncio.wait_for(handler_started.wait(), timeout=2.0)

        await asyncio.wait_for(server.stop(), timeout=2.0)
        assert handler_cancelled.is_set()
        assert len(server._client_tasks) == 0
        assert len(server._active_connections) == 0
    finally:
        writer.close()
        await writer.wait_closed()
        await server.stop()
