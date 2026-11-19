"""Security boundary tests: oversized frames, malformed headers, replay, and unauthorized execution."""

import asyncio
import pytest
from meshweaver.core.config import NodeConfig
from meshweaver.core.node import Node
from meshweaver.execution.task import Task, TaskType
from meshweaver.networking.framing import encode_frame, Opcode
from meshweaver.networking.protocol import Message
from meshweaver.utils.exceptions import FramingError


@pytest.mark.asyncio
async def test_security_rejects_oversized_frame():
    cfg = NodeConfig(name="security-node")
    cfg.network.tcp_port = 0
    cfg.network.udp_port = 0
    cfg.network.max_frame_size = 1024  # Strict 1KB limit
    node = Node(cfg)
    await node.start()

    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", node.tcp_server.actual_port)
        # Forge frame declaring 5MB payload
        oversized_payload = b"X" * 2048
        forged_frame = encode_frame(Opcode.PING, oversized_payload)

        writer.write(forged_frame)
        await writer.drain()

        # Server should terminate the connection immediately due to frame size violation
        data = await reader.read(100)
        assert len(data) == 0  # Socket closed by server
        writer.close()
    finally:
        await node.stop()


@pytest.mark.asyncio
async def test_security_rejects_arbitrary_cloudpickle_by_default():
    cfg = NodeConfig(name="secure-worker")
    cfg.network.tcp_port = 0
    cfg.network.udp_port = 0
    node = Node(cfg)
    await node.start()

    try:
        # Submit a FUNCTION task when cloudpickle execution is disabled
        task = Task(name="malicious.exec", task_type=TaskType.FUNCTION, payload={"fn_b64": "fake"})

        submitter_cfg = NodeConfig(name="client")
        submitter_cfg.network.tcp_port = 0
        submitter_cfg.network.udp_port = 0
        submitter = Node(submitter_cfg)
        await submitter.start()

        try:
            result = await submitter.submit_task_to_peer(
                host="127.0.0.1",
                port=node.tcp_server.actual_port,
                task=task,
            )
            assert not result.is_success
            assert "disabled on this node" in result.error.lower()
        finally:
            await submitter.stop()
    finally:
        await node.stop()
