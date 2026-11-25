"""TLS certificate generation, SSLContext enforcement, and live TLS node-to-node tests."""

import ssl
import pytest

from meshweaver.core.config import NodeConfig
from meshweaver.core.node import Node
from meshweaver.execution.task import Task, TaskState
from meshweaver.networking.tcp import create_ssl_context
from meshweaver.security.certificates import create_self_signed_cert
from meshweaver.utils.exceptions import ConfigurationError


def test_create_self_signed_cert_and_ssl_context_validation(tmp_path):
    cert_path = str(tmp_path / "node.crt")
    key_path = str(tmp_path / "node.key")

    created = create_self_signed_cert(
        cert_path=cert_path,
        key_path=key_path,
        common_name="127.0.0.1",
        ip_addresses=["127.0.0.1"],
    )
    assert created is True
    assert (tmp_path / "node.crt").exists()
    assert (tmp_path / "node.key").exists()

    # Unpaired cert/key fails closed
    with pytest.raises(ConfigurationError, match="together"):
        create_ssl_context(cert_path=cert_path, is_server=True)

    # Client SSL without ca_path fails closed unless allow_insecure_client=True
    with pytest.raises(ConfigurationError, match="ca_path"):
        create_ssl_context(cert_path=cert_path, key_path=key_path, is_server=False)

    insecure_ctx = create_ssl_context(
        cert_path=cert_path,
        key_path=key_path,
        is_server=False,
        allow_insecure_client=True,
    )
    assert insecure_ctx is not None
    assert insecure_ctx.verify_mode == ssl.CERT_NONE

    # Valid server + verified client contexts
    srv_ctx = create_ssl_context(cert_path=cert_path, key_path=key_path, ca_path=cert_path, is_server=True)
    cli_ctx = create_ssl_context(cert_path=cert_path, key_path=key_path, ca_path=cert_path, is_server=False)
    assert srv_ctx is not None and srv_ctx.verify_mode == ssl.CERT_REQUIRED
    assert cli_ctx is not None and cli_ctx.verify_mode == ssl.CERT_REQUIRED


@pytest.mark.asyncio
async def test_live_tls_node_to_node_communication_and_untrusted_rejection(tmp_path):
    cert_a = str(tmp_path / "node_a.crt")
    key_a = str(tmp_path / "node_a.key")
    assert create_self_signed_cert(cert_a, key_a, common_name="localhost", ip_addresses=["127.0.0.1"])

    other_cert = str(tmp_path / "other.crt")
    other_key = str(tmp_path / "other.key")
    assert create_self_signed_cert(other_cert, other_key, common_name="localhost", ip_addresses=["127.0.0.1"])

    def make_tls_cfg(name: str, cert: str, key: str, ca: str) -> NodeConfig:
        cfg = NodeConfig(name=name)
        cfg.network.host = "127.0.0.1"
        cfg.network.tcp_port = 0
        cfg.network.udp_port = 0
        cfg.network.tls_enabled = True
        cfg.network.tls_cert_path = cert
        cfg.network.tls_key_path = key
        cfg.network.tls_ca_path = ca
        return cfg

    node_server = Node(make_tls_cfg("tls-server", cert_a, key_a, cert_a))
    node_client = Node(make_tls_cfg("tls-client", cert_a, key_a, cert_a))
    node_untrusted = Node(make_tls_cfg("tls-untrusted", other_cert, other_key, other_cert))

    await node_server.start()
    await node_client.start()
    await node_untrusted.start()

    try:
        # Trusted TLS ping_tcp & task submission succeed
        pong = await node_client.ping_tcp("127.0.0.1", node_server.tcp_server.actual_port)
        assert pong.msg_type == "pong"
        assert pong.sender_id == node_server.node_id

        task = Task(name="math.add", payload={"args": [19, 23]})
        res = await node_client.submit_task_to_peer("127.0.0.1", node_server.tcp_server.actual_port, task)
        assert res.is_success
        assert res.result == 42
        assert task.state == TaskState.COMPLETED

        # Untrusted certificate fails TLS verification
        with pytest.raises(Exception):
            await node_untrusted.ping_tcp("127.0.0.1", node_server.tcp_server.actual_port, timeout=1.0)
    finally:
        await node_client.stop()
        await node_untrusted.stop()
        await node_server.stop()
