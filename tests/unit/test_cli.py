"""Unit tests for CLI helpers, NodeConfig.from_env, and asymmetric reboot persistence."""

import argparse
import asyncio
import pytest

from meshweaver.__main__ import _parse_target, submit_task_cmd
from meshweaver.core.config import NodeConfig
from meshweaver.core.node import Node
from meshweaver.execution.task import Task, TaskState
from meshweaver.utils.exceptions import ConfigurationError


def test_parse_target_ipv4_ipv6_and_validation():
    assert _parse_target("127.0.0.1:8765") == ("127.0.0.1", 8765)
    assert _parse_target("[::1]:8765") == ("::1", 8765)
    assert _parse_target("[2001:db8::1]:9000") == ("2001:db8::1", 9000)

    with pytest.raises(ValueError, match="expected host:port"):
        _parse_target("localhost")
    with pytest.raises(ValueError, match="bracket"):
        _parse_target("::1:8765")
    with pytest.raises(ValueError, match="Invalid port"):
        _parse_target("127.0.0.1:70000")
    with pytest.raises(ValueError, match="empty host"):
        _parse_target(":8765")


def test_node_config_from_env_and_validate(monkeypatch):
    monkeypatch.setenv("MESHWEAVER_SECRET", "env_secret_alias")
    monkeypatch.setenv("MESHWEAVER_LOG_LEVEL", "debug")
    monkeypatch.setenv("MESHWEAVER_ADMIN_NODES", "admin1, admin2")
    monkeypatch.setenv("MESHWEAVER_ALLOW_TOFU", "false")
    monkeypatch.setenv("MESHWEAVER_NAME", "cli-node")

    cfg = NodeConfig.from_env()
    assert cfg.security.shared_secret == "env_secret_alias"
    assert cfg.security.enabled is True
    assert cfg.monitoring.log_level == "DEBUG"
    assert cfg.security.admin_nodes == ["admin1", "admin2"]
    assert cfg.security.allow_tofu is False
    assert cfg.name == "cli-node"

    # MESHWEAVER_SHARED_SECRET takes precedence over MESHWEAVER_SECRET when both set
    monkeypatch.setenv("MESHWEAVER_SHARED_SECRET", "primary_secret")
    cfg2 = NodeConfig.from_env()
    assert cfg2.security.shared_secret == "primary_secret"

    # TLS validation fails closed when cert/key missing
    cfg2.network.tls_enabled = True
    with pytest.raises(ConfigurationError, match="tls_cert_path"):
        cfg2.validate()


@pytest.mark.asyncio
async def test_submit_task_cmd_validates_args_kwargs_and_executes(capsys):
    # Invalid --args (not a JSON list)
    bad_args_ns = argparse.Namespace(
        target="127.0.0.1:8765",
        task_name="math.add",
        args='{"not": "a list"}',
        kwargs="{}",
        timeout=5.0,
        secret=None,
    )
    with pytest.raises(ValueError, match="JSON array"):
        await submit_task_cmd(bad_args_ns)

    # Invalid --kwargs (not a JSON dict)
    bad_kwargs_ns = argparse.Namespace(
        target="127.0.0.1:8765",
        task_name="math.add",
        args="[1, 2]",
        kwargs="[1, 2]",
        timeout=5.0,
        secret=None,
    )
    with pytest.raises(ValueError, match="JSON object"):
        await submit_task_cmd(bad_kwargs_ns)

    # Valid submission against an authenticated live node
    secret = "cli_submit_secret"
    srv_cfg = NodeConfig(name="cli-target")
    srv_cfg.network.tcp_port = 0
    srv_cfg.network.udp_port = 0
    srv_cfg.security.enabled = True
    srv_cfg.security.require_signatures = True
    srv_cfg.security.shared_secret = secret
    server_node = Node(srv_cfg)
    await server_node.start()

    try:
        ok_ns = argparse.Namespace(
            target=f"[::1]:{server_node.tcp_server.actual_port}"
            if ":" in server_node.config.network.host
            else f"127.0.0.1:{server_node.tcp_server.actual_port}",
            task_name="math.add",
            args="[14, 28]",
            kwargs="{}",
            timeout=5.0,
            secret=secret,
        )
        await submit_task_cmd(ok_ns)
        out = capsys.readouterr().out
        assert "[SUCCESS]" in out
        assert "42" in out
    finally:
        await server_node.stop()


@pytest.mark.asyncio
async def test_node_asymmetric_reboot_keypair_restoration_and_task_resumption(tmp_path):
    db_file = str(tmp_path / "asym_reboot.db")

    def make_cfg() -> NodeConfig:
        cfg = NodeConfig(name="asym-reboot-node")
        cfg.network.tcp_port = 0
        cfg.network.udp_port = 0
        cfg.discovery.enable_lan_broadcast = False
        cfg.security.enabled = True
        cfg.security.require_signatures = True
        cfg.security.shared_secret = None
        cfg.storage.enabled = True
        cfg.storage.db_path = db_file
        return cfg

    node1 = Node(make_cfg())
    node2 = None
    try:
        await node1.start()
        orig_node_id = node1.node_id
        orig_pub_hex = node1.keys.public_key_hex
        orig_priv_pem = node1.keys.private_key_pem
        assert orig_priv_pem is not None

        task = Task(name="math.add", payload={"args": [7, 8]})
        task.transition_to(TaskState.ASSIGNED)
        await node1.store.save_task(task)
        await node1.stop()

        node2 = Node(make_cfg())
        await node2.start()

        # Keypair and node_id are restored across reboot
        assert node2.node_id == orig_node_id
        assert node2.keys.public_key_hex == orig_pub_hex
        assert node2.keys.private_key_pem == orig_priv_pem

        # Incomplete task starts in RETRYING and resumes automatically
        assert task.task_id in node2.tasks
        assert node2.tasks[task.task_id].state == TaskState.RETRYING

        for _ in range(30):
            if node2.tasks[task.task_id].state == TaskState.COMPLETED:
                break
            await asyncio.sleep(0.05)

        assert node2.tasks[task.task_id].state == TaskState.COMPLETED
        assert node2.tasks[task.task_id].result == 15
    finally:
        await node1.stop()
        if node2 is not None:
            await node2.stop()
