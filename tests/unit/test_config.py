"""Unit tests for configuration models."""

import os
from meshweaver.core.config import NodeConfig


def test_default_config():
    cfg = NodeConfig()
    assert len(cfg.node_id) == 40  # 160-bit hex string
    assert cfg.network.host == "127.0.0.1"
    assert cfg.network.tcp_port == 8765
    assert cfg.network.udp_port == 8766
    assert cfg.scheduler.strategy == "multi_metric"
    assert cfg.security.allow_cloudpickle is False


def test_from_env(monkeypatch):
    monkeypatch.setenv("MESHWEAVER_NAME", "worker-1")
    monkeypatch.setenv("MESHWEAVER_HOST", "0.0.0.0")
    monkeypatch.setenv("MESHWEAVER_TCP_PORT", "9999")
    monkeypatch.setenv("MESHWEAVER_ALLOW_CLOUDPICKLE", "true")

    cfg = NodeConfig.from_env()
    assert cfg.name == "worker-1"
    assert cfg.network.host == "0.0.0.0"
    assert cfg.network.tcp_port == 9999
    assert cfg.security.allow_cloudpickle is True
