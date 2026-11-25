"""MeshWeaver Command Line Interface (CLI) entry point."""

import argparse
import asyncio
import json
import logging
import signal
import sys
from typing import Any
from meshweaver.core.config import NodeConfig
from meshweaver.core.node import Node
from meshweaver.execution.task import Task
from meshweaver.monitoring.dashboard import TerminalDashboard
from meshweaver.networking.protocol import Message
from meshweaver.utils.helpers import json_dumps, json_loads


def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


async def run_node_daemon(config: NodeConfig, show_dashboard: bool = False) -> None:
    """Run MeshWeaver node in daemon mode with optional live Rich TUI."""
    node = Node(config)
    await node.start()

    dashboard: TerminalDashboard | None = None
    if show_dashboard:
        dashboard = TerminalDashboard(node)
        await dashboard.start()

    stop_event = asyncio.Event()

    def signal_handler() -> None:
        stop_event.set()

    # Handle graceful exit on Windows & POSIX
    try:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, signal_handler)
            except NotImplementedError:
                # Signal handlers not fully supported on some Windows event loops
                pass
    except Exception:
        pass

    try:
        print(f"\n[+] MeshWeaver node {node.node_id[:8]} started on {config.network.host}:{node.info.tcp_port}")
        print("    Press Ctrl+C to stop.\n")
        while not stop_event.is_set():
            await asyncio.sleep(0.5)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        print("\n[*] Shutting down MeshWeaver cleanly...")
        if dashboard:
            await dashboard.stop()
        await node.stop()
        print("[✓] Node stopped.")


def _parse_target(target: str) -> tuple[str, int]:
    """Parse host:port or [ipv6]:port target address."""
    if ":" not in target:
        raise ValueError(f"Invalid target '{target}': expected host:port")
    host_part, port_part = target.rsplit(":", 1)
    if host_part.startswith("[") and host_part.endswith("]"):
        host_part = host_part[1:-1]
    elif host_part.startswith("[") or host_part.endswith("]") or ":" in host_part:
        raise ValueError(f"IPv6 target '{target}' must bracket the host as [host]:port")
    if not host_part:
        raise ValueError(f"Invalid target '{target}': empty host")
    port = int(port_part)
    if not (1 <= port <= 65535):
        raise ValueError(f"Invalid port {port}: must be between 1 and 65535")
    return host_part, port


async def submit_task_cmd(args: argparse.Namespace) -> None:
    """Submit a task to a MeshWeaver node from the CLI."""
    target_host, target_port = _parse_target(args.target)

    task_args = json_loads(args.args) if args.args else []
    if not isinstance(task_args, list):
        raise ValueError("--args must be a JSON array (list)")
    task_kwargs = json_loads(args.kwargs) if args.kwargs else {}
    if not isinstance(task_kwargs, dict):
        raise ValueError("--kwargs must be a JSON object (dict)")

    task = Task(
        name=args.task_name,
        payload={"args": task_args, "kwargs": task_kwargs},
        timeout=args.timeout,
    )

    client_cfg = NodeConfig.from_env()
    client_cfg.name = "cli-submitter"
    client_cfg.network.tcp_port = 0
    client_cfg.network.udp_port = 0
    client_cfg.discovery.enabled = False
    client_cfg.gossip.enabled = False
    client_cfg.storage.enabled = False
    if args.secret:
        client_cfg.security.enabled = True
        client_cfg.security.require_signatures = True
        client_cfg.security.shared_secret = args.secret

    client_node = Node(client_cfg, server_tls=False)

    try:
        print(f"Submitting task '{args.task_name}' to {target_host}:{target_port}...")
        result = await client_node.submit_task_to_peer(
            host=target_host,
            port=target_port,
            task=task,
            timeout=args.timeout + 5.0,
        )

        if result.is_success:
            print("\n[SUCCESS] Task execution completed!")
            print(f"Worker Node: {result.worker_node_id}")
            print(f"Duration:    {result.execution_time_sec:.3f}s" if result.execution_time_sec is not None else "Duration: N/A")
            print(f"Result:      {json_dumps(result.result)}")
        else:
            print("\n[FAILED] Task execution failed!")
            print(f"Error: {result.error}")
            sys.exit(1)
    finally:
        await client_node.stop()


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="meshweaver",
        description="MeshWeaver: Zero-Dependency P2P Asynchronous Task Broker for Edge Computing",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Command: start
    start_p = subparsers.add_parser("start", help="Start a MeshWeaver node")
    start_p.add_argument("--name", default=None, help="Friendly name of this node (default: env or mesh-node)")
    start_p.add_argument("--host", default=None, help="Network host interface to bind (default: env or 127.0.0.1)")
    start_p.add_argument("--port", type=int, default=None, help="TCP port to bind (default: env or 8765, 0 for dynamic)")
    start_p.add_argument("--udp-port", type=int, default=None, help="UDP port to bind (default: env or 8766, 0 for dynamic)")
    start_p.add_argument("--bootstrap", help="Comma-separated bootstrap peers host:port")
    start_p.add_argument("--secret", help="Shared cluster security secret for HMAC signing")
    start_p.add_argument("--dashboard", action="store_true", help="Launch live terminal dashboard (requires rich)")
    start_p.add_argument("--log-level", default=None, choices=["DEBUG", "INFO", "WARNING", "ERROR"])

    # Command: submit
    submit_p = subparsers.add_parser("submit", help="Submit a task to a MeshWeaver cluster")
    submit_p.add_argument("task_name", help="Registered task handler name (e.g. math.add)")
    submit_p.add_argument("--target", default="127.0.0.1:8765", help="Target node address (host:port or [ipv6]:port)")
    submit_p.add_argument("--args", default="[]", help="JSON array of positional arguments, e.g. '[10, 20]'")
    submit_p.add_argument("--kwargs", default="{}", help="JSON object of keyword arguments")
    submit_p.add_argument("--timeout", type=float, default=60.0, help="Task timeout in seconds")
    submit_p.add_argument("--secret", help="Shared cluster security secret for HMAC signing")

    args = parser.parse_args()

    if args.command == "start":
        cfg = NodeConfig.from_env()
        if args.name is not None:
            cfg.name = args.name
        if args.host is not None:
            cfg.network.host = args.host
        if args.port is not None:
            cfg.network.tcp_port = args.port
        if args.udp_port is not None:
            cfg.network.udp_port = args.udp_port
        if args.bootstrap:
            cfg.discovery.bootstrap_peers = [p.strip() for p in args.bootstrap.split(",") if p.strip()]
        if args.secret:
            cfg.security.enabled = True
            cfg.security.require_signatures = True
            cfg.security.shared_secret = args.secret

        log_level = args.log_level or cfg.monitoring.log_level
        setup_logging(log_level if not args.dashboard else "ERROR")

        try:
            asyncio.run(run_node_daemon(cfg, show_dashboard=args.dashboard))
        except KeyboardInterrupt:
            pass

    elif args.command == "submit":
        cfg = NodeConfig.from_env()
        setup_logging(cfg.monitoring.log_level if cfg.monitoring.log_level else "WARNING")
        try:
            asyncio.run(submit_task_cmd(args))
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
