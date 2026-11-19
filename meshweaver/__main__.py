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


async def submit_task_cmd(args: argparse.Namespace) -> None:
    """Submit a task to a MeshWeaver node from the CLI."""
    target_host, target_port = args.target.split(":")
    target_port = int(target_port)

    task_args = json_loads(args.args) if args.args else []
    task_kwargs = json_loads(args.kwargs) if args.kwargs else {}

    task = Task(
        name=args.task_name,
        payload={"args": task_args, "kwargs": task_kwargs},
        timeout=args.timeout,
    )

    client_cfg = NodeConfig(name="cli-submitter")
    client_cfg.network.tcp_port = 0
    client_cfg.network.udp_port = 0
    client_node = Node(client_cfg)

    await client_node.start()
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
            print(f"Duration:    {result.execution_time_sec:.3f}s" if result.execution_time_sec else "Duration: N/A")
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
    start_p.add_argument("--name", default="mesh-node", help="Friendly name of this node")
    start_p.add_argument("--host", default="127.0.0.1", help="Network host interface to bind")
    start_p.add_argument("--port", type=int, default=8765, help="TCP port to bind (default: 8765, 0 for dynamic)")
    start_p.add_argument("--udp-port", type=int, default=8766, help="UDP port to bind (default: 8766, 0 for dynamic)")
    start_p.add_argument("--bootstrap", help="Comma-separated bootstrap peers host:port")
    start_p.add_argument("--secret", help="Shared cluster security secret for HMAC signing")
    start_p.add_argument("--dashboard", action="store_true", help="Launch live terminal dashboard (requires rich)")
    start_p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])

    # Command: submit
    submit_p = subparsers.add_parser("submit", help="Submit a task to a MeshWeaver cluster")
    submit_p.add_argument("task_name", help="Registered task handler name (e.g. math.add)")
    submit_p.add_argument("--target", default="127.0.0.1:8765", help="Target node address (host:port)")
    submit_p.add_argument("--args", default="[]", help="JSON array of positional arguments, e.g. '[10, 20]'")
    submit_p.add_argument("--kwargs", default="{}", help="JSON object of keyword arguments")
    submit_p.add_argument("--timeout", type=float, default=60.0, help="Task timeout in seconds")

    args = parser.parse_args()

    if args.command == "start":
        setup_logging(args.log_level if not args.dashboard else "ERROR")
        cfg = NodeConfig(name=args.name)
        cfg.network.host = args.host
        cfg.network.tcp_port = args.port
        cfg.network.udp_port = args.udp_port
        if args.bootstrap:
            cfg.discovery.bootstrap_peers = [p.strip() for p in args.bootstrap.split(",") if p.strip()]
        if args.secret:
            cfg.security.enabled = True
            cfg.security.require_signatures = True
            cfg.security.shared_secret = args.secret

        try:
            asyncio.run(run_node_daemon(cfg, show_dashboard=args.dashboard))
        except KeyboardInterrupt:
            pass

    elif args.command == "submit":
        setup_logging("WARNING")
        try:
            asyncio.run(submit_task_cmd(args))
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
