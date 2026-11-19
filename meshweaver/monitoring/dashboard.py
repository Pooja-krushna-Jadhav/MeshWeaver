"""Live terminal dashboard for MeshWeaver using Rich, designed under Impeccable Operate mode."""

import asyncio
import datetime
import logging
import sys
import time
from typing import Any
from meshweaver.core.state import NodeStatus, PeerHealth
from meshweaver.execution.task import TaskState
from meshweaver.monitoring.metrics import MetricsCollector
from meshweaver.utils.helpers import utc_now

logger = logging.getLogger("meshweaver.dashboard")

try:
    from rich.align import Align
    from rich.console import Console, RenderableType
    from rich.layout import Layout
    from rich.live import Live
    from rich.panel import Panel
    from rich.progress import BarColumn, Progress, TextColumn
    from rich.table import Table
    from rich.text import Text
    HAS_RICH = True
except ImportError:
    HAS_RICH = False


def _format_time_delta(seconds: float) -> str:
    td = datetime.timedelta(seconds=int(seconds))
    return str(td)


def _render_bar(percent: float, width: int = 14) -> str:
    """Render a text ASCII/Unicode progress bar."""
    clamped = max(0.0, min(100.0, percent))
    filled = int(round((clamped / 100.0) * width))
    empty = width - filled
    return "█" * filled + "░" * empty


class TerminalDashboard:
    """Production-grade live terminal dashboard adhering to Impeccable Operate mode."""

    def __init__(self, node: Any, refresh_interval: float = 1.0) -> None:
        self.node = node
        self.refresh_interval = refresh_interval
        self.metrics_collector = MetricsCollector(node.events)
        self._is_running = False
        self._task: asyncio.Task | None = None
        self._console = Console() if HAS_RICH else None

    async def start(self) -> None:
        """Start live dashboard update loop."""
        if self._is_running or not HAS_RICH:
            return
        self._is_running = True
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        """Stop dashboard update loop."""
        self._is_running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run_loop(self) -> None:
        """Asynchronous live rendering loop."""
        assert self._console is not None
        with Live(console=self._console, screen=True, auto_refresh=False) as live:
            while self._is_running:
                try:
                    layout = self.render_layout()
                    live.update(layout, refresh=True)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.debug("Dashboard render error: %s", e)

                await asyncio.sleep(self.refresh_interval)

    def render_layout(self) -> Any:
        """Construct the high-density Rich layout."""
        if not HAS_RICH:
            return "Rich not available"

        layout = Layout()
        layout.split_column(
            Layout(name="header", size=4),
            Layout(name="body", ratio=1),
            Layout(name="footer", size=3),
        )

        layout["body"].split_row(
            Layout(name="left", ratio=3),
            Layout(name="right", ratio=2),
        )

        layout["left"].split_column(
            Layout(name="peers", ratio=1),
            Layout(name="tasks", ratio=1),
        )

        layout["right"].split_column(
            Layout(name="telemetry", size=13),
            Layout(name="events", ratio=1),
        )

        layout["header"].update(self._build_header())
        layout["peers"].update(self._build_peers_table())
        layout["tasks"].update(self._build_tasks_table())
        layout["telemetry"].update(self._build_telemetry_panel())
        layout["events"].update(self._build_events_panel())
        layout["footer"].update(self._build_footer())

        return layout

    def _build_header(self) -> Panel:
        uptime_sec = utc_now() - self.node.info.started_at
        uptime_str = _format_time_delta(uptime_sec)

        grid = Table.grid(expand=True)
        grid.add_column(justify="left", ratio=1)
        grid.add_column(justify="center", ratio=1)
        grid.add_column(justify="right", ratio=1)

        title = Text.assemble(
            ("⬡ ", "bold cyan"),
            ("MESHWEAVER", "bold white"),
            (" | Edge Task Broker", "dim white"),
        )

        status_style = "bold green" if self.node.status == NodeStatus.ONLINE else "bold yellow"
        center = Text.assemble(
            ("Node: ", "dim"),
            (f"{self.node.node_id[:8]}… ", "bold cyan"),
            (f"[{self.node.config.name}] ", "white"),
            (f"● {self.node.status.value}", status_style),
        )

        right = Text.assemble(
            ("TCP: ", "dim"),
            (f"{self.node.info.host}:{self.node.info.tcp_port} ", "white"),
            ("• Uptime: ", "dim"),
            (uptime_str, "white"),
        )

        grid.add_row(title, center, right)
        return Panel(grid, border_style="cyan", padding=(0, 1))

    def _build_peers_table(self) -> Panel:
        table = Table(box=None, expand=True, show_edge=False, pad_edge=False)
        table.add_column("Peer ID", style="bold cyan", width=10)
        table.add_column("Endpoint", style="white", ratio=2)
        table.add_column("Health", justify="center", width=10)
        table.add_column("RTT", justify="right", width=8)
        table.add_column("CPU", justify="right", width=7)
        table.add_column("RAM", justify="right", width=7)
        table.add_column("Tasks", justify="right", width=6)

        peers = self.node.peer_table.all_peers()
        if not peers:
            table.add_row("—", "No external peers connected yet", "IDLE", "—", "—", "—", "—")
        else:
            for p in peers:
                health_style = "bold green" if p.health == PeerHealth.HEALTHY else ("bold yellow" if p.health == PeerHealth.SUSPECT else "bold red")
                table.add_row(
                    f"{p.info.node_id[:8]}…",
                    f"{p.info.host}:{p.info.tcp_port}",
                    Text(f"● {p.health.value}", style=health_style),
                    f"{p.rtt_ms:.1f}ms",
                    f"{p.metrics.cpu_percent:.0f}%",
                    f"{p.metrics.memory_percent:.0f}%",
                    f"{p.metrics.active_tasks}/{p.metrics.max_tasks}",
                )

        return Panel(
            table,
            title="[bold white]Cluster Peers[/] [dim]({} discovered)[/]".format(len(peers)),
            border_style="blue",
            padding=(0, 1),
        )

    def _build_tasks_table(self) -> Panel:
        table = Table(box=None, expand=True, show_edge=False, pad_edge=False)
        table.add_column("Task ID", style="bold cyan", width=10)
        table.add_column("Handler", style="white", ratio=2)
        table.add_column("State", justify="center", width=12)
        table.add_column("Worker", justify="left", width=10)
        table.add_column("Duration", justify="right", width=9)

        tasks = list(self.node.tasks.values())[-6:]  # Last 6 tasks
        if not tasks:
            table.add_row("—", "No task activity in queue", "IDLE", "—", "—")
        else:
            for t in reversed(tasks):
                state_colors = {
                    TaskState.COMPLETED: "bold green",
                    TaskState.RUNNING: "bold cyan",
                    TaskState.QUEUED: "bold yellow",
                    TaskState.ASSIGNED: "bold blue",
                    TaskState.FAILED: "bold red",
                    TaskState.RETRYING: "bold dark_orange3",
                }
                color = state_colors.get(t.state, "white")
                dur_str = f"{t.execution_time_sec:.2f}s" if t.execution_time_sec is not None else "…"
                worker_str = t.assigned_node_id[:8] if t.assigned_node_id else "local"

                table.add_row(
                    f"{t.task_id[:8]}…",
                    t.name,
                    Text(t.state.value, style=color),
                    worker_str,
                    dur_str,
                )

        return Panel(
            table,
            title=f"[bold white]Task Pipeline[/] [dim]({len(self.node.tasks)} total)[/]",
            border_style="magenta",
            padding=(0, 1),
        )

    def _build_telemetry_panel(self) -> Panel:
        local_metrics = self.node.resource_monitor.sample(active_tasks=self.node.executor.active_task_count)
        snap = self.metrics_collector.snapshot()

        table = Table.grid(expand=True, padding=(0, 1))
        table.add_column(ratio=1)

        # CPU Metric row
        cpu_pct = local_metrics.cpu_percent
        cpu_color = "green" if cpu_pct < 60 else ("yellow" if cpu_pct < 85 else "red")
        cpu_bar = _render_bar(cpu_pct, width=12)
        table.add_row(
            Text.assemble(
                ("CPU Load: ", "bold"),
                (f"[{cpu_bar}] ", cpu_color),
                (f"{cpu_pct:.1f}%", f"bold {cpu_color}"),
            )
        )

        # RAM Metric row
        ram_pct = local_metrics.memory_percent
        ram_color = "green" if ram_pct < 70 else ("yellow" if ram_pct < 90 else "red")
        ram_bar = _render_bar(ram_pct, width=12)
        table.add_row(
            Text.assemble(
                ("RAM Load: ", "bold"),
                (f"[{ram_bar}] ", ram_color),
                (f"{ram_pct:.1f}% ", f"bold {ram_color}"),
                (f"({local_metrics.memory_available_mb:.0f}MB free)", "dim"),
            )
        )

        # Worker Capacity
        active = local_metrics.active_tasks
        max_t = local_metrics.max_tasks
        cap_bar = _render_bar((active / max_t) * 100.0 if max_t > 0 else 0, width=12)
        table.add_row(
            Text.assemble(
                ("Capacity: ", "bold"),
                (f"[{cap_bar}] ", "cyan"),
                (f"{active}/{max_t} slots", "white"),
            )
        )

        table.add_row(Text("─" * 32, style="dim"))

        # Task Counters
        stats_line = Text.assemble(
            ("Completed: ", "dim"), (str(snap.total_tasks_completed), "bold green"),
            ("  Failed: ", "dim"), (str(snap.total_tasks_failed), "bold red" if snap.total_tasks_failed > 0 else "white"),
            ("  Retries: ", "dim"), (str(snap.total_tasks_retried), "bold yellow" if snap.total_tasks_retried > 0 else "white"),
        )
        table.add_row(stats_line)

        return Panel(table, title="[bold white]Local Telemetry[/]", border_style="green", padding=(0, 1))

    def _build_events_panel(self) -> Panel:
        snap = self.metrics_collector.snapshot()
        table = Table(box=None, expand=True, show_edge=False, pad_edge=False, show_header=False)
        table.add_column("Time", style="dim", width=8)
        table.add_column("Event", style="white")

        if not snap.recent_events:
            table.add_row("—", "Awaiting cluster activity…")
        else:
            for t_stamp, evt_name, summary in list(snap.recent_events)[-5:]:
                time_str = datetime.datetime.fromtimestamp(t_stamp).strftime("%H:%M:%S")
                style = "green" if "completed" in evt_name else ("red" if "failed" in evt_name or "lost" in evt_name else "white")
                table.add_row(time_str, Text(summary, style=style))

        return Panel(table, title="[bold white]Live Event Stream[/]", border_style="dim white", padding=(0, 1))

    def _build_footer(self) -> Panel:
        footer_text = Text.assemble(
            ("  Commands: ", "bold white"),
            ("[Ctrl+C] ", "bold cyan"), ("Graceful Shutdown   ", "dim white"),
            ("[Gossip Interval] ", "bold cyan"), (f"{self.node.config.gossip.interval:.1f}s   ", "dim white"),
            ("[Scheduler] ", "bold cyan"), (f"{self.node.config.scheduler.strategy}   ", "dim white"),
            ("[Security] ", "bold cyan"), ("Active" if self.node.config.security.enabled else "None", "green" if self.node.config.security.enabled else "dim"),
        )
        return Panel(footer_text, border_style="dim", padding=(0, 1))
