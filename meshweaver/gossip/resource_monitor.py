"""Zero-dependency CPU and RAM resource monitor for Linux (edge/RPi), macOS, and Windows."""

import asyncio
import ctypes
import os
import platform
import subprocess
import threading
from typing import Any
from meshweaver.core.state import ResourceMetrics
from meshweaver.utils.helpers import utc_now


class ResourceMonitor:
    """Collects real-time CPU and RAM metrics without external dependencies."""

    def __init__(self, max_tasks: int = 8, min_interval: float = 0.5) -> None:
        self.max_tasks = max_tasks
        self.min_interval = min_interval
        self.os_type = platform.system()
        self._prev_cpu_times: tuple[Any, ...] | None = None
        self._prev_sample_time: float = 0.0
        self._cached_metrics: ResourceMetrics | None = None
        self._lock = threading.Lock()

        # Prime initial CPU baseline without populating sample cache
        self._get_cpu_percent(0.0)

    def sample(self, active_tasks: int = 0) -> ResourceMetrics:
        """Collect current system resource usage with interval caching."""
        with self._lock:
            now = utc_now()
            if self._cached_metrics is not None and (now - self._prev_sample_time) < self.min_interval:
                return ResourceMetrics(
                    cpu_percent=self._cached_metrics.cpu_percent,
                    memory_total_mb=self._cached_metrics.memory_total_mb,
                    memory_available_mb=self._cached_metrics.memory_available_mb,
                    memory_percent=self._cached_metrics.memory_percent,
                    active_tasks=active_tasks,
                    max_tasks=self.max_tasks,
                    timestamp=now,
                )

            cpu_percent = self._get_cpu_percent(now)
            total_mb, avail_mb, mem_percent = self._get_memory_info()
            metrics = ResourceMetrics(
                cpu_percent=round(cpu_percent, 2),
                memory_total_mb=round(total_mb, 2),
                memory_available_mb=round(avail_mb, 2),
                memory_percent=round(mem_percent, 2),
                active_tasks=active_tasks,
                max_tasks=self.max_tasks,
                timestamp=now,
            )
            self._cached_metrics = metrics
            self._prev_sample_time = now
            return metrics

    async def sample_async(self, active_tasks: int = 0) -> ResourceMetrics:
        """Asynchronously collect current system resource usage without blocking the event loop."""
        return await asyncio.to_thread(self.sample, active_tasks)

    def _get_cpu_percent(self, now: float) -> float:
        """Calculate CPU percentage cross-platform."""
        if self.os_type == "Linux":
            return self._get_linux_cpu()
        elif self.os_type == "Windows":
            return self._get_windows_cpu()
        elif self.os_type == "Darwin":
            return self._get_darwin_cpu()
        return 0.0

    def _get_linux_cpu(self) -> float:
        """Calculate CPU usage on Linux by reading /proc/stat."""
        try:
            with open("/proc/stat", "r") as f:
                line = f.readline()
            parts = line.split()[1:8]
            values = [float(x) for x in parts]
            user, nice, system, idle, iowait, irq, softirq = values

            idle_time = idle + iowait
            total_time = sum(values)

            if self._prev_cpu_times is not None:
                prev_idle, prev_total = self._prev_cpu_times
                idle_delta = idle_time - prev_idle
                total_delta = total_time - prev_total
                self._prev_cpu_times = (idle_time, total_time)
                if total_delta > 0:
                    usage = 100.0 * (1.0 - (idle_delta / total_delta))
                    return max(0.0, min(100.0, usage))

            self._prev_cpu_times = (idle_time, total_time)
            return 0.0
        except Exception:
            return 0.0

    def _get_windows_cpu(self) -> float:
        """Calculate CPU usage on Windows using ctypes kernel32.GetSystemTimes."""
        try:
            class FILETIME(ctypes.Structure):
                _fields_ = [("dwLowDateTime", ctypes.c_uint), ("dwHighDateTime", ctypes.c_uint)]

            def ft_to_int(ft: FILETIME) -> int:
                return (ft.dwHighDateTime << 32) | ft.dwLowDateTime

            idle_ft = FILETIME()
            kernel_ft = FILETIME()
            user_ft = FILETIME()

            kernel32 = ctypes.windll.kernel32  # type: ignore
            success = kernel32.GetSystemTimes(
                ctypes.byref(idle_ft),
                ctypes.byref(kernel_ft),
                ctypes.byref(user_ft),
            )
            if not success:
                return 0.0

            idle = ft_to_int(idle_ft)
            kernel = ft_to_int(kernel_ft)
            user = ft_to_int(user_ft)

            if self._prev_cpu_times is not None:
                prev_idle, prev_kernel, prev_user = self._prev_cpu_times
                idle_delta = idle - prev_idle
                kernel_delta = kernel - prev_kernel
                user_delta = user - prev_user
                total_delta = kernel_delta + user_delta

                self._prev_cpu_times = (idle, kernel, user)
                if total_delta > 0:
                    cpu = 100.0 * (1.0 - (idle_delta / total_delta))
                    return max(0.0, min(100.0, cpu))

            self._prev_cpu_times = (idle, kernel, user)
            return 0.0
        except Exception:
            return 0.0

    def _get_memory_info(self) -> tuple[float, float, float]:
        """Return (total_mb, available_mb, percent_used)."""
        if self.os_type == "Linux":
            return self._get_linux_memory()
        elif self.os_type == "Windows":
            return self._get_windows_memory()
        elif self.os_type == "Darwin":
            return self._get_darwin_memory()
        # Generic fallback
        return (1024.0, 512.0, 50.0)

    def _get_darwin_cpu(self) -> float:
        """Estimate CPU usage percentage on macOS via 1-minute load average."""
        try:
            load_1m = os.getloadavg()[0]
            cpu_count = os.cpu_count() or 1
            return max(0.0, min(100.0, (load_1m / cpu_count) * 100.0))
        except Exception:
            return 0.0

    def _get_darwin_memory(self) -> tuple[float, float, float]:
        """Query macOS memory via sysctl and vm_stat."""
        try:
            total_bytes_out = subprocess.check_output(
                ["sysctl", "-n", "hw.memsize"],
                text=True,
                timeout=1.0,
            ).strip()
            total_mb = int(total_bytes_out) / (1024.0 * 1024.0)

            vm_out = subprocess.check_output(["vm_stat"], text=True, timeout=1.0)
            page_size = 4096
            first_line = vm_out.splitlines()[0] if vm_out else ""
            if "page size of" in first_line:
                for token in first_line.split():
                    if token.isdigit():
                        page_size = int(token)
                        break

            pages: dict[str, int] = {}
            for line in vm_out.splitlines()[1:]:
                if ":" in line:
                    k, v = line.split(":", 1)
                    v = v.strip().rstrip(".")
                    if v.isdigit():
                        pages[k.strip()] = int(v)

            avail_pages = (
                pages.get("Pages free", 0)
                + pages.get("Pages inactive", 0)
                + pages.get("Pages speculative", 0)
            )
            avail_mb = (avail_pages * page_size) / (1024.0 * 1024.0)
            avail_mb = min(total_mb, max(0.0, avail_mb))
            percent = ((total_mb - avail_mb) / total_mb) * 100.0 if total_mb > 0 else 0.0
            return (total_mb, avail_mb, percent)
        except Exception:
            return (1024.0, 512.0, 50.0)

    def _get_linux_memory(self) -> tuple[float, float, float]:
        """Parse Linux /proc/meminfo."""
        try:
            mem: dict[str, float] = {}
            with open("/proc/meminfo", "r") as f:
                for line in f:
                    parts = line.split(":")
                    if len(parts) == 2:
                        k = parts[0].strip()
                        v = float(parts[1].split()[0])
                        mem[k] = v

            total_kb = mem.get("MemTotal", 1024.0)
            avail_kb = mem.get("MemAvailable", mem.get("MemFree", 512.0))
            total_mb = total_kb / 1024.0
            avail_mb = avail_kb / 1024.0
            used_mb = total_mb - avail_mb
            percent = (used_mb / total_mb) * 100.0 if total_mb > 0 else 0.0
            return (total_mb, avail_mb, percent)
        except Exception:
            return (1024.0, 512.0, 50.0)

    def _get_windows_memory(self) -> tuple[float, float, float]:
        """Query Windows kernel32 GlobalMemoryStatusEx via ctypes."""
        try:
            class MEMORYSTATUSEX(ctypes.Structure):
                _fields_ = [
                    ("dwLength", ctypes.c_ulong),
                    ("dwMemoryLoad", ctypes.c_ulong),
                    ("ullTotalPhys", ctypes.c_ulonglong),
                    ("ullAvailPhys", ctypes.c_ulonglong),
                    ("ullTotalPageFile", ctypes.c_ulonglong),
                    ("ullAvailPageFile", ctypes.c_ulonglong),
                    ("ullTotalVirtual", ctypes.c_ulonglong),
                    ("ullAvailVirtual", ctypes.c_ulonglong),
                    ("sullAvailExtendedVirtual", ctypes.c_ulonglong),
                ]

            stat = MEMORYSTATUSEX()
            stat.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
            kernel32 = ctypes.windll.kernel32  # type: ignore
            if kernel32.GlobalMemoryStatusEx(ctypes.byref(stat)):
                total_mb = stat.ullTotalPhys / (1024.0 * 1024.0)
                avail_mb = stat.ullAvailPhys / (1024.0 * 1024.0)
                percent = float(stat.dwMemoryLoad)
                return (total_mb, avail_mb, percent)
        except Exception:
            pass
        return (1024.0, 512.0, 50.0)
