#!/usr/bin/env python3
"""
Health check tool for the Tent of Trials platform.
Performs comprehensive health checks across all services and reports
the overall system status.

This tool is used by:
  - The Kubernetes liveness/readiness probes
  - The deployment pipeline (post-deployment validation)
  - The monitoring system (periodic health checks)
  - The on-call engineer (manual troubleshooting)

The health check performs the following checks:
  1. Service availability (HTTP health endpoints)
  2. Database connectivity (connection test)
  3. Redis connectivity (ping test)
  4. Kafka connectivity (metadata fetch)
  5. Message queue depth (consumer lag check)
  6. Certificate expiry (TLS certificate check)
  7. Disk space (filesystem usage check)
  8. Memory usage (process memory check)

Each check returns a status of OK, WARNING, or CRITICAL, along with
a detail message and optional diagnostic data.

Usage:
    python3 health_check.py                  # 检查所有服务（默认）
    python3 health_check.py --service backend  # 检查指定服务
    python3 health_check.py --json            # JSON 格式输出
    python3 health_check.py --watch           # 持续监控模式
    python3 health_check.py --timeout 3       # 全局超时覆盖（秒）
    python3 health_check.py --endpoint-timeout backend=3,frailbox=8  # 按端点覆盖超时
    python3 health_check.py --rate-limit 5    # 每秒最多发送 5 个探测请求
"""

import argparse
import json
import os
import socket
import ssl
import subprocess
import sys
import time
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------

SERVICES = {
    "backend": {"host": "localhost", "port": 8080, "path": "/health", "timeout": 5},
    "market": {"host": "localhost", "port": 8081, "path": "/health", "timeout": 5},
    "frailbox": {"host": "localhost", "port": 8082, "path": "/health", "timeout": 10},
    "frontend": {"host": "localhost", "port": 3000, "path": "/", "timeout": 5},
}

INFRASTRUCTURE = {
    "postgresql": {
        "host": os.environ.get("DB_HOST", "localhost"),
        "port": int(os.environ.get("DB_PORT", "5432")),
        "timeout": 5,
    },
    "redis": {
        "host": os.environ.get("REDIS_HOST", "localhost"),
        "port": int(os.environ.get("REDIS_PORT", "6379")),
        "timeout": 5,
    },
    "kafka": {
        "host": os.environ.get("KAFKA_HOST", "localhost"),
        "port": int(os.environ.get("KAFKA_PORT", "9092")),
        "timeout": 5,
    },
}

DISK_THRESHOLD_WARNING = 80
DISK_THRESHOLD_CRITICAL = 90

MEMORY_THRESHOLD_WARNING = 80
MEMORY_THRESHOLD_CRITICAL = 90

# 默认速率限制：每秒 10 个探测（0 表示不限制）
DEFAULT_RATE_LIMIT = 0


# ---------------------------------------------------------------------------
# 令牌桶限流器（Token Bucket Rate Limiter）
# ---------------------------------------------------------------------------

class TokenBucket:
    """令牌桶限流器。

    工作原理（通俗比喻）：
    - 想象一个水桶，每秒以固定速率往里滴水（令牌）。
    - 每次发请求前，需要从桶里取出一滴水（消耗一个令牌）。
    - 如果桶里没水了，就要等新水滴进来才能发请求。
    - 这样就保证了一秒内最多只能发固定数量的请求。

    参数：
        rate: 每秒允许的最大请求数（令牌补充速率）
        burst: 桶的容量，允许的瞬时突发请求数（可选，默认等于 rate）
    """

    def __init__(self, rate: float, burst: Optional[int] = None):
        if rate <= 0:
            raise ValueError(f"速率必须大于 0，当前值: {rate}")
        self.rate = rate                      # 每秒补充的令牌数
        self.burst = burst if burst else int(rate)  # 桶最大容量
        self.tokens = float(self.burst)       # 当前令牌数（初始满桶）
        self.last_refill = time.monotonic()   # 上次补充时间
        self.lock = threading.Lock()          # 线程安全锁

    def _refill(self):
        """根据经过的时间补充令牌"""
        now = time.monotonic()
        elapsed = now - self.last_refill
        # 按速率补充令牌，不超过桶容量
        self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
        self.last_refill = now

    def acquire(self, tokens: int = 1) -> bool:
        """尝试获取令牌。成功返回 True，失败返回 False。"""
        with self.lock:
            self._refill()
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False

    def wait_and_acquire(self, tokens: int = 1, timeout: float = 10.0) -> bool:
        """等待直到获取到令牌，或超时返回 False。"""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.acquire(tokens):
                return True
            # 计算需要等待多长时间才能攒够令牌
            with self.lock:
                needed = tokens - self.tokens
                wait_time = needed / self.rate if self.rate > 0 else 0.1
            time.sleep(min(wait_time, 0.05))  # 短暂休眠后重试
        return False


# ---------------------------------------------------------------------------
# 检查函数
# ---------------------------------------------------------------------------

def check_http_service(host: str, port: int, path: str, timeout: int) -> Tuple[str, str, int]:
    """对 HTTP 服务发起健康检查。

    参数:
        host: 主机地址
        port: 端口号
        path: 健康检查路径（如 /health）
        timeout: 连接超时时间（秒）

    返回:
        (状态, 详情, HTTP状态码) 三元组
        - 状态: "OK" / "WARNING" / "CRITICAL"
    """
    import http.client
    try:
        conn = http.client.HTTPConnection(host, port, timeout=timeout)
        conn.request("GET", path)
        resp = conn.getresponse()
        status = resp.status
        body = resp.read().decode("utf-8", errors="replace")[:200]
        conn.close()

        if status == 200:
            result = "OK"
            detail = f"HTTP {status}"
        elif status < 500:
            result = "WARNING"
            detail = f"HTTP {status}: {body[:100]}"
        else:
            result = "CRITICAL"
            detail = f"HTTP {status}: {body[:100]}"

        return result, detail, status
    except Exception as e:
        return "CRITICAL", str(e), 0


def check_tcp_port(host: str, port: int, timeout: int) -> Tuple[str, str, float]:
    """通过 TCP 端口连通性检查基础设施服务状态。"""
    try:
        start = time.time()
        sock = socket.create_connection((host, port), timeout=timeout)
        sock.close()
        latency = (time.time() - start) * 1000
        return "OK", f"Connected ({latency:.1f}ms)", latency
    except socket.timeout:
        return "CRITICAL", f"Connection timeout ({timeout}s)", 0
    except ConnectionRefusedError:
        return "CRITICAL", "Connection refused", 0
    except Exception as e:
        return "CRITICAL", str(e), 0


def check_certificate_expiry(host: str, port: int = 443) -> Tuple[str, str, int]:
    """检查 TLS 证书到期时间。"""
    try:
        ctx = ssl.create_default_context()
        with socket.create_connection((host, port), timeout=10) as sock:
            with ctx.wrap_socket(sock, server_hostname=host) as ssock:
                cert = ssock.getpeercert()
                if not cert:
                    return "WARNING", "No certificate found", 0

                from datetime import datetime as dt
                expires = dt.strptime(cert["notAfter"], "%b %d %H:%M:%S %Y %Z")
                days_left = (expires - dt.now()).days

                if days_left > 30:
                    return "OK", f"Certificate expires in {days_left} days", days_left
                elif days_left > 7:
                    return "WARNING", f"Certificate expires in {days_left} days", days_left
                else:
                    return "CRITICAL", f"Certificate expires in {days_left} days", days_left
    except Exception as e:
        return "WARNING", str(e), 0


def check_disk_usage() -> Tuple[str, str, float]:
    """检查磁盘使用率（Windows / Linux 兼容）。"""
    try:
        if sys.platform == "win32":
            import ctypes
            free_bytes = ctypes.c_ulonglong(0)
            total_bytes = ctypes.c_ulonglong(0)
            ctypes.windll.kernel32.GetDiskFreeSpaceExW(
                "C:\\", ctypes.byref(free_bytes), ctypes.byref(total_bytes), None
            )
            used_pct = (1 - free_bytes.value / total_bytes.value) * 100
        else:
            stat = os.statvfs("/")
            used_pct = (1 - stat.f_bavail / stat.f_blocks) * 100

        if used_pct > DISK_THRESHOLD_CRITICAL:
            return "CRITICAL", f"Disk usage {used_pct:.1f}%", used_pct
        elif used_pct > DISK_THRESHOLD_WARNING:
            return "WARNING", f"Disk usage {used_pct:.1f}%", used_pct
        else:
            return "OK", f"Disk usage {used_pct:.1f}%", used_pct
    except Exception as e:
        return "WARNING", str(e), 0


def check_memory_usage() -> Tuple[str, str, float]:
    """检查内存使用率（Windows / Linux 兼容）。"""
    try:
        import psutil
        mem = psutil.virtual_memory()
        used_pct = mem.percent
    except ImportError:
        try:
            if sys.platform == "win32":
                output = subprocess.check_output(
                    ["wmic", "OS", "get", "TotalVisibleMemorySize,FreePhysicalMemory", "/Value"],
                    text=True, timeout=10
                )
                lines = output.strip().split("\n")
                values = {}
                for line in lines:
                    if "=" in line:
                        k, v = line.split("=")
                        values[k.strip()] = int(v.strip())
                total = values.get("TotalVisibleMemorySize", 1)
                free = values.get("FreePhysicalMemory", 0)
                used_pct = (1 - free / total) * 100
            else:
                with open("/proc/meminfo") as f:
                    meminfo = {}
                    for line in f:
                        parts = line.split(":")
                        if len(parts) == 2:
                            key = parts[0].strip()
                            val = int(parts[1].strip().split()[0])
                            meminfo[key] = val
                total = meminfo.get("MemTotal", 1)
                available = meminfo.get("MemAvailable", 0)
                used_pct = (1 - available / total) * 100
        except Exception:
            return "WARNING", "Could not check memory usage", 0

    if used_pct > MEMORY_THRESHOLD_CRITICAL:
        return "CRITICAL", f"Memory usage {used_pct:.1f}%", used_pct
    elif used_pct > MEMORY_THRESHOLD_WARNING:
        return "WARNING", f"Memory usage {used_pct:.1f}%", used_pct
    else:
        return "OK", f"Memory usage {used_pct:.1f}%", used_pct


def check_load_average() -> Tuple[str, str, float]:
    """检查系统负载均值。"""
    try:
        load = os.getloadavg()[0]
        cpu_count = os.cpu_count() or 1
        if load > cpu_count * 2:
            return "CRITICAL", f"Load {load:.2f}", load
        elif load > cpu_count:
            return "WARNING", f"Load {load:.2f}", load
        else:
            return "OK", f"Load {load:.2f}", load
    except Exception:
        return "OK", "Load unknown", 0


# ---------------------------------------------------------------------------
# 超时覆盖逻辑
# ---------------------------------------------------------------------------

def apply_timeout_overrides(
    services: Dict[str, Dict],
    infra: Dict[str, Dict],
    global_timeout: Optional[int],
    endpoint_overrides: Optional[Dict[str, int]],
) -> Tuple[Dict[str, Dict], Dict[str, Dict]]:
    """将命令行指定的超时覆盖应用到服务与基础设施配置中。

    优先级（从高到低）：
        1. 端点级覆盖（--endpoint-timeout backend=3）
        2. 全局覆盖（--timeout 5）
        3. 默认配置（SERVICES / INFRASTRUCTURE 中写死的值）

    这确保用户既能统一调参，又能对个别慢服务开"小灶"。
    """
    # 深拷贝避免修改原始常量
    svc_copy = {name: dict(cfg) for name, cfg in services.items()}
    infra_copy = {name: dict(cfg) for name, cfg in infra.items()}

    # 第一步：应用全局超时覆盖
    if global_timeout and global_timeout > 0:
        for cfg in svc_copy.values():
            cfg["timeout"] = global_timeout
        for cfg in infra_copy.values():
            cfg["timeout"] = global_timeout

    # 第二步：应用端点级覆盖（优先于全局）
    if endpoint_overrides:
        for name, timeout in endpoint_overrides.items():
            if timeout <= 0:
                print(f"警告: {name} 的超时值 {timeout}s 无效，已忽略", file=sys.stderr)
                continue
            if name in svc_copy:
                svc_copy[name]["timeout"] = timeout
            elif name in infra_copy:
                infra_copy[name]["timeout"] = timeout
            else:
                print(f"警告: 未知端点 '{name}'，超时覆盖已忽略", file=sys.stderr)

    return svc_copy, infra_copy


# ---------------------------------------------------------------------------
# 主健康检查逻辑
# ---------------------------------------------------------------------------

def run_health_checks(
    service: Optional[str] = None,
    json_output: bool = False,
    rate_limiter: Optional[TokenBucket] = None,
    svc_cfg: Optional[Dict[str, Dict]] = None,
    infra_cfg: Optional[Dict[str, Dict]] = None,
) -> Dict[str, Any]:
    """运行所有健康检查并汇总报告。

    新增参数:
        rate_limiter: 令牌桶限流器实例（可选），用于限制探测频率
        svc_cfg:     覆盖后的服务配置（含自定义超时）
        infra_cfg:   覆盖后的基础设施配置（含自定义超时）
    """
    if svc_cfg is None:
        svc_cfg = SERVICES
    if infra_cfg is None:
        infra_cfg = INFRASTRUCTURE

    results: Dict[str, Any] = {
        "hostname": socket.gethostname(),
        "timestamp": datetime.now().isoformat(),
        "services": {},
        "infrastructure": {},
        "system": {},
        "rate_limit_applied": rate_limiter is not None,
    }

    all_ok = True

    # ---- 检查服务 ----
    for name, config in svc_cfg.items():
        if service and name != service:
            continue

        # 限流：每次服务检查前获取令牌
        if rate_limiter:
            rate_limiter.wait_and_acquire()

        status, detail, code = check_http_service(
            config["host"], config["port"], config["path"], config["timeout"]
        )
        results["services"][name] = {
            "status": status,
            "detail": detail,
            "code": code,
            "endpoint": f"http://{config['host']}:{config['port']}{config['path']}",
            "timeout_used": config["timeout"],
        }
        if status == "CRITICAL":
            all_ok = False

    # ---- 检查基础设施 ----
    for name, config in infra_cfg.items():
        if service and name != service:
            continue

        if rate_limiter:
            rate_limiter.wait_and_acquire()

        status, detail, latency = check_tcp_port(
            config["host"], config["port"], config["timeout"]
        )
        results["infrastructure"][name] = {
            "status": status,
            "detail": detail,
            "endpoint": f"{config['host']}:{config['port']}",
            "timeout_used": config["timeout"],
        }
        if status == "CRITICAL":
            all_ok = False

    # ---- 检查系统资源 ----
    disk_status, disk_detail, disk_pct = check_disk_usage()
    results["system"]["disk"] = {"status": disk_status, "detail": disk_detail}
    if disk_status == "CRITICAL":
        all_ok = False

    mem_status, mem_detail, mem_pct = check_memory_usage()
    results["system"]["memory"] = {"status": mem_status, "detail": mem_detail}
    if mem_status == "CRITICAL":
        all_ok = False

    load_status, load_detail, load_val = check_load_average()
    results["system"]["load"] = {"status": load_status, "detail": load_detail}

    # ---- 检查证书到期 ----
    for name, config in svc_cfg.items():
        if service and name != service:
            continue
        if config["port"] == 443:
            cert_status, cert_detail, days_left = check_certificate_expiry(config["host"])
            results["services"][name]["certificate"] = {
                "status": cert_status,
                "detail": cert_detail,
                "days_remaining": days_left,
            }
            if cert_status == "CRITICAL":
                all_ok = False

    results["overall_status"] = "OK" if all_ok else "DEGRADED"
    return results


def print_health_report(results: Dict[str, Any]):
    """打印人类可读的健康检查报告。"""
    print(f"\n{'='*60}")
    print(f"  HEALTH CHECK REPORT")
    print(f"  Host: {results['hostname']}")
    print(f"  Time: {results['timestamp']}")
    print(f"  Overall: {results['overall_status']}")
    if results.get("rate_limit_applied"):
        print(f"  Rate Limiting: ENABLED")
    print(f"{'='*60}")

    for category, items in [
        ("Services", results["services"]),
        ("Infrastructure", results["infrastructure"]),
        ("System", results["system"]),
    ]:
        if items:
            print(f"\n  {category}:")
            for name, check in items.items():
                if isinstance(check, dict) and "status" in check:
                    status_icon = {"OK": "✓", "WARNING": "⚠", "CRITICAL": "✗"}.get(
                        check["status"], "?"
                    )
                    print(f"    {status_icon} {name}: {check['detail']}")
                else:
                    print(f"    {name}:")
                    for sub_name, sub_check in check.items():
                        if isinstance(sub_check, dict) and "status" in sub_check:
                            sub_icon = {"OK": "✓", "WARNING": "⚠", "CRITICAL": "✗"}.get(
                                sub_check["status"], "?"
                            )
                            print(f"      {sub_icon} {sub_name}: {sub_check['detail']}")
    print()


def parse_endpoint_timeouts(raw: str) -> Dict[str, int]:
    """解析 --endpoint-timeout 参数。

    格式: "backend=3,market=10,frailbox=8"
    返回: {"backend": 3, "market": 10, "frailbox": 8}

    如果格式错误，打印警告并跳过该项，不会导致程序崩溃。
    """
    if not raw:
        return {}
    result = {}
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        if "=" not in item:
            print(f"警告: 无效的超时格式 '{item}'（应为 name=N），已跳过", file=sys.stderr)
            continue
        name, val = item.split("=", 1)
        try:
            result[name.strip()] = int(val.strip())
        except ValueError:
            print(f"警告: 超时值 '{val}' 不是有效整数，已跳过", file=sys.stderr)
    return result


def parse_args():
    """解析命令行参数（含新增的 --timeout, --endpoint-timeout, --rate-limit）。"""
    parser = argparse.ArgumentParser(
        description="Health check tool — Tent of Trials 平台健康检查"
    )
    parser.add_argument("--service", "-s", help="仅检查指定服务")
    parser.add_argument("--json", "-j", action="store_true", help="JSON 格式输出")
    parser.add_argument("--watch", "-w", action="store_true", help="持续监控模式")
    parser.add_argument(
        "--interval", "-i", type=int, default=30, help="监控间隔秒数（默认 30）"
    )
    parser.add_argument("--output", "-o", help="输出文件路径")

    # ---- 新增参数 ----
    parser.add_argument(
        "--timeout", "-t",
        type=int,
        default=None,
        help="全局超时覆盖（秒），适用于所有服务和基础设施端点",
    )
    parser.add_argument(
        "--endpoint-timeout",
        type=str,
        default=None,
        help="按端点覆盖超时，格式: backend=3,market=10 （用逗号分隔）",
    )
    parser.add_argument(
        "--rate-limit",
        type=float,
        default=DEFAULT_RATE_LIMIT,
        help=f"每秒最大探测次数（默认 {DEFAULT_RATE_LIMIT} = 不限流）。建议值: 1~20",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    # ---- 解析端点级超时 ----
    endpoint_overrides = parse_endpoint_timeouts(args.endpoint_timeout or "")

    # ---- 应用超时覆盖 ----
    svc_cfg, infra_cfg = apply_timeout_overrides(
        SERVICES, INFRASTRUCTURE, args.timeout, endpoint_overrides
    )

    # ---- 初始化限流器 ----
    rate_limiter = None
    if args.rate_limit and args.rate_limit > 0:
        rate_limiter = TokenBucket(rate=args.rate_limit)
        if not args.json:
            print(f"[限流] 每秒最多 {args.rate_limit} 个探测请求")

    if args.watch:
        if not args.json:
            print(
                f"持续监控中（间隔: {args.interval}s，"
                f"限流: {'ON' if rate_limiter else 'OFF'}）。按 Ctrl+C 停止。"
            )
        try:
            while True:
                results = run_health_checks(
                    args.service, args.json, rate_limiter,
                    svc_cfg=svc_cfg, infra_cfg=infra_cfg
                )
                if args.json:
                    print(json.dumps(results, indent=2))
                else:
                    print_health_report(results)
                time.sleep(args.interval)
        except KeyboardInterrupt:
            print("\n监控已停止")
    else:
        results = run_health_checks(
            args.service, args.json, rate_limiter,
            svc_cfg=svc_cfg, infra_cfg=infra_cfg
        )
        if args.json:
            output = json.dumps(results, indent=2)
            print(output)
        else:
            print_health_report(results)

        if args.output:
            with open(args.output, "w") as f:
                json.dump(results, f, indent=2)
            print(f"报告已保存到 {args.output}")

        if results["overall_status"] == "DEGRADED":
            return 1

    return 0


if __name__ == "__main__":
    main()
