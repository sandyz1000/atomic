"""Peak-RSS measurement for all benchmark engines.

Monitors the full process tree (driver + all children/workers) at 100 ms intervals
during each job and reports peak RSS in MB.

Usage
-----
    benchmarks/.venv/bin/python benchmarks/measure_memory.py

Requires
--------
    pip install psutil
    cargo build --release        # Rust binaries
    cargo build --release -p atomic-worker
    maturin develop --release    # atomic-py (in crates/atomic-py)
    pip install pyspark cloudpickle  # (already in benchmark venv)
"""

import os
import pathlib
import signal
import subprocess
import threading
import time

import psutil
import tomli_w

REPO = pathlib.Path(__file__).resolve().parent.parent
BENCH = pathlib.Path(__file__).parent
VENV = BENCH / ".venv" / "bin" / "python"
WORKER_BIN = REPO / "target" / "release" / "atomic-worker"
HOSTS_CONF = pathlib.Path.home() / "hosts.conf"

NUM_WORKERS = 4
BASE_PORT = 10001
LOCAL_IP = "127.0.0.1"
VENV_SITE = BENCH / ".venv" / "lib" / "python3.14" / "site-packages"

SAMPLE_MS = 100   # sample every 100 ms


# ---------------------------------------------------------------------------
# process-tree RSS sampler
# ---------------------------------------------------------------------------

def measure_peak_rss(
    cmd: list[str],
    env: dict | None = None,
    extra_pids: list[int] | None = None,
) -> tuple[str, int, int]:
    """Run *cmd* and return (stdout, returncode, peak_rss_bytes).

    Monitors the running process and all its children (plus any *extra_pids*)
    every SAMPLE_MS milliseconds and returns the maximum observed total RSS.
    """
    proc = subprocess.Popen(
        cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    peak = [0]

    def _sample() -> None:
        while proc.poll() is None:
            try:
                main_ps = psutil.Process(proc.pid)
                procs = [main_ps] + main_ps.children(recursive=True)
                if extra_pids:
                    for pid in extra_pids:
                        try:
                            procs.append(psutil.Process(pid))
                        except psutil.NoSuchProcess:
                            pass
                rss = sum(
                    p.memory_info().rss
                    for p in procs
                    if p.is_running()
                )
                if rss > peak[0]:
                    peak[0] = rss
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
            time.sleep(SAMPLE_MS / 1000)

    t = threading.Thread(target=_sample, daemon=True)
    t.start()
    stdout, _ = proc.communicate()
    t.join(1)
    return stdout, proc.returncode, peak[0]


def mb(b: int) -> float:
    return b / (1024 * 1024)


# ---------------------------------------------------------------------------
# worker management (for distributed measurements)
# ---------------------------------------------------------------------------

def start_workers(n: int) -> list[subprocess.Popen]:
    """Start n atomic-worker processes and return their Popen handles."""
    pythonpath = str(VENV_SITE)
    existing = os.environ.get("PYTHONPATH", "")
    if existing:
        pythonpath = f"{pythonpath}:{existing}"
    env = {**os.environ, "RUST_LOG": "warn", "PYTHONPATH": pythonpath}
    procs = []
    for i in range(n):
        port = BASE_PORT + i
        p = subprocess.Popen(
            [str(WORKER_BIN), "--port", str(port), "--local-ip", LOCAL_IP],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        procs.append(p)
    time.sleep(2)
    return procs


def stop_workers(procs: list[subprocess.Popen]) -> None:
    for p in procs:
        try:
            p.send_signal(signal.SIGTERM)
        except ProcessLookupError:
            pass
    for p in procs:
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()


def write_hosts_conf(n: int) -> bytes | None:
    previous = HOSTS_CONF.read_bytes() if HOSTS_CONF.exists() else None
    slaves = [f"worker@{LOCAL_IP}:{BASE_PORT + i}" for i in range(n)]
    with open(HOSTS_CONF, "wb") as f:
        tomli_w.dump({"master": f"{LOCAL_IP}:9000", "slaves": slaves}, f)
    return previous


def restore_hosts_conf(previous: bytes | None) -> None:
    if previous is None:
        HOSTS_CONF.unlink(missing_ok=True)
    else:
        HOSTS_CONF.write_bytes(previous)


# ---------------------------------------------------------------------------
# measurement runs
# ---------------------------------------------------------------------------

def measure_rust(workload: str) -> None:
    """Atomic Rust benchmark binary — one binary per workload."""
    binary = REPO / "target" / "release" / f"bench_{workload}"
    print(f"Rust: {workload}")
    if not binary.exists():
        print(f"  SKIP: {binary.name} not found")
        return
    _, rc, peak = measure_peak_rss([str(binary)])
    if rc != 0:
        print(f"  FAILED (rc={rc})")
        return
    print(f"  peak RSS: {mb(peak):.0f} MB")


def _print_result_lines(out: str) -> None:
    for line in out.splitlines():
        if line.startswith("RESULT"):
            print(f"  {line}")


def measure_spark(workload: str, script: str) -> None:
    """PySpark benchmark — includes JVM heap + Python worker processes."""
    print(f"Spark: {workload}")
    out, rc, peak = measure_peak_rss([str(VENV), str(BENCH / "spark" / script)])
    if rc != 0:
        print(f"  FAILED (rc={rc})")
        return
    _print_result_lines(out)
    print(f"  peak RSS: {mb(peak):.0f} MB")


def measure_atomic_py_local(workload: str, script: str) -> None:
    """atomic-py local mode — single driver Python process + ProcessPoolExecutor workers."""
    print(f"atomic-py local: {workload}")
    out, rc, peak = measure_peak_rss([str(VENV), str(BENCH / "atomic_py" / script)])
    if rc != 0:
        print(f"  FAILED (rc={rc})")
        return
    _print_result_lines(out)
    print(f"  peak RSS: {mb(peak):.0f} MB")


def measure_atomic_py_dist(workload: str, script: str) -> None:
    """atomic-py distributed — driver + 4 worker processes monitored together."""
    print(f"atomic-py distributed ({NUM_WORKERS} workers): {workload}")
    workers = start_workers(NUM_WORKERS)
    worker_pids = [w.pid for w in workers]
    prev = write_hosts_conf(NUM_WORKERS)
    try:
        env = {
            **os.environ,
            "ATOMIC_DEPLOYMENT_MODE": "distributed",
            "ATOMIC_LOCAL_IP": LOCAL_IP,
        }
        out, rc, peak = measure_peak_rss(
            [str(VENV), str(BENCH / "atomic_py" / script)],
            env=env,
            extra_pids=worker_pids,
        )
        if rc != 0:
            print(f"  FAILED (rc={rc})")
            return
        _print_result_lines(out)
        # Also include steady-state worker RSS before job starts
        worker_idle_rss = 0
        for pid in worker_pids:
            try:
                worker_idle_rss += psutil.Process(pid).memory_info().rss
            except psutil.NoSuchProcess:
                pass
        print(f"  peak RSS (driver + {NUM_WORKERS} workers): {mb(peak):.0f} MB")
        print(f"  workers idle RSS: {mb(worker_idle_rss):.0f} MB")
    finally:
        stop_workers(workers)
        restore_hosts_conf(prev)


# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------

WORKLOADS = [
    # (label, script_name)
    ("pi", "pi_benchmark.py"),
    ("wordcount", "wordcount_benchmark.py"),
    ("sort", "sort_benchmark.py"),
    ("join", "join_benchmark.py"),
]


def main() -> None:
    """Run one measurement pass for every engine × workload combination."""
    print("=" * 60)
    print("MEMORY MEASUREMENT — peak RSS per engine")
    print(f"Sampling interval: {SAMPLE_MS} ms")
    print("=" * 60)

    # ---- Rust binaries ---------------------------------------------------
    print()
    for name, _ in WORKLOADS:
        measure_rust(name)

    # ---- PySpark ---------------------------------------------------------
    print()
    for name, script in WORKLOADS:
        measure_spark(name, script)

    # ---- atomic-py local -------------------------------------------------
    print()
    for name, script in WORKLOADS:
        measure_atomic_py_local(name, script)

    # ---- atomic-py distributed -------------------------------------------
    print()
    measure_atomic_py_dist("pi", "dist_pi_benchmark.py")
    print()
    measure_atomic_py_dist("wordcount", "dist_wordcount_benchmark.py")


if __name__ == "__main__":
    main()
