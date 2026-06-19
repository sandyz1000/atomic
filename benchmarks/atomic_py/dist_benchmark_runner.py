"""Runner that starts/stops atomic-worker processes, writes ~/hosts.conf, and
drives the distributed benchmarks.

Usage
-----
    # Run Pi benchmark (3 runs)
    benchmarks/.venv/bin/python benchmarks/atomic_py/dist_benchmark_runner.py pi

    # Run WordCount benchmark (3 runs)
    benchmarks/.venv/bin/python benchmarks/atomic_py/dist_benchmark_runner.py wordcount

    # Run all
    benchmarks/.venv/bin/python benchmarks/atomic_py/dist_benchmark_runner.py all

Worker count
------------
NUM_WORKERS below controls parallelism.  Default is 4 — raise to 12 to match
PySpark local[12], but each worker binary initialises a Python interpreter so
12 workers requires ~600 MB extra RAM.

The binary is expected at:
    <repo-root>/target/release/atomic-worker

If missing, run:  cargo build --release -p atomic-worker
"""

import os
import pathlib
import signal
import subprocess
import sys
import time
import tomli_w

NUM_WORKERS = 4
BASE_PORT = 10001
LOCAL_IP = "127.0.0.1"
REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
WORKER_BIN = REPO_ROOT / "target" / "release" / "atomic-worker"
HOSTS_CONF = pathlib.Path.home() / "hosts.conf"
BENCHMARKS_DIR = pathlib.Path(__file__).parent

VENV_PYTHON = REPO_ROOT / "benchmarks" / ".venv" / "bin" / "python"
# Site-packages path so atomic-worker (which uses auto-initialized PyO3) can find
# cloudpickle and other benchmark dependencies.
VENV_SITE_PACKAGES = REPO_ROOT / "benchmarks" / ".venv" / "lib" / "python3.14" / "site-packages"


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _build_hosts_conf(n: int) -> dict:
    slaves = [f"worker@{LOCAL_IP}:{BASE_PORT + i}" for i in range(n)]
    return {"master": f"{LOCAL_IP}:9000", "slaves": slaves}


def start_workers(n: int) -> list[subprocess.Popen]:
    """Start n atomic-worker processes on sequential ports and wait for them to bind."""
    if not WORKER_BIN.exists():
        sys.exit(
            f"ERROR: {WORKER_BIN} not found.\n"
            "Run:  cargo build --release -p atomic-worker"
        )

    procs = []
    for i in range(n):
        port = BASE_PORT + i
        # Prepend venv site-packages so PyO3 auto-initialize finds cloudpickle.
        pythonpath = str(VENV_SITE_PACKAGES)
        existing = os.environ.get("PYTHONPATH", "")
        if existing:
            pythonpath = f"{pythonpath}:{existing}"
        env = {**os.environ, "RUST_LOG": "warn", "PYTHONPATH": pythonpath}
        p = subprocess.Popen(
            [str(WORKER_BIN), "--port", str(port), "--local-ip", LOCAL_IP],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        procs.append(p)
    # Give workers a moment to bind their ports.
    time.sleep(2)
    last_port = BASE_PORT + n - 1
    print(f"  started {n} workers on ports {BASE_PORT}–{last_port}", flush=True)
    return procs


def stop_workers(procs: list[subprocess.Popen]) -> None:
    """Send SIGTERM to all workers and wait for them to exit."""
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
    print("  workers stopped", flush=True)


def write_hosts_conf(n: int) -> dict | None:
    """Write ~/hosts.conf and return the previous content (or None)."""
    previous = None
    if HOSTS_CONF.exists():
        with open(HOSTS_CONF, "rb") as f:
            previous = f.read()
    conf = _build_hosts_conf(n)
    with open(HOSTS_CONF, "wb") as f:
        tomli_w.dump(conf, f)
    return previous


def restore_hosts_conf(previous: bytes | None) -> None:
    """Restore ~/hosts.conf to its previous state (or remove it if it didn't exist)."""
    if previous is None:
        HOSTS_CONF.unlink(missing_ok=True)
    else:
        HOSTS_CONF.write_bytes(previous)


def run_benchmark(script: str, runs: int = 3) -> None:
    """Run a benchmark script the given number of times and print RESULT lines."""
    env = {
        **os.environ,
        "ATOMIC_DEPLOYMENT_MODE": "distributed",
        "ATOMIC_LOCAL_IP": LOCAL_IP,
    }
    for i in range(runs):
        result = subprocess.run(
            [str(VENV_PYTHON), str(BENCHMARKS_DIR / script)],
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            print(f"  run {i + 1} FAILED:\n{result.stderr}", flush=True)
        else:
            for line in result.stdout.splitlines():
                if line.startswith("RESULT"):
                    print(f"  run {i + 1}: {line}", flush=True)


# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------

BENCHMARKS = {
    "pi": "dist_pi_benchmark.py",
    "wordcount": "dist_wordcount_benchmark.py",
}


def main() -> None:
    """Parse the workload argument, start workers, run benchmarks, clean up."""
    target = sys.argv[1] if len(sys.argv) > 1 else "all"
    to_run = list(BENCHMARKS.items()) if target == "all" else [(target, BENCHMARKS[target])]

    print(f"Starting {NUM_WORKERS} workers…", flush=True)
    procs = start_workers(NUM_WORKERS)
    previous_conf = write_hosts_conf(NUM_WORKERS)

    try:
        for name, script in to_run:
            print(f"\n--- {name} (distributed, {NUM_WORKERS} workers) ---", flush=True)
            run_benchmark(script)
    finally:
        stop_workers(procs)
        restore_hosts_conf(previous_conf)


if __name__ == "__main__":
    main()
