"""
bench_write.py

Micro-benchmark for comparing different WAL write policies
(WriteMode.SYNC / BATCH / ADAPTIVE) in MiniKV.

For each mode, the script:
  - Issues N write operations (default: 50,000).
  - Reports:
      * Total elapsed time (seconds)
      * Throughput (QPS, ops/s)
      * Number of fsync calls (as recorded by MiniKV._sync_wal_now)

Example usage:
    python bench_write.py              # uses default N = 50_000
    python bench_write.py 100000       # runs with 100_000 operations

"""

import os
import shutil
import sys
import time

from minikv.config import MiniKVConfig, WriteMode
from minikv.engine import MiniKV


def run_bench(mode: WriteMode, n_ops: int) -> dict:
    """
    Runs a write-only benchmark for a given WriteMode.

    Returns a result dictionary containing:
      - mode:        WriteMode name
      - n_ops:       number of operations
      - elapsed:     total elapsed time in seconds
      - qps:         achieved throughput in ops/s
      - fsyncs:      number of WAL fsync invocations
    """
    data_dir = f"bench_data_{mode.name.lower()}"

    # Use a dedicated data directory per mode; remove stale data beforehand
    # to avoid interference from previous runs.
    shutil.rmtree(data_dir, ignore_errors=True)

    # To focus on WAL behavior, set memtable_limit large enough so that
    # SST flushes do not dominate the workload. Most writes remain in-memory
    # and are flushed only at close().
    config = MiniKVConfig(
        data_dir=data_dir,
        write_mode=mode,
        batch_size=100,          # Base batch size for BATCH/ADAPTIVE modes
        batch_interval_ms=5,     # Maximum interval between fsyncs
        memtable_limit=n_ops + 1,  # Prevent mid-run flushes; flush happens at close()
    )

    kv = MiniKV(config)
    kv.open()

    print(f"\n=== Benchmark {mode.name} ===")
    print(f"Data dir: {data_dir}")
    print(f"Total ops: {n_ops}")

    start = time.time()

    # Issue a sequence of put operations.
    # A fixed-size value is used to minimize variation in serialization cost.
    value = "x" * 32
    for i in range(n_ops):
        key = f"key_{i}"
        kv.put(key, value)

    # Close the engine, which will trigger any pending flush and final sync.
    kv.close()
    end = time.time()

    elapsed = end - start
    qps = n_ops / elapsed if elapsed > 0 else 0.0

    # fsync_count reflects the number of times MiniKV._sync_wal_now
    # has invoked wal.sync(), excluding the final safety fsync in WAL.close().
    fsync_count = kv.fsync_count

    print(f"Elapsed: {elapsed:.4f} s")
    print(f"QPS:     {qps:,.0f} ops/s")
    print(f"fsyncs:  {fsync_count}")

    return {
        "mode": mode.name,
        "n_ops": n_ops,
        "elapsed": elapsed,
        "qps": qps,
        "fsyncs": fsync_count,
    }


def print_summary(results: list[dict]) -> None:
    """
    Prints a compact summary table for easy inclusion in documentation.
    """
    print("\n=== Summary ===")
    header = f"{'Mode':<10} {'Ops':>10} {'Elapsed(s)':>12} {'QPS':>12} {'fsyncs':>10}"
    print(header)
    print("-" * len(header))

    for r in results:
        print(
            f"{r['mode']:<10} "
            f"{r['n_ops']:>10} "
            f"{r['elapsed']:>12.4f} "
            f"{r['qps']:>12.0f} "
            f"{r['fsyncs']:>10}"
        )


def main() -> None:
    """
    Entry point for the benchmark script.

    Accepts an optional command-line argument specifying the number
    of operations to run; defaults to 50,000 if omitted.
    """
    if len(sys.argv) >= 2:
        try:
            n_ops = int(sys.argv[1])
        except ValueError:
            print("Usage: python bench_write.py [num_ops]")
            return
    else:
        n_ops = 50_000

    results: list[dict] = []
    for mode in (WriteMode.SYNC, WriteMode.BATCH, WriteMode.ADAPTIVE):
        res = run_bench(mode, n_ops)
        results.append(res)

    print_summary(results)


if __name__ == "__main__":
    main()
