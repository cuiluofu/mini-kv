"""
test_restart_delete.py

Test objectives:
  1) First run:
       - Perform a sequence of puts and deletes that triggers MemTable flushes
         and produces multiple SST files.
       - Ensure at least one live key and one deleted key whose tombstone
         is flushed into SSTs.
       - Close the engine to persist WAL and SST state.
  2) Second run:
       - Re-open the engine using the same data directory.
       - Verify that:
           * Live keys can still be read correctly.
           * Deleted keys return None through the public get() API.
           * Tombstone records truly reside in SST files (not only in WAL).
       - Run a full compaction and verify:
           * Logical behavior (get) remains unchanged.
           * The deleted key is physically removed from compacted SSTs.
           * WAL is checkpointed and shrinks on disk.

Usage:
  Phase 1 only (write and close):
      python test_restart_delete.py 1

  Phase 2 only (reopen and verify):
      python test_restart_delete.py 2

  Run both phases in a single process (not a real restart but convenient):
      python test_restart_delete.py both
"""

import os
import shutil
import sys

from minikv.config import MiniKVConfig, WriteMode
from minikv.engine import MiniKV, TOMBSTONE


DATA_DIR = "data_restart_test"


def make_config() -> MiniKVConfig:
    """
    Constructs a configuration that uses a dedicated data directory and
    a small MemTable limit, so that flushes are triggered with only a few writes.
    """
    return MiniKVConfig(
        data_dir=DATA_DIR,
        write_mode=WriteMode.SYNC,  # Deterministic: fsync on each write
        memtable_limit=2,           # Flush after 2 entries to create multiple SSTs
    )


def phase1_first_run() -> None:
    """
    Phase 1: initial run.

    Steps:
      - Remove any existing data directory for a clean environment.
      - Open the engine.
      - Execute a sequence of put/delete operations that:
          * Produces a set of SST files.
          * Ensures that at least one key is flushed as a tombstone.
      - Close the engine to persist state.
    """
    # Start from a clean state.
    shutil.rmtree(DATA_DIR, ignore_errors=True)

    config = make_config()
    kv = MiniKV(config)
    kv.open()

    print("=== [Phase 1] Start ===")

    # With memtable_limit=2, these operations cause multiple flushes:
    # First flush:  contains k_alive_1 and k_delete (with a non-tombstone value).
    kv.put("k_alive_1", "v1")
    kv.put("k_delete", "temp_value")

    # Second flush: contains k_alive_2 and k_delete (as TOMBSTONE).
    kv.put("k_alive_2", "v2")
    kv.delete("k_delete")

    # Third flush: produced at close() for k_alive_3.
    kv.put("k_alive_3", "v3")

    print(f"memtable before close: {kv.memtable}")
    kv.close()

    # Inspect data directory contents after phase 1.
    print("Files in data dir after phase1:")
    for name in sorted(os.listdir(DATA_DIR)):
        print("  -", name)

    print("=== [Phase 1] Done ===")


def phase2_second_run() -> None:
    """
    Phase 2: restart and verification.

    Steps:
      - Reconstruct a MiniKV instance using the same data directory.
      - Open the engine, which will:
          * Lazily load SST metadata.
          * Open the WAL.
          * Replay WAL into the MemTable.
      - Verify via get():
          * Live keys return the expected values.
          * Deleted keys return None.
      - Directly inspect SST contents to:
          * Confirm that the deleted key appears as a tombstone in the newest SST.
      - Trigger a full compaction and:
          * Verify that the logical view via get() is unchanged.
          * Confirm that the deleted key is physically absent from the compacted SSTs.
          * Check that WAL is checkpointed (size does not increase and usually shrinks).
    """
    config = make_config()
    kv = MiniKV(config)
    kv.open()

    print("=== [Phase 2] Start ===")
    print(f"Loaded {len(kv.sst_files)} SST files:")
    for sst in kv.sst_files:
        print("  -", sst.path)

    # 1. Verify logical view through the public get() API.
    alive_expect = {
        "k_alive_1": "v1",
        "k_alive_2": "v2",
        "k_alive_3": "v3",
    }
    deleted_keys = ["k_delete"]

    print("\n[Check] get() on alive keys (before compaction):")
    for k, expected in alive_expect.items():
        v = kv.get(k)
        print(f"  get({k!r}) = {v!r}")
        assert v == expected, f"Key {k} expected {expected!r}, got {v!r}"

    print("\n[Check] get() on deleted keys (before compaction):")
    for k in deleted_keys:
        v = kv.get(k)
        print(f"  get({k!r}) = {v!r}")
        assert v is None, f"Deleted key {k} should return None, got {v!r}"

    # 2. Directly inspect SST files to confirm tombstone materialization:
    #    - The newest SST should contain k_delete as TOMBSTONE.
    #    - Live keys should be present with correct values in some SST.
    print("\n[Check] SST contents via SSTFile.search() (before compaction):")

    sst_tombstone = None
    for sst in reversed(kv.sst_files):  # Newest to oldest
        v = sst.search("k_delete")
        if v is not None:
            sst_tombstone = (sst.path, v)
            break

    print(f"  SST search('k_delete') -> {sst_tombstone}")
    assert sst_tombstone is not None, "k_delete should appear in some SST"
    assert sst_tombstone[1] == TOMBSTONE, (
        f"k_delete should be TOMBSTONE in newest SST, got {sst_tombstone[1]!r}"
    )

    wal_path = os.path.join(DATA_DIR, "wal.log")
    wal_size_before = os.path.getsize(wal_path)
    print(f"\n[Checkpoint] wal.log size before compaction: {wal_size_before} bytes")

    # 3. Trigger a full compaction and inspect the resulting SST layout.
    print("\n[Compaction] Run compact_all() ...")
    old_sst_paths = [s.path for s in kv.sst_files]
    old_count = len(old_sst_paths)
    kv.compact_all()
    new_count = len(kv.sst_files)
    new_paths = [s.path for s in kv.sst_files]

    print(f" SST file count: {old_count} -> {new_count}")
    print("Old SST files:")
    for p in old_sst_paths:
        print("     -", p)
    print(" New SST files:")
    for p in new_paths:
        print("     -", p)

    assert new_count <= old_count, "SST file count should not increase after compaction"
    assert new_count >= 0

    wal_size_after = os.path.getsize(wal_path)
    print(f"\n[Checkpoint] wal.log size after compaction: {wal_size_after} bytes")
    # WAL is expected to shrink (often to zero) after checkpoint.
    assert wal_size_after <= wal_size_before, "wal.log should not grow after checkpoint"
    # For stricter expectations, this could be:
    # assert wal_size_after == 0

    # 4. Verify that logical behavior remains correct after compaction.

    print("\n[Check] get() on alive keys (after compaction):")
    for k, expected in alive_expect.items():
        v = kv.get(k)
        print(f"  get({k!r}) = {v!r}")
        assert v == expected, (
            f"(after compaction) Key {k} expected {expected!r}, got {v!r}"
        )

    print("\n[Check] get() on deleted keys (after compaction):")
    for k in deleted_keys:
        v = kv.get(k)
        print(f"  get({k!r}) = {v!r}")
        assert v is None, (
            f"(after compaction) Deleted key {k} should return None, got {v!r}"
        )

    # 4.2 Physical verification: compacted SSTs must not contain k_delete.
    print("\n[Check] SST files physically (after compaction):")
    for sst in kv.sst_files:
        print(f"  Inspect {sst.path}:")
        with open(sst.path, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                k, v = line.rstrip("\n").split("\t", 1)
                assert k != "k_delete", (
                    "Deleted key k_delete should not appear in compacted SST"
                )

    print("\nAll checks passed ✅")
    print("=== [Phase 2] Done ===")


def main() -> None:
    """
    Entry point for the restart + delete + compaction test.

    Command-line arguments:
      1      Run phase 1 only (initial write and close).
      2      Run phase 2 only (restart, verify, compact).
      both   Run both phases sequentially in a single process.
    """
    arg = sys.argv[1] if len(sys.argv) >= 2 else "both"
    if arg == "1":
        phase1_first_run()
    elif arg == "2":
        phase2_second_run()
    elif arg == "both":
        phase1_first_run()
        phase2_second_run()
    else:
        print("Usage: python test_restart_delete.py [1|2|both]")


if __name__ == "__main__":
    main()
