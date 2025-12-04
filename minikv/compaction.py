"""
Compaction logic for the storage engine.

This module currently provides a simple full compaction routine compact_all():
  - Flushes the active MemTable so that all in-memory updates become SSTs.
  - Merges all existing SSTs into a single consolidated snapshot.
  - Removes tombstone-marked keys during the merge.
  - Deletes all old SST segments and generates a single compacted SST.
  - Performs a WAL checkpoint (truncate + reopen), because the snapshot now
    represents the complete engine state.

Future extensions may include:
  - Leveled or tiered compaction strategies
  - More advanced compaction triggers (size-based, overlap-based)
  - Background compaction threads with scheduling and rate limiting
  - Snapshot and sequence-number semantics
"""

from __future__ import annotations

from typing import Any
import os

from .sst import SSTFile


def compact_all(kv: Any, tombstone: str) -> None:
    """
    Executes a full compaction over the entire key-value engine.

    The kv object is expected to expose:
      - kv._ensure_open()
      - kv._flush_to_sst()
      - kv._next_sst_path()
      - kv.memtable      : dict[str, str]
      - kv.sst_files     : list[SSTFile]
      - kv.wal           : WAL or None
      - kv.config        : MiniKVConfig (optional)

    Compaction workflow:
      1) Flushes the current MemTable so all pending writes are persisted.
      2) Scans SSTs from newest to oldest and reconstructs the latest key view:
         - Regular key-value pairs are retained.
         - Tombstones indicate deletion and suppress any older values.
      3) Removes all existing SST files.
      4) Emits a new SST containing the merged snapshot.
      5) Resets the WAL to begin logging from the new snapshot boundary.
    """

    kv._ensure_open()

    # 1. Flush pending MemTable updates into SSTs.
    kv._flush_to_sst()

    if not kv.sst_files:
        # No SSTs exist; the flush step already captured all state.
        return

    # 2. Merge SSTs from newest to oldest.
    merged: dict[str, str] = {}
    deleted: set[str] = set()

    for sst in reversed(kv.sst_files):
        if not os.path.exists(sst.path):
            continue

        with open(sst.path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.rstrip("\n")
                if not line:
                    continue

                parts = line.split("\t", 1)
                if len(parts) != 2:
                    continue
                k, v = parts

                # Keys resolved by newer SSTs are skipped (whether preserved or deleted).
                if k in merged or k in deleted:
                    continue

                if v == tombstone:
                    deleted.add(k)
                else:
                    merged[k] = v

    # 3. Remove obsolete SST files and clear the in-memory list.
    for sst in kv.sst_files:
        try:
            os.remove(sst.path)
        except FileNotFoundError:
            pass
    kv.sst_files = []

    # If the merged result contains no keys, no SST output is needed.
    if not merged:
        _checkpoint_wal(kv)
        return

    # 4. Emit a new compacted SST snapshot.
    path = kv._next_sst_path()
    new_sst = SSTFile.write_from_memtable(path=path, memtable=merged)
    kv.sst_files.append(new_sst)

    # 5. The WAL can now be safely truncated, since the snapshot
    #    reflects the complete engine state.
    _checkpoint_wal(kv)


def _checkpoint_wal(kv: Any) -> None:
    """
    Truncates the WAL and reinitializes it as an empty log whose
    contents begin immediately after the latest snapshot.

    This helper assumes the engine's full state has already been
    materialized into SST files by compact_all().
    """

    wal = getattr(kv, "wal", None)
    if wal is None:
        return

    if not hasattr(wal, "path"):
        return

    # 1. Close the existing WAL to ensure all buffered data is persisted.
    wal.close()

    wal_path = wal.path

    # 2. Truncate the WAL file to zero length.
    os.makedirs(os.path.dirname(wal_path), exist_ok=True)
    open(wal_path, "w", encoding="utf-8").close()

    # 3. Reopen a fresh WAL instance using the same path.
    from .wal import WAL  # Deferred import to avoid circular dependencies

    kv.wal = WAL(wal_path)
    if hasattr(kv.wal, "open"):
        kv.wal.open()

# ================= Skeleton for Future Extensions =================

class CompactionPlan:
    """
    Placeholder structure representing a plan for a future partial compaction.

    A compaction plan may eventually include:
      - The set of SST files (or levels) participating in the merge
      - The target output level
      - Estimated I/O cost or write-amplification budget

    Fields such as level ranges, input file lists, and output key intervals
    can be added when leveled or tiered compaction is introduced.
    """
    # Example future fields:
    # level_from: int
    # level_to: int
    # input_files: list[SSTFile]
    # output_ranges: list[tuple[str, str]]


def pick_leveled_compaction_plan(kv: Any) -> CompactionPlan | None:
    """
    Selects a partial compaction plan for leveled compaction (future feature).

    Intended design:
      - SST files are organized into levels following a LevelDB/RocksDB style layout.
      - Level-0 triggers might be based on file count or size thresholds.
      - Level >=1 triggers may consider level size limits or excessive overlap
        with adjacent levels' key ranges.

    Current version:
      - Returns None, indicating that no leveled compaction is scheduled.
    """
    return None


def run_leveled_compaction(kv: Any, plan: CompactionPlan) -> None:
    """
    Executes a partial compaction according to the given plan.

    This routine would merge the SST files specified in the plan and
    generate new SST outputs based on the defined target level or ranges.

    Current version:
      - Not implemented.
    """
    raise NotImplementedError("Leveled compaction is not implemented yet.")
