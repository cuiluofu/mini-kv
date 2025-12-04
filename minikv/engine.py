import time
from typing import Optional
from .config import MiniKVConfig, WriteMode
from .wal import WAL
from .sst import SSTFile
from . import compaction

import os

TOMBSTONE = "__MINIKV_TOMBSTONE__"


class MiniKV:
    """
    Core MiniKV storage engine.

    Exposes a simple key-value interface (put/get/delete) and internally
    relies on WAL + MemTable + SST files + compaction to provide durability
    and crash recovery.
    """

    def __init__(self, config: MiniKVConfig):
        """
        Constructs a MiniKV instance without opening any files.

        The actual I/O resources (WAL, SST metadata, etc.) are initialized
        lazily when open() is invoked.
        """
        self.config = config
        self.memtable = {}
        self.sst_files = []                 # In-memory list of SSTFile descriptors
        self.wal: Optional[WAL] = None      # WAL instance, initialized on open()
        self._is_open = False
        self._pending_wal_ops = 0
        self._last_sync_time = 0.0
        self._adaptive_batch_size = config.batch_size
        self._fsync_count = 0               # Number of times wal.sync() has been invoked

    def open(self) -> None:
        """
        Opens the engine and reconstructs in-memory state.

        Steps:
          - Marks the engine as open.
          - Initializes an empty MemTable.
          - Loads existing SST file descriptors (lazy metadata only).
          - Opens or creates the WAL file.
          - Replays the WAL into the MemTable.
        """
        if self._is_open:
            return

        # 1. Mark engine as open.
        self._is_open = True

        # 2. Initialize an empty MemTable.
        self.memtable = {}

        # Load existing SST descriptors without scanning contents.
        self._load_sst_files()

        # 3. Create the WAL instance and open the log file.
        wal_path = self._build_wal_path()
        self.wal = WAL(wal_path)
        self.wal.open()

        # 4. Replay WAL records to reconstruct the MemTable.
        self._replay_wal()

    def close(self) -> None:
        """
        Closes the engine and releases associated resources.

        Current behavior:
          - Flushes the remaining MemTable to SST.
          - Flushes and closes the WAL if present.
          - Marks the engine as closed.
        """
        if not self._is_open:
            return

        # Flush remaining in-memory state to SST.
        self._flush_to_sst()

        if self.wal is not None:
            # Ensure WAL contents are durably persisted.
            self._sync_wal_now()
            self.wal.close()
            self.wal = None

        self._is_open = False

    def put(self, key: str, value: str) -> None:
        """
        Inserts or updates a key-value pair.

        Write path:
          - Appends a PUT record to the WAL (fsync policy depends on write_mode).
          - Updates the in-memory MemTable.
          - Optionally triggers a MemTable flush when the size threshold is reached.
        """
        self._ensure_open()

        # 1. Append to WAL.
        self._ensure_wal_initialized()
        self.wal.append_put(key, value)

        # 2. Update WAL-related accounting and apply the chosen sync policy.
        self._pending_wal_ops += 1
        self._after_wal_append()

        # 3. Apply the update to the MemTable.
        self.memtable[key] = value
        self._maybe_flush_memtable()

    def get(self, key: str) -> Optional[str]:
        """
        Retrieves the value associated with the given key.

        Lookup order:
          - First consults the MemTable.
          - Then searches SST files from newest to oldest.

        Behavior:
          - Returns the latest non-tombstone value if present.
          - Returns None if the key is not found or is logically deleted.
        """
        self._ensure_open()

        # 1. Check the MemTable.
        val = self.memtable.get(key)
        if val is not None:
            if val == TOMBSTONE:
                return None
            return val

        # 2. Scan SST files from newest to oldest.
        for sst in reversed(self.sst_files):
            val = sst.search(key)
            if val is None:
                continue
            if val == TOMBSTONE:
                return None
            return val

        return None
    def delete(self, key: str) -> None:
        """
        Issues a logical deletion for the specified key.

        Workflow:
          - Appends a DELETE record to the WAL.
          - Marks the key as deleted in the MemTable using a tombstone.
          - Applies the configured WAL persistence policy.
        """
        self._ensure_open()

        self._ensure_wal_initialized()
        self.wal.append_delete(key)

        self._pending_wal_ops += 1
        self._after_wal_append()

        # Record tombstone in the MemTable (logical deletion).
        self.memtable[key] = TOMBSTONE
        self._maybe_flush_memtable()

    def _append_wal(self, op_type: str, key: str, value: Optional[str]) -> None:
        """Low-level WAL append hook (reserved for future customization)."""
        raise NotImplementedError

    def _maybe_flush_memtable(self) -> None:
        """
        Checks whether the MemTable size exceeds the configured threshold,
        and triggers a flush to SST if necessary.
        """
        if len(self.memtable) >= self.config.memtable_limit:
            self._flush_to_sst()

    def _flush_to_sst(self) -> None:
        """
        Flushes the current MemTable into a new SST segment.

        Steps:
          - Constructs the output SST file path.
          - Emits the SST via SSTFile.write_from_memtable().
          - Registers the SST in the in-memory SST list.
          - Resets the MemTable.
        """
        if not self.memtable:
            return

        path = self._next_sst_path()
        sst = SSTFile.write_from_memtable(path=path, memtable=self.memtable)
        self.sst_files.append(sst)

        # Reset MemTable after flush.
        self.memtable = {}

    def _replay_wal(self) -> None:
        """
        Replays WAL records during engine startup to reconstruct
        the latest in-memory MemTable state.
        """
        if self.wal is None:
            return

        self.wal.replay_into(self.memtable)

    def _ensure_open(self) -> None:
        """Ensures that the engine is open before performing any operation."""
        if not self._is_open:
            raise RuntimeError("MiniKV is not open")

    def _ensure_wal_initialized(self) -> None:
        """Ensures that the WAL object has been created and initialized."""
        if self.wal is None:
            raise RuntimeError("WAL is not initialized")

    def _build_wal_path(self) -> str:
        """
        Returns the filesystem path for the WAL file and ensures
        that the storage directory exists.
        """
        os.makedirs(self.config.data_dir, exist_ok=True)
        return os.path.join(self.config.data_dir, "wal.log")

    def _next_sst_path(self) -> str:
        """
        Generates the next SST filename based on the current number of SST files.
        """
        os.makedirs(self.config.data_dir, exist_ok=True)
        index = len(self.sst_files)
        filename = f"sst_{index:04d}.txt"
        return os.path.join(self.config.data_dir, filename)

    def _after_wal_append(self) -> None:
        """
        Applies the configured WAL persistence policy after a WAL append.

        Supported policies:
          - SYNC:      fsync after each operation.
          - BATCH:     fsync periodically or after accumulating a batch.
          - ADAPTIVE:  dynamically adjusts the batch size.
        """
        mode = self.config.write_mode
        if mode == WriteMode.SYNC:
            self._sync_wal_now()
        elif mode == WriteMode.BATCH:
            self._maybe_sync_batch()
        elif mode == WriteMode.ADAPTIVE:
            self._maybe_sync_adaptive()

    def _sync_wal_now(self) -> None:
        """
        Forces an immediate WAL sync.

        Updates:
          - Resets the pending-WAL-ops counter.
          - Records sync timestamp for future batch/adaptive policies.
          - Maintains internal fsync statistics for adaptive tuning.
        """
        if self.wal is None:
            return

        now = time.time()
        elapsed = None if self._last_sync_time == 0.0 else now - self._last_sync_time
        pending = self._pending_wal_ops

        self.wal.sync()
        self._fsync_count += 1
        self._pending_wal_ops = 0
        self._last_sync_time = now

        # Estimate QPS and update the adaptive batch size if applicable.
        if elapsed is not None and elapsed > 0 and pending > 0:
            qps = pending / elapsed
            self._update_adaptive_batch_size(qps)

    def _maybe_sync_batch(self) -> None:
        """
        Implements the fixed-batch WAL sync policy.

        A sync is triggered when:
          - The number of pending WAL writes reaches 'batch_size', or
          - The time since the last sync exceeds 'batch_interval_ms'.
        """
        if self.wal is None:
            return

        if self._pending_wal_ops >= self.config.batch_size:
            self._sync_wal_now()
            return

        now = time.time()
        interval = self.config.batch_interval_ms / 1000.0
        if now - self._last_sync_time > interval:
            self._sync_wal_now()

    def _maybe_sync_adaptive(self) -> None:
        """
        Implements an adaptive WAL syncing strategy that tunes
        batch size based on observed write throughput.
        """
        if self.wal is None:
            return

        if self._pending_wal_ops >= self._adaptive_batch_size:
            self._sync_wal_now()
            return

        now = time.time()
        interval = self.config.batch_interval_ms / 1000.0
        if now - self._last_sync_time > interval:
            self._sync_wal_now()

    def _update_adaptive_batch_size(self, qps: float) -> None:
        """
        Adjusts the adaptive WAL batch size based on observed write throughput.

        Logic:
          - If throughput is high, larger batches reduce fsync frequency
            and improve write efficiency.
          - If throughput is low, a smaller batch size favors lower latency
            and faster durability.
        """
        base = self.config.batch_size
        LOW = 1000
        HIGH = 10000

        if qps >= HIGH:
            # High throughput → increase batch size to reduce fsync overhead.
            self._adaptive_batch_size = base * 4
        elif qps <= LOW:
            # Low throughput → revert batch size to preserve durability.
            self._adaptive_batch_size = base

    def _load_sst_files(self) -> None:
        """
        Lazily loads existing SST file descriptors.

        Behavior:
          - Only constructs SSTFile objects from file names.
          - Does not read file contents or compute key-range metadata.
            These will be populated on-demand by SSTFile during search().
        """
        self.sst_files = []

        data_dir = self.config.data_dir
        if not os.path.exists(data_dir):
            return

        names = [
            name for name in os.listdir(data_dir)
            if name.startswith("sst_") and name.endswith(".txt")
        ]

        names.sort()  # Maintains chronological ordering (sst_0000, sst_0001, ...)

        for name in names:
            path = os.path.join(data_dir, name)
            sst = SSTFile(path)
            # Key-range metadata (min_key / max_key) is left unset
            # and will be populated lazily during SST search.
            self.sst_files.append(sst)

    @property
    def fsync_count(self) -> int:
        """
        Returns the number of WAL fsync operations performed
        during the current engine open() lifecycle.
        """
        return self._fsync_count

    def compact_all(self) -> None:
        """
        Public API for triggering a full compaction.

        Current behavior:
          - Delegates directly to compaction.compact_all(self, TOMBSTONE),
            which performs a global merge of all SST files and checkpoints the WAL.

        Future extensions:
          - Additional compaction modes (e.g., leveled, tiered) may be selected here
            based on configuration or runtime heuristics.
        """
        compaction.compact_all(self, TOMBSTONE)
