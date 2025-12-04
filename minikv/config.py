from enum import Enum

class WriteMode(Enum):
    """Supported WAL persistence strategies."""
    SYNC = 1        # Forces fsync after every write operation
    BATCH = 2       # Groups multiple WAL writes before performing fsync
    ADAPTIVE = 3    # Dynamically adjusts batch size based on workload characteristics


class MiniKVConfig:
    """Configuration parameters for the MiniKV storage engine."""

    def __init__(
        self,
        data_dir: str = "data/",
        write_mode: WriteMode = WriteMode.SYNC,
        batch_size: int = 10,
        batch_interval_ms: int = 5,
        memtable_limit: int = 1000,
    ):
        """
        Initializes configuration settings for the engine.

        Attributes:
          - data_dir:
                Base directory where WAL and SST files are stored.
          - write_mode:
                WAL persistence policy (SYNC / BATCH / ADAPTIVE).
          - batch_size:
                Target number of WAL records per fsync in BATCH or ADAPTIVE mode.
          - batch_interval_ms:
                Time-based fsync interval used in BATCH or ADAPTIVE mode.
          - memtable_limit:
                Maximum number of keys in the MemTable before a flush is triggered.
        """
        self.data_dir = data_dir
        self.write_mode = write_mode
        self.batch_size = batch_size
        self.batch_interval_ms = batch_interval_ms
        self.memtable_limit = memtable_limit
