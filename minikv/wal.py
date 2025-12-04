from typing import Optional, TextIO
import os

TOMBSTONE = "__MINIKV_TOMBSTONE__"


class WAL:
    """
    Write-Ahead Log (WAL) for MiniKV.

    Responsibilities:
      - Appends all write operations in order to a persistent log.
      - Supports sequential replay to rebuild MemTable state on startup.
    """

    def __init__(self, path: str):
        """
        Initializes a WAL object bound to the specified filesystem path.
        """
        self.path = path
        self._fp: Optional[TextIO] = None

    def open(self) -> None:
        """
        Opens (or creates) the WAL file in append mode.

        Behavior:
          - Ensures the parent directory exists.
          - Opens the log with 'a+' (read/write, create-if-missing).
          - Moves the file pointer to the end to guarantee append semantics.
        """
        if self._fp is not None:
            return  # Avoid reopening an already-open file handle.

        dirname = os.path.dirname(self.path)
        if dirname:
            os.makedirs(dirname, exist_ok=True)

        self._fp = open(self.path, "a+", encoding="utf-8")
        self._fp.seek(0, os.SEEK_END)

    def append_put(self, key: str, value: str) -> None:
        """
        Appends a PUT operation to the WAL using the format:

            PUT\t{key}\t{value}\n

        The call does not guarantee durability; sync() must be invoked
        according to the engine's write policy.
        """
        if self._fp is None:
            raise RuntimeError("WAL is not open")

        line = f"PUT\t{key}\t{value}\n"
        self._fp.write(line)

    def append_delete(self, key: str) -> None:
        """
        Appends a DELETE operation to the WAL using the format:

            DEL\t{key}\n
        """
        if self._fp is None:
            raise RuntimeError("WAL is not open")

        line = f"DEL\t{key}\n"
        self._fp.write(line)

    def sync(self) -> None:
        """
        Forces all buffered WAL writes to durable storage.

        Implementation:
          - flush() ensures Python's internal buffers are written to the OS.
          - os.fsync() forces the OS to persist data to disk.
        """
        if self._fp is None:
            raise RuntimeError("WAL is not open")

        self._fp.flush()
        os.fsync(self._fp.fileno())

    def close(self) -> None:
        """
        Closes the WAL file and ensures durability before release.
        """
        if self._fp is None:
            return

        self._fp.flush()
        os.fsync(self._fp.fileno())

        self._fp.close()
        self._fp = None

    def replay_into(self, memtable: dict) -> None:
        """
        Replays all WAL records and applies them to the provided MemTable.

        Semantics:
          - Each PUT overwrites the previous value.
          - Each DEL records a tombstone, representing a logical deletion.
          - Invalid or malformed lines are ignored.
        """
        if not os.path.exists(self.path):
            return  # No WAL → no state to restore.

        with open(self.path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.rstrip("\n")
                if not line:
                    continue  # Skip empty lines.

                parts = line.split("\t")
                if not parts:
                    continue

                op = parts[0]

                if op == "PUT" and len(parts) >= 3:
                    key = parts[1]
                    value = parts[2]
                    memtable[key] = value

                elif op == "DEL" and len(parts) >= 2:
                    key = parts[1]
                    memtable[key] = TOMBSTONE

                else:
                    # Malformed or unsupported WAL entry.
                    continue
