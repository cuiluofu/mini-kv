import os.path
from typing import Optional


class SSTFile:
    """
    Represents a single SST (Sorted String Table) file on disk.

    The object stores:
      - the filesystem path
      - lazily loaded metadata such as the minimum and maximum key
    """

    def __init__(self, path: str):
        self.path = path
        self.min_key = None
        self.max_key = None

    @staticmethod
    def write_from_memtable(path: str, memtable: dict[str, str]) -> "SSTFile":
        """
        Writes the contents of a MemTable into a new SST file.

        Process:
          - Sorts key–value pairs lexicographically by key.
          - Emits each pair as a tab-separated line.
          - Returns an SSTFile instance with initialized key-range metadata.
        """
        items: list[tuple[str, str]] = sorted(memtable.items(), key=lambda kv: kv[0])

        # Emit sorted entries into the SST file.
        with open(path, "w", encoding="utf-8") as f:
            for k, v in items:
                f.write(f"{k}\t{v}\n")

        # Construct the SST descriptor with key-range metadata.
        sst = SSTFile(path)
        if items:
            sst.min_key = items[0][0]
            sst.max_key = items[-1][0]

        return sst

    def search(self, key: str) -> Optional[str]:
        """
        Searches for a key inside the SST file using a linear scan.

        Behavior:
          - Returns the corresponding value if the key is found.
          - Returns None if the key is not present.

        Key-range metadata (min/max key) is used for inexpensive pruning
        before performing the full scan.
        """
        if not os.path.exists(self.path):
            return None

        # Load key-range metadata if not yet initialized.
        self._ensure_min_max_loaded()

        # Key-range pruning.
        if self.min_key is not None and self.max_key is not None:
            if key < self.min_key or key > self.max_key:
                return None

        # Sequential scan of the file.
        with open(self.path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip("\n")
                if not line:
                    continue

                # Split into key/value at the first tab.
                parts = line.split("\t", 1)
                if len(parts) != 2:
                    continue  # Ignore malformed lines.

                k, v = parts
                if k == key:
                    return v

        return None  # Key not found.

    def _ensure_min_max_loaded(self) -> None:
        """
        Lazily initializes the minimum and maximum keys for the SST file.

        Operation:
          - If metadata is already populated, no work is performed.
          - Otherwise performs a single sequential scan:
                * the first valid key becomes min_key
                * the last valid key becomes max_key

        This avoids scanning the file on every read and enables
        efficient key-range pruning.
        """
        if self.min_key is not None and self.max_key is not None:
            return

        if not os.path.exists(self.path):
            return

        first_key: str | None = None
        last_key: str | None = None

        with open(self.path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.rstrip("\n")
                if not line:
                    continue

                parts = line.split("\t", 1)
                if len(parts) != 2:
                    continue

                k, _ = parts
                if first_key is None:
                    first_key = k
                last_key = k

        self.min_key = first_key
        self.max_key = last_key
