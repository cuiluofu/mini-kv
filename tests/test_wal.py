import os
import tempfile

from minikv.wal import WAL, TOMBSTONE


def test_wal_replay_basic() -> None:
    """
    Validates basic WAL replay behavior.

    Scenario:
      1) Create a WAL file and append:
           - PUT a=1
           - PUT b=2
           - DEL a
      2) Close the WAL.
      3) Reopen the WAL and replay its contents into an empty MemTable.
      4) Final state should contain:
             b → "2"
         and key 'a' should be represented as a tombstone and therefore omitted.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "wal.log")

        # First run: generate WAL contents.
        wal = WAL(wal_path)
        wal.open()
        wal.append_put("a", "1")
        wal.append_put("b", "2")
        wal.append_delete("a")
        wal.close()

        # Second run: replay WAL content.
        memtable = {}
        wal2 = WAL(wal_path)
        wal2.replay_into(memtable)

        # Expected: key "a" was deleted; only "b" remains.
        print("✅ test_wal_replay_basic passed:", memtable)


def test_wal_replay_empty_file() -> None:
    """
    Ensures replay_into() correctly handles an empty WAL file:
      - No operations should be applied.
      - The MemTable remains empty.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "wal.log")

        # Create an empty file.
        open(wal_path, "w").close()

        memtable = {}
        wal = WAL(wal_path)
        wal.replay_into(memtable)

        assert memtable == {}
        print("✅ test_wal_replay_empty_file passed:", memtable)


def test_wal_replay_file_not_exist() -> None:
    """
    Ensures replay_into() is tolerant of a missing WAL file:
      - No exception should be raised.
      - MemTable remains empty.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "wal.log")  # File not created

        memtable = {}
        wal = WAL(wal_path)
        wal.replay_into(memtable)

        assert memtable == {}
        print("✅ test_wal_replay_file_not_exist passed:", memtable)


if __name__ == "__main__":
    test_wal_replay_basic()
    test_wal_replay_empty_file()
    test_wal_replay_file_not_exist()
