import os
import tempfile

from minikv.wal import WAL


def test_wal_replay_basic():
    # 1. 创建一个临时目录，避免污染真实工程目录
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "wal.log")
        # 2. 第一次：写 WAL
        wal = WAL(wal_path)
        wal.open()
        wal.append_put("a", "1")
        wal.append_put("b", "2")
        wal.append_delete("a")
        wal.close()

        # 3. 第二次,重启后 replay
        memtable = {}
        wal2 = WAL(wal_path)
        wal2.replay_into(memtable)

        # 4. 检查最终状态
        assert memtable == {"b": "2"}
        print("✅ test_wal_replay_basic passed:", memtable)

def test_wal_replay_empty_file():
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "wal.log")

        open(wal_path, "w").close()

        memtable = {}
        wal = WAL(wal_path)
        wal.replay_into(memtable)

        assert memtable == {}
        print("✅ test_wal_replay_empty_file passed:", memtable)

def test_wal_replay_file_not_exist():
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = os.path.join(tmpdir, "wal.log")  # 注意：不创建

        memtable = {}
        wal = WAL(wal_path)
        wal.replay_into(memtable)

        assert memtable == {}
        print("✅ test_wal_replay_file_not_exist passed:", memtable)

if __name__ == "__main__":
    test_wal_replay_basic()
    test_wal_replay_empty_file()
    test_wal_replay_file_not_exist()