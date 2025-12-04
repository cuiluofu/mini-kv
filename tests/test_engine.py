# test_engine.py
import os
import tempfile

from minikv.engine import MiniKV
from minikv.config import MiniKVConfig, WriteMode


def make_config(tmpdir: str) -> MiniKVConfig:
    """
    Constructs a MiniKVConfig instance targeting the given temporary directory.

    The write mode is fixed to SYNC for determinism; other parameters
    rely on their default values.
    """
    return MiniKVConfig(
        data_dir=tmpdir,
        write_mode=WriteMode.SYNC,
    )


def test_minikv_replay_basic() -> None:
    """
    Verifies that WAL replay correctly restores the latest key-value state.

    Scenario:
      1) First run:
           - Open engine
           - Put k1, k2
           - Delete k1
           - Close engine (WAL is persisted)
      2) Second run:
           - Open engine with the same config
           - Replay WAL into MemTable
           - Verify:
               * k1 is logically deleted (get returns None)
               * k2 retains its latest value
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        cfg = make_config(tmpdir)

        # First run: apply a sequence of updates and close the engine.
        kv1 = MiniKV(cfg)
        kv1.open()
        kv1.put("k1", "v1")
        kv1.put("k2", "v2")
        kv1.delete("k1")
        kv1.close()

        # Second run: WAL replay should reconstruct the final state.
        kv2 = MiniKV(cfg)
        kv2.open()
        assert kv2.get("k1") is None
        assert kv2.get("k2") == "v2"
        kv2.close()

        print("✅ test_minikv_replay_basic passed")


if __name__ == "__main__":
    test_minikv_replay_basic()
    print("🎉 All MiniKV tests passed")
