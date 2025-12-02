# test_engine.py
import os
import tempfile

from minikv.engine import MiniKV
from minikv.config import MiniKVConfig, WriteMode  # 路径按你的工程来改


def make_config(tmpdir: str) -> MiniKVConfig:

    return MiniKVConfig(
        data_dir=tmpdir,
        write_mode=WriteMode.SYNC,
        # 其他参数用默认值
    )


def test_minikv_replay_basic():
    with tempfile.TemporaryDirectory() as tmpdir:
        cfg = make_config(tmpdir)

        # 第一次启动：写入数据并关闭
        kv1 = MiniKV(cfg)
        kv1.open()
        kv1.put("k1", "v1")
        kv1.put("k2", "v2")
        kv1.delete("k1")
        kv1.close()

        # 第二次启动：通过 WAL replay 恢复 memtable
        kv2 = MiniKV(cfg)
        kv2.open()
        assert kv2.get("k1") is None
        assert kv2.get("k2") == "v2"
        kv2.close()

        print("✅ test_minikv_replay_basic passed")


if __name__ == "__main__":
    test_minikv_replay_basic()
    print("🎉 All MiniKV tests passed")
