"""
测试目标：
1）第一次运行：写入 + 删除，确保触发 flush，close；
2）第二次运行：重启后 open（加载 SST + 重放 WAL），验证：
   - 正常 key: get 拿到值
   - 删除 key: get 返回 None
   - SST 里确实有 tombstone 记录

用法：
  第一阶段（写入并关闭）：
    python test_restart_delete.py 1

  第二阶段（重启后验证）：
    python test_restart_delete.py 2

  或者在同一进程里连续做两阶段（不是严格“重启”，但方便快速自测）：
    python test_restart_delete.py both
"""

import os
import shutil
import sys

# 如果你的包名不是 mini_kv，这几行自己改一下
from minikv.config import MiniKVConfig, WriteMode
from minikv.engine import MiniKV, TOMBSTONE


DATA_DIR = "data_restart_test"


def make_config() -> MiniKVConfig:
    """
    配一个单独的数据目录 + 小一点的 memtable_limit，
    方便我们在少量 put/delete 下就触发 flush。
    """
    return MiniKVConfig(
        data_dir=DATA_DIR,
        write_mode=WriteMode.SYNC,  # 简化：每次都 fsync，避免“没落盘”干扰测试
        memtable_limit=2,           # 2 条就触发 flush，方便制造多个 SST
    )


def phase1_first_run() -> None:
    """
    第一次运行：
    - 清空旧数据目录
    - open
    - 连续 put / delete，制造：
        * 至少一个普通 key
        * 至少一个被删 key（写入过，再 tombstone）
      且 tombstone 要被 flush 到 SST
    - close
    """
    # 先清理掉历史数据，保证是一个干净场景
    shutil.rmtree(DATA_DIR, ignore_errors=True)

    config = make_config()
    kv = MiniKV(config)
    kv.open()

    print("=== [Phase 1] Start ===")

    # memtable_limit=2，下面这些操作会产生多次 flush：
    # 第一次 flush: 包含 k_alive_1, k_delete (value)
    kv.put("k_alive_1", "v1")
    kv.put("k_delete", "temp_value")

    # 第二次 flush: 包含 k_alive_2, k_delete (TOMBSTONE)
    kv.put("k_alive_2", "v2")
    kv.delete("k_delete")

    # 第三次 flush: close() 时把 k_alive_3 刷出去
    kv.put("k_alive_3", "v3")

    print(f"memtable before close: {kv.memtable}")
    kv.close()

    # 看一下当前 data_dir 里有什么
    print("Files in data dir after phase1:")
    for name in sorted(os.listdir(DATA_DIR)):
        print("  -", name)

    print("=== [Phase 1] Done ===")


def phase2_second_run() -> None:
    """
    第二次运行：
    - 用同一个 data_dir 再次构建 MiniKV，open()
      （内部会 lazy 加载 SST 列表 + 打开 WAL + replay）
    - 验证：
        * k_alive_* 能拿到正确值
        * k_delete 返回 None（删除语义）
    - 额外：直接用 SSTFile.search 验证 SST 中 tombstone 的存在，
      证明“删除”已经物化到 SST，而不是仅靠 WAL replay。
    """
    config = make_config()
    kv = MiniKV(config)
    kv.open()

    print("=== [Phase 2] Start ===")
    print(f"Loaded {len(kv.sst_files)} SST files:")
    for sst in kv.sst_files:
        print("  -", sst.path)

    # 1. 用对外接口 get() 验证“逻辑视图”
    alive_expect = {
        "k_alive_1": "v1",
        "k_alive_2": "v2",
        "k_alive_3": "v3",
    }
    deleted_keys = ["k_delete"]

    print("\n[Check] get() on alive keys(before compaction):")
    for k, expected in alive_expect.items():
        v = kv.get(k)
        print(f"  get({k!r}) = {v!r}")
        assert v == expected, f"Key {k} expected {expected!r}, got {v!r}"

    print("\n[Check] get() on deleted keys(before compaction):")
    for k in deleted_keys:
        v = kv.get(k)
        print(f"  get({k!r}) = {v!r}")
        assert v is None, f"Deleted key {k} should return None, got {v!r}"

    # 2. 直接访问 SST，验证：
    #    - k_delete 在“最新”的 SST 里是 TOMBSTONE
    #    - k_alive_* 至少在某个 SST 里有正确值
    print("\n[Check] SST contents via SSTFile.search()(before compaction):")

    # 2.1 deleted key 在某个 SST 里是 tombstone
    sst_tombstone = None
    for sst in reversed(kv.sst_files):  # 新到旧
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

    # 3. 触发一次全量 compaction
    print("\n[Compaction] Run compact_all() ...")
    old_sst_paths = [s.path for s in kv.sst_files]
    old_count = len(old_sst_paths)
    kv.compact_all()
    new_count = len(kv.sst_files)
    new_paths = [s.path for s in kv.sst_files]

    print(f" SST file count: {old_count} -> {new_count}")
    print("Old SST files:")
    for p in old_sst_paths:
        print("     -",p)
    print(" New SST files:")
    for p in new_paths:
        print("     -",p)

    assert new_count <= old_count, "SST file count should not increase after compaction"
    assert new_count >= 0

    wal_size_after = os.path.getsize(wal_path)
    print(f"\n[Checkpoint] wal.log size after compaction: {wal_size_after} bytes")
    # 理论上应该是明显变小（通常直接归零）
    assert wal_size_after <= wal_size_before, "wal.log should not grow after checkpoint"
    # 如果你期望是 0，可以更强一点：
    # assert wal_size_after == 0

    # 4. 验证：删除 key 已被“物理删除”，且 get 行为不变

    # 4.1 get 逻辑视图仍然正确
    print("\n[Check] get() on alive keys (after compaction):")
    for k, expected in alive_expect.items():
        v = kv.get(k)
        print(f"  get({k!r}) = {v!r}")
        assert v == expected, f"(after compaction) Key {k} expected {expected!r}, got {v!r}"

    print("\n[Check] get() on deleted keys (after compaction):")
    for k in deleted_keys:
        v = kv.get(k)
        print(f"  get({k!r}) = {v!r}")
        assert v is None, f"(after compaction) Deleted key {k} should return None, got {v!r}"

    # 4.2 物理检查：新生成的 SST 中，不再有 k_delete 行
    print("\n[Check] SST files physically (after compaction):")
    for sst in kv.sst_files:
        print(f"  Inspect {sst.path}:")
        with open(sst.path, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                k, v = line.rstrip("\n").split("\t", 1)
                assert k != "k_delete", "Deleted key k_delete should not appear in compacted SST"

    print("\nAll checks passed ✅")
    print("=== [Phase 2] Done ===")


def main():
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
