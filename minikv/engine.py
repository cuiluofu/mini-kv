import time
from typing import Optional
from .config import MiniKVConfig, WriteMode
from .wal import WAL
from .sst import SSTFile
import os

TOMBSTONE = "__MINIKV_TOMBSTONE__"

class MiniKV:
    """
    MiniKV引擎： 提供 put/get/delete 等对外接口、
    内部由 WAL + MemTable +SST files + Compaction 组成、
    """

    def __init__(self, config: MiniKVConfig):
        """
        创建一个 MiniKV 实例，但不立刻打开文件。
        需要调用 open() 才会真正加载 WAL/SST 等
        """
        self.config = config
        self.memtable = {}
        self.sst_files = []  # 未来可以用来存路径或元数据
        self.wal: Optional[WAL] = None  # 未来会变成 WAL 对象
        self._is_open = False
        self._pending_wal_ops = 0
        self._last_sync_time = 0.0
        self._adaptive_batch_size = config.batch_size

    def open(self) -> None:
        """
        打开引擎：
        - 创建数据目录（如不存在）
        - 打开或创建 WAL
        - 加载已有 SST 文件元数据 (暂时没有)
        - replay WAL，重建 MemTable
        """
        if self._is_open:
            return

        # 1. 标记为open
        self._is_open = True
        # 2、 初始化 memtable
        self.memtable = {}  # 清空 memtable

        # 先加载已有 SST 列表（只建对象，不扫内容）
        self._load_sst_files()

        # 3. 创建 WAL 实例，并打开文件
        wal_path = self._build_wal_path()
        self.wal = WAL(wal_path)
        self.wal.open()

        # 4. 从 WAL replay 到 memtable
        self._replay_wal()

    def close(self) -> None:
        """ 关闭引擎：
        - flush MemTable
        - 关闭 WAL
        Milestone 1 ： 以内存方式关闭 KV 引擎
        - 只修改状态，不做flush、也没有 WAL
        """
        if not self._is_open:
            return
        # 先 flush 一下剩余的 MemTable
        self._flush_to_sst()

        if self.wal is not None:
            self._sync_wal_now() # 确保 WAL 落盘
            self.wal.close()
            self.wal = None

        self._is_open = False

    def put(self, key: str, value: str) -> None:
        """
        写入一个 key-value:
        - 写 WAL (根据 write_mode 决定是否/何时 fsync)
        - 更新 MemTable
        """
        self._ensure_open()
        # 1. 先写 WAL
        self._ensure_wal_initialized()
        self.wal.append_put(key, value)

        # 2. 暂时立刻sync一下，后面再换成策略
        self._pending_wal_ops += 1
        self._after_wal_append()

        # 3. 再更新内存
        self.memtable[key] = value
        self._maybe_flush_memtable()

    def get(self, key: str) -> Optional[str]:
        """
        读取 key:
        - 先查 MemTable
        - 再查 SST (从新到旧)
        如果不存在，返回 None
        Milestone 1:
        - 先检查 is_open
        - 再从memtable里查，如果存在，返回对应的字符串；如果不存在，返回None
        """
        self._ensure_open()

        if key in self.memtable:
            val = self.memtable[key]
            if val == TOMBSTONE:
                return None
            return val

        for sst in reversed(self.sst_files):
            val = sst.search(key)
            if val == TOMBSTONE:
                return None

            return val

        return None

        # 1. 先查 MemTable
        if key in self.memtable and self.memtable[key] != TOMBSTONE:
            return self.memtable.get(key)
        # 2. 再按“新到旧”的顺序查SST
        for sst in reversed(self.sst_files):
            value = sst.search(key)
            if value is not None and value != TOMBSTONE:
                return value

        return None

    def delete(self, key: str) -> None:
        """
        删除 key:
        - 写 WAL (记录 delete 操作)
        - 在 MemTable 中标记为删除 (tombstone)
        """
        self._ensure_open()

        self._ensure_wal_initialized()
        self.wal.append_delete(key)

        self._pending_wal_ops += 1
        self._after_wal_append()

        # 再从 memtable 中删除（存在才删）
        self.memtable[key] = TOMBSTONE
        self._maybe_flush_memtable()

    def _append_wal(self, op_type: str, key: str, value: Optional[str]) -> None:
        """向 WAL 追加一条记录，具体格式后面再定。"""
        raise NotImplementedError

    def _maybe_flush_memtable(self) -> None:
        """在每次写后检查是否需要 flush MemTable"""
        if len(self.memtable) >= self.config.memtable_limit:
            self._flush_to_sst()

    def _flush_to_sst(self) -> None:
        """把当前 MemTable 刷成一个新的 SST 文件"""
        # 1. 构造文件路径，例如 data_dir / sst_0001.txt
        # 2. 调用 SSTFile.write_from_memtable(...)
        # 3. 把返回的 SSTFile 对象 append 到 self.sst_files
        # 4. 清空 memtable
        if not self.memtable:
            return
        path = self._next_sst_path()
        sst = SSTFile.write_from_memtable(path=path, memtable=self.memtable)
        self.sst_files.append(sst)

        # 刷盘后， 清空 Memtable
        self.memtable = {}

    def _replay_wal(self) -> None:
        """启动时 replay WAL, 重建 MemTable """
        if self.wal is None:
            return
        # 将 wal 中记录的操作按顺序应用到 memtable
        self.wal.replay_into(self.memtable)

    def _ensure_open(self) -> None:
        if not self._is_open:
            raise RuntimeError("MiniKV is not open")

    def _ensure_wal_initialized(self) -> None:
        if self.wal is None:
            raise RuntimeError("WAL is not initialized")

    def _build_wal_path(self) -> str:
        # 确保 data_dir 存在
        os.makedirs(self.config.data_dir, exist_ok=True)
        return os.path.join(self.config.data_dir, "wal.log")

    def _next_sst_path(self) -> str:
        """
        根据当前已有 SST 文件数量生成新文件名。
        """
        os.makedirs(self.config.data_dir, exist_ok=True)
        index = len(self.sst_files)
        filename = f"sst_{index:04d}.txt"
        return os.path.join(self.config.data_dir, filename)

    def _after_wal_append(self) -> None:
        """
        每追加一条 WAL 记录后，根据 write_mode决定是否触发sync()
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
        每次写入 WAL 都刷
        """
        if self.wal is None:
            return
        now = time.time()
        if self._last_sync_time == 0.0:
            elapsed = None
        else:
            elapsed = now - self._last_sync_time
        pending = self._pending_wal_ops
        self.wal.sync()
        self._pending_wal_ops = 0
        self._last_sync_time = now

        # 更新 QPS 估计
        if elapsed is not None and elapsed > 0 and pending > 0:
            qps = pending / elapsed
            self._update_adaptive_batch_size(qps)

    def _maybe_sync_batch(self) -> None:
        """
        若写满 batch_size条再sync
        活着距离上次sync超过batch_interval_ms即sync
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
        base = self.config.batch_size
        LOW = 1000
        HIGH = 10000

        if qps >= HIGH:
            # 负载很高，增大批量减少 fsync 次数
            self._adaptive_batch_size = base * 4
        elif qps <= LOW:
            # 负载不高，保持更小的批量，提高可靠性
            self._adaptive_batch_size = base

    def _load_sst_files(self) -> None:
        """
        懒加载已有 SST 文件：
        - 只根据文件名构造 SSTFile 对象
        - 不预先读取内容，也不计算 min/max（交给 SSTFile 自己懒加载）
        """
        self.sst_files = []

        data_dir = self.config.data_dir
        if not os.path.exists(data_dir):
            return

        names = [
            name for name in os.listdir(data_dir)
            if name.startswith('sst_') and name.endswith(".txt")
        ]

        names.sort()

        for name in names:
            path = os.path.join(data_dir, name)
            sst = SSTFile(path)
            # min_key / max_key 先保持 None，search() 时懒加载
            self.sst_files.append(sst)


    def compact_all(self) -> None:
        """
        手动触发一次“全量 compaction”

        - 先 flush 当前 MemTable
        - 把磁盘上所有 SST 全量读一遍，从“新到旧”合并成一个最新快照
        - 最新快照中：
            * 普通 key 保留最新值
            * 最新记录为TOMBSTONE 的 key被物理删除（不再写入新SST）
        - 删除旧 SST 文件，只保留一个新的合并SST

        注意：
        - 当前是一个“暂停世界”的简单实现，没有任何并发/后台线程
        """
        raise NotImplementedError