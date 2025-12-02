from typing import Optional
from .config import MiniKVConfig, WriteMode
from .wal import WAL
import os


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
        self.wal : Optional[WAL] = None  # 未来会变成 WAL 对象
        self._is_open = False

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
        # 未来：这里可以做一次 flush + wal.sync()
        self._ensure_wal_initialized()
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
        self.wal.sync()

        # 3. 再更新内存
        self.memtable[key] = value

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

        return self.memtable.get(key)

    def delete(self, key: str) -> None:
        """
        删除 key:
        - 写 WAL (记录 delete 操作)
        - 在 MemTable 中标记为删除 (tombstone)
        """
        self._ensure_open()

        self._ensure_wal_initialized()
        self.wal.append_delete(key)

        self.wal.sync()
        # 再从 memtable 中删除（存在才删）
        self.memtable.pop(key, None)

    def _append_wal(self, op_type: str, key: str, value: Optional[str]) -> None:
        """向 WAL 追加一条记录，具体格式后面再定。"""
        raise NotImplementedError

    def _maybe_flush_memtable(self) -> None:
        """在每次写后检查是否需要 flush MemTable"""
        raise NotImplementedError

    def _flush_to_sst(self) -> None:
        """把当前 MemTable 刷成一个新的 SST 文件"""
        raise NotImplementedError

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
        return os.path.join(self.config.data_dir,"wal.log")