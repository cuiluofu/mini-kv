from typing import Optional
from .config import MiniKVConfig, WriteMode


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
        self.wal = None  # 未来会变成 WAL 对象

    def open(self) -> None:
        """
        打开引擎：
        - 创建数据目录（如不存在）
        - 打开或创建 WAL
        - 加载已有 SST 文件元数据
        - replay WAL，重建 MemTable
        """
        raise NotImplementedError

    def close(self) -> None:
        """
        关闭引擎：
        - flush MemTable
        - 关闭 WAL
        """
        raise NotImplementedError

    def put(self, key: str, value: str) -> None:
        """
        写入一个 key-value:
        - 写 WAL (根据 write_mode 决定是否/何时 fsync)
        - 更新 MemTable
        """
        raise NotImplementedError

    def get(self) -> Optional[str]:
        """
        读取 key:
        - 先查 MemTable
        - 再查 SST (从新到旧)
        如果不存在，返回 None
        """
        raise NotImplementedError

    def delete(self, key: str) -> None:
        """
        删除 key:
        - 写 WAL (记录 delete 操作)
        - 在 MemTable 中标记为删除 (tombstone)
        """
        raise NotImplementedError

    def _append_wal(self, op_type:str, key:str,value:Optional[str]) -> None:
        """向 WAL 追加一条记录，具体格式后面再定。"""
        raise NotImplementedError

    def _maybe_flush_memtable(self) -> None:
        """在每次写后检查是否需要 flush MemTable"""
        raise NotImplementedError

    def _flush_to_sst(self) -> None:
        """把当前 MemTable 刷成一个新的 SST 文件"""
        raise NotImplementedError

    def _replay_walk(self) -> None:
        """启动时 replay WAL, 重建 MemTable """
        raise NotImplementedError