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
        self._is_open = False

    def open(self) -> None:
        """
        打开引擎：
        - 创建数据目录（如不存在）
        - 打开或创建 WAL
        - 加载已有 SST 文件元数据
        - replay WAL，重建 MemTable

        Milestone 1: 以内存方式打开 KV 引擎
        - 如果已经打开，直接返回
        - 标记为打开状态
        - 使用一个干净的memtable （每次open清空memtable）
        """
        if self._is_open:
            return

        self._is_open = True
        self.memtable = {} # 清空 memtable

    def close(self) -> None:
        """ 关闭引擎：
        - flush MemTable
        - 关闭 WAL
        Milestone 1 ： 以内存方式关闭 KV 引擎
        - 只修改状态，不做flush、也没有 WAL
        """
        self._is_open = False

    def put(self, key: str, value: str) -> None:
        """
        写入一个 key-value:
        - 写 WAL (根据 write_mode 决定是否/何时 fsync)
        - 更新 MemTable
        Milestone 1:
        - 把key对应的value写入memtable
        - 如果key已经存在，就覆盖旧值
        """
        self._ensure_open()

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

        self.memtable.pop(key,None)

    def _append_wal(self, op_type:str, key:str,value:Optional[str]) -> None:
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
        raise NotImplementedError

    def _ensure_open(self) -> None:
        if not self._is_open:
            raise RuntimeError("MiniKV is not open")