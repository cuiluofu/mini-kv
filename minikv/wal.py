from typing import Optional, TextIO
import os

TOMBSTONE = "__MINIKV_TOMBSTONE__"
class WAL:
    """
    Write-Ahead Log 抽象：
    - 负责把操作顺序记录到 wal.log
    - 提供 replay 功能，用于启动时重建 MemTable
    """

    def __init__(self, path: str):
        """
        :param path: WAL 文件路径，例如data_dir 下的“wal.log”
        """
        self.path = path
        self._fp: Optional[TextIO] = None

    def open(self) -> None:
        """
        打开（或创建） WAL文件，准备追加写入。
        - 如果文件不存在，则创建新文件
        - 如果文件存在，则以 append 模式打开
        """
        # 如果已经 open，则无需重复打开（避免文件句柄泄露）
        if self._fp is not None:
            return
        # 处理可能没有目录的情况（如 "wal.log"）
        dirname = os.path.dirname(self.path)
        if dirname:
            os.makedirs(dirname, exist_ok=True)
        # a+ = 可读可写、若不存在则创建
        self._fp = open(self.path, "a+", encoding="utf-8")
        # 确保指针在末尾（a+ 模式下不是严格保证）
        self._fp.seek(0, os.SEEK_END)

    def append_put(self, key: str, value: str) -> None:
        """
        追加一条 PUT 记录到 WAL。
        格式：“PUT\\t{key}\\t{value}\\n”
        (注意：实际写入时使用的是制表符合换行符)
        """
        # 确保 _fp 已经打开，否则报错
        if self._fp is None:
            raise RuntimeError("WAL is not open")
        line = f"PUT\t{key}\t{value}\n"
        self._fp.write(line)

    def append_delete(self, key: str) -> None:
        """
        追加一条 DEL 记录到 WAL。
        格式： “DEL\\t{key}\\n”
        """
        # 确保 _fp 已经打开，否则报错
        if self._fp is None:
            raise RuntimeError("WAL is not open")
        line = f"DEL\t{key}\n"
        self._fp.write(line)

    def sync(self) -> None:
        """
        将 WAL 中已写入的数据刷到磁盘。
        - 目前简单用 file.flush() + os.fsync()
        - 后面做对比实验的时候再根据 write_mode 做策略上的控制
        """
        if self._fp is None:
            raise RuntimeError("WAL is not open")
        self._fp.flush()
        os.fsync(self._fp.fileno())

    def close(self) -> None:
        """
        关闭 WAL 文件
        """
        if self._fp is None:
            return

        # 尽量保证已经持久化到盘上
        self._fp.flush()
        os.fsync(self._fp.fileno())

        self._fp.close()
        self._fp = None

    def replay_into(self, memtable: dict) -> None:
        """
        从 WAL 文件中读取所有记录，并按顺序应用到给定的 memtable上。
        - 先清空 memtable 或者调用方保证传入的是空dict
        - 按行读取，解析 PUT/DEL, 更新 memtable
        """
        # 1. 如果 wal 文件不存在，说明没有历史记录，直接返回
        if not os.path.exists(self.path):
            return

        with open(self.path, "r",encoding="utf-8") as f:
            for line in f:
                line = line.rstrip("\n")
                if not line:
                    # 空行，跳过
                    continue
                parts = line.split("\t")
                if not parts:
                    continue

                op = parts[0]

                if op == "PUT" and len(parts) >= 3:
                    # 格式：PUT\tkey\tvalue
                    key = parts[1]
                    value = parts[2]
                    memtable[key] = value

                elif op == "DEL" and len(parts) >= 2:
                    # 格式：DEL\tkey
                    key = parts[1]
                    value = TOMBSTONE
                    memtable[key] = value
                else:
                    # 非法行：目前先忽略
                    # TODO: 未来可以加日志记录或抛异常
                    continue

