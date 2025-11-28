from enum import Enum

class WriteMode(Enum):
    """WAL 落盘策略"""
    SYNC = 1    # 每次写后立刻fsync
    BATCH = 2   # 固定批量fsync
    ADAPTIVE = 3    # 自适应批量fsync

class MiniKVConfig:
    """MiniKV 引擎的配置项"""

    def __init__(
        self,
        data_dir: str = "data/",
        write_mode: WriteMode = WriteMode.SYNC,
        batch_size: int = 10,
        batch_interval_ms: int = 5,
        memtable_limit: int = 1000,
    ):
        """
        参数说明：
        - data_dir: 数据目录（WAL / SST 都放在这里）
        - write_mode: WAL 落盘模式（SYNC/BATCH/ADAPTIVE）
        - batch_size: BATCH/ADAPTIVE 模式下，目标批量条数
        - batch_interval_ms: BATCH/ADAPTIVE 模式下，定时 fsync 的时间间隔
        - memtable_limit: 触发 flush 的 MemTable 最大 key 数量
        """
        self.data_dir = data_dir
        self.write_mode = write_mode
        self.batch_size = batch_size
        self.batch_interval_ms = batch_interval_ms
        self.memtable_limit = memtable_limit
        