import os.path
from typing import Optional


class SSTFile:
    """
    表示一个 SST 文件： 包含文件路径和元数据（例如最小/最大 key）
    """

    def __init__(self, path: str):
        self.path = path
        self.min_key = None
        self.max_key = None

    @staticmethod
    def write_from_memtable(path: str, memtable: dict[str, str]) -> "SSTFile":
        """
        将 memtable 排序后写入SST文件
        """
        # 排序 -> 写入文件 -> 返回 SSTFile 实例
        # 1. 把memtable的（k, v）拿出来，并按 key 排序
        # memtable.items() -> Iterable[(key, value)]
        # sorted 默认按 key 排序， 因为(key, value) 是元组，先比 key
        items: list[tuple[str, str]] = sorted(memtable.items(), key=lambda kv: kv[0])

        # 2. 打开目录文件，覆盖写(w)
        with open(path, "w", encoding="utf-8") as f:
            for k, v in items:
                # 每行一个key-value, 用\t分隔
                f.write(f"{k}\t{v}\n")

        # 3. 构造 SSTFile 元数据对象
        sst = SSTFile(path)
        if items:
            sst.min_key = items[0][0]
            sst.max_key = items[-1][0]

        return sst

    def search(self, key: str) -> Optional[str]:
        """
        从 SST 文件中搜索 key（顺序扫描）
        - 找到则返回对应的value(字符串)
        - 找不到则返回 None
        """
        # 如果文件不存在，直接返回None
        if not os.path.exists(self.path):
            return None

        # 懒加载 min_key / max_key（老 SST 第一次被访问时才做一次扫）
        self._ensure_min_max_loaded()

        # 如果已经有了范围信息，先剪枝
        if self.min_key is not None and self.max_key is not None:
            if key < self.min_key or key > self.max_key:
                return None

        # 只读打开 SST文件, 逐行扫描
        with open(self.path, "r", encoding="utf-8") as f:
            for line in f:
                # 去掉末尾换行符
                line = line.strip("\n")
                if not line:
                    continue  # 跳过空行

                # 按第一个 \t 拆成 key 和 value
                # 限制maxsplit = 1 可以防止 value 中包含 \t 时被拆成多段
                parts = line.split("\t", 1)
                if len(parts) != 2:
                    # 格式异常（可添加日志，先跳过）
                    continue

                k, v = parts
                if k == key:
                    return v

            # 全文件扫描完没找到
            return None

    def _ensure_min_max_loaded(self) -> None:
        """
        懒加载 min_key / max_key：
        - 如果已经有值，直接返回
        - 如果还没有，就顺序扫一遍文件，拿到首行和末行的 key
        """
        if self.min_key is not None and self.max_key is not None:
            return

        if not os.path.exists(self.path):
            return

        first_key: str | None = None
        last_key: str | None = None

        with open(self.path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.rstrip("\n")
                if not line:
                    continue

                parts = line.split("\t", 1)
                if len(parts) != 2:
                    continue

                k, _ = parts
                if first_key is None:
                    first_key = k
                last_key = k

        self.min_key = first_key
        self.max_key = last_key

