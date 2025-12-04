# Mini-KV 设计说明（强化版）

本项目旨在构建一个简化但结构完整的 KV 存储引擎，用于展示 LSM-Tree 在写密集场景下的核心设计思想与系统级 trade-off。整个系统采用典型的写路径：**先写 WAL，再更新 MemTable，最终 flush 成不可变的有序 SST 文件**。
 此外，本项目将重点研究不同 WAL fsync 策略对吞吐、延迟与可靠性的影响。

## 1. 写入路径（Write Path）

当用户调用 `put(k, v)` 时，系统执行如下步骤：

### （1）Append-only 写入 WAL

- 将操作以追加记录的方式写入 WAL，以获得顺序写带来的低延迟。
- WAL 是整个系统的持久化来源，用于在 crash 时恢复最新 MemTable 状态。
- 写 WAL 是写入路径的“可靠性屏障”——记录一旦落盘，则最终可以恢复。

### （2）更新 MemTable

- 在 WAL 写入成功后，更新内存中的 MemTable。
- MemTable 保存最新的数据，是所有读取请求的首选查询位置。

上述流程确保了 LSM 的关键不变量：

> **已写入 WAL 的操作最终一定可恢复。**

## 2. Flush 与 SST 文件（SST Files）

当 MemTable 达到容量阈值时，其内容会被排序并写入磁盘，生成一个不可变的 SST 文件。

SST 文件具有以下特性：

1. 内部按 key 排序，可进行二分查找。
2. 文件不可修改，只能追加生成，从而避免随机写。
3. 多个 SST 文件按时间顺序堆叠形成“层级结构”。

此设计使系统获得高写入吞吐，但需要依赖 compaction 控制文件数量和读放大。

## 3. Compaction 策略（Key-Range Merge）

Compaction 是 LSM 树的核心操作，用于控制读放大与磁盘占用。本项目采用一个简化的两层 compaction 策略：

1. 最新的 SST（Level-0）与旧 SST 之间按 key 范围进行合并。
2. 去除删除标记（tombstones）与旧版本的 key。
3. 生成 key 范围不重叠的新 SST 文件，保持读取路径的高效性。

该策略保持 LSM 基本结构，同时避免实现复杂的多层 compaction 算法。

## 4. 三种 WAL fsync 策略（研究部分）

为了研究 reliability–latency–throughput 之间的系统级 trade-off，本项目实现三种 WAL 持久化策略：

### **（1）Sync Every Write（强一致落盘）**

- 每个 `put()` 后立即执行 fsync。
- 保证可靠性最高（最多丢 0 条）。
- 延迟最高，吞吐最低。

### **（2）Fixed Batch Write（固定批量写）**

- 累积 N 条 WAL 记录后再 fsync。
- 延迟显著降低，吞吐提高。
- crash 时最多丢失 N 条记录。

### **（3）Adaptive Batch Write（自适应批量写，创新点）**

根据实时负载调整批量大小：

- 高写入负载 → 扩大批量，提高吞吐
- 低写入负载 → 缩小批量，提高可靠性
- 目标是得到比固定批量更好的“吞吐—可靠性折中”

本项目将评估三种策略在以下指标上的表现：

- 写吞吐（QPS）
- P99 写延迟
- fsync 次数
- crash 场景下的最大数据丢失量