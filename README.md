# MiniKV: A Small LSM-Style Key–Value Store (Built for Learning Systems)

MiniKV is a minimal key–value store I built while learning how storage engines such as LevelDB or RocksDB work internally. 
The goal is not to create a production database, but to understand and re-implement the core mechanisms of LSM-tree systems:

- Write-Ahead Logging (WAL)
- MemTable → SST flushing
- Tombstone-based deletion
- Full compaction and snapshots
- Durability policies based on fsync frequency

The entire codebase is intentionally kept small so that each component is easy to explain and reason about.


## 1. Why I Built This

I wanted to understand the internals of modern LSM-tree storage engines through a hands-on implementation, rather than only reading papers or documentation. Building MiniKV helped me explore several core questions:

- how a write-ahead log ensures durability and supports crash recovery,
- how data flows from MemTable to immutable SST files,
- how tombstones and versioning are handled in practice,
- how compaction cleans up stale data and rebuilds a consistent snapshot,
- how different fsync policies affect write throughput.

MiniKV is my way of learning these mechanisms by implementing them end-to-end in a small, explainable system.



## 2. Architecture Overview (High-Level)

MiniKV follows a simplified LSM-tree workflow:

```text
put / delete
    ↓
  WAL (append-only)
    ↓
  MemTable (in memory)
    ↓
  SST files (sorted, immutable)
    ↓
  Compaction (merge + clean up + snapshot)
```

Reads consult:

1. MemTable  
2. SST files (from newest to oldest)

This matches the “latest write wins” semantics commonly used in real LSM-tree systems.


## 3. Main Components

### WAL (Write-Ahead Log)
- Append-only text log (`wal.log`)
- Each write is recorded before being applied to MemTable
- Replayed on startup to restore state after a crash

### MemTable & SST Files
- MemTable is an in-memory dictionary
- When full, it is flushed into a new SST file
- SST files are immutable and sorted, stored as `key\tvalue` lines

### Tombstone-Based Deletion
- Delete operations write a tombstone marker
- Tombstones override older SST values
- Compaction removes tombstones physically

### Full Compaction
- Merges all SST files from newest to oldest
- Keeps only the latest version of each key
- Drops keys whose latest value is a tombstone
- Produces one compacted SST and truncates WAL (checkpoint)

### WAL Durability Policies
- **SYNC** – fsync after every operation  
- **BATCH** – fsync every *N* operations or after a time interval  
- **ADAPTIVE** – adjusts batch size according to recent write throughput  


## 4. Performance Summary

Benchmark: 500K writes on a consumer SSD.

| Mode     | QPS       | fsyncs  |
|----------|-----------|---------|
| SYNC     | 2.3 K     | 500,001 |
| BATCH    | 189 K     | 5,003   |
| ADAPTIVE | 402 K     | 1,253   |

The ADAPTIVE policy achieves roughly 170× higher throughput than SYNC while also reducing the number of `fsync` calls from 500K to about 1.2K.

## 5. Repository Structure

```text
minikv/
  engine.py         # main KV engine
  wal.py            # WAL format + replay
  sst.py            # SST read/write logic
  compaction.py     # full compaction implementation
  config.py         # engine config + WAL policies


tests/
  basic_test.py
  test_wal.py
  test_sst.py
  test_engine.py
  test_restart_delete.py
  bench_write.py      # benchmark for write durability modes

README.md
```

## 6. Getting Started

### Requirements
- Python 3.10+
- No external dependencies; everything uses the standard library.

### Running Tests

```bash
# Run from the project root directory
python -m tests.test_wal
python -m tests.test_sst
python -m tests.test_engine
python -m tests.test_restart_delete
```
### Running the Benchmark

```bash
python -m tests.bench_write
```
This benchmark compares SYNC, BATCH, and ADAPTIVE WAL durability policies.

## 7. Future Work

MiniKV is intentionally minimal and single-threaded.  
Possible future extensions include:

- Leveled or tiered compaction strategies  
- Background compaction thread  
- Sparse indexes / block-based SST layout  
- Bloom filters to reduce disk lookups  
- Concurrent readers with finer-grained locking  
- More realistic workloads (p95/p99 latency)

---

This project helped me understand durability, crash recovery, IO paths, compaction, and performance trade-offs — the core ideas behind modern LSM-tree storage engines.