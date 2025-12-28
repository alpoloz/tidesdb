# Chapter 1

An LSM-tree (Log-Structured Merge-tree) is a write-optimized storage design: updates land in memory first, then roll out to disk as immutable, sorted files. The upside is fast sequential writes and predictable compaction; the tradeoff is that reads may consult several files until compaction trims them down.

This first chapter sets the backbone in place. Every write is appended to a write-ahead log (WAL), a sequential record of changes that makes recovery deterministic. If the process crashes, replaying the WAL restores the latest in-memory state. That in-memory state is the memtable, a transient structure that holds the newest versions of keys before they are flushed to disk.

When the memtable crosses a size threshold, it is written out as a sorted string table (SSTable). SSTables are immutable, ordered files that serve two goals: stable disk storage and fast point lookups. Each SSTable stores records in key order and a compact in-memory index maps keys to offsets, so random reads can jump directly to the right place.

Compaction ties the loop together. As more SSTables accumulate, older tables are merged into newer ones, collapsing multiple versions of the same key and discarding tombstones. This keeps read amplification in check while maintaining the write-friendly append-only behavior.

The result is intentionally small and readable: WAL for durability, memtable for speed, SSTables for persistence, and compaction to keep the system tidy. The next step is to make the memtable structure itself more LSM-friendly.
