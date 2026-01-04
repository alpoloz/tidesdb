# Chapter 6

Background workers let the database keep accepting writes while flushes and compactions run. When the memtable crosses its size threshold, the active WAL is rotated and the memtable snapshot is queued for a background flush. New writes immediately begin in a fresh memtable and a new WAL, so foreground latency stays steady.

Flush jobs write SSTables on a separate goroutine and then remove the rotated WAL once the SSTable is durable. Compactions are also queued in the background and run sequentially, reducing interference with writes and reads. The key idea is simple: slow disk work is moved off the critical path while the in-memory state remains responsive.

This chapter also introduces a manifest. The manifest is a small append-only log that tracks which SSTables exist in each level. On startup, the database replays the manifest to rebuild the table set without scanning the directory. When a flush or compaction completes, the worker records add/remove events in the manifest so metadata stays durable and consistent.

This keeps the system light and predictable without adding concurrency hazards in the core data structures. The worker pipeline is intentionally minimal, but it establishes the shape for more advanced scheduling and prioritization later.
