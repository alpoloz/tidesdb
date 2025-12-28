# Chapter 2

The memtable is the staging area of an LSM-tree. It absorbs writes at memory speed, but it also defines how expensive a flush will be. A plain map is easy to code, yet it leaves keys unordered and forces a full sort whenever an SSTable is built.

This chapter replaces the map with a skiplist backed by a small arena allocator. A skiplist maintains keys in sorted order as they arrive, so a flush can stream entries directly to disk without an extra sorting pass. That aligns with the LSM principle of sequential output and reduces the cost of turning memory into an SSTable.

The arena allocator is a pragmatic memory trick: instead of allocating new byte slices for each key/value pair, bytes are copied into preallocated blocks. This reduces GC pressure and keeps memory layout tighter, which is useful once the write rate grows. It is not perfect—overwrites can leave unused bytes behind—but the tradeoff is clear and predictable.

At this point the memtable looks and behaves like the component described in LSM literature: ordered, write-optimized, and designed to flush efficiently. That sets the stage for richer SSTable features and more nuanced compaction down the line.
