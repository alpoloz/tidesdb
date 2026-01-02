# Chapter 4

Multi-level compaction moves the system beyond "merge everything" behavior. Instead of collapsing all tables into one, SSTables are organized into levels (L0, L1, L2, ...), each holding a progressively more compacted view of the data. When a level grows past its limit, its tables are merged into the next level, and the process continues downstream only as needed.

In this implementation, new flushes land in L0. If L0 exceeds the configured table count, the tables in L0 and L1 are merged into a single new SSTable in L1. If L1 becomes too large, the merge continues into L2, and so on. This keeps write amplification controlled while avoiding the full stop-the-world merge of all tables.

The file layout reflects this structure: tables are named with a level prefix (`sst_L{level}_{id}.*`), which makes it easy to load level metadata on startup. Reads still check newer tables first, but the leveled layout sets the stage for more nuanced policies later (overlap tracking, size-based targets, and background compaction).

This is still a deliberately simple version of leveled compaction, but it establishes the right shape for a more LevelDB-like strategy.
