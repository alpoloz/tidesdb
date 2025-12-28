# Chapter 1

## Changes

- Built a minimal LSM-tree key-value store with WAL, memtable, SSTables, and compaction.
- Added a small demo program in `cmd/demo`.
- Expanded `README.md` with architecture notes and usage samples.
- Added `.gitignore` for the `data/` directory.

## Why This Design

- **WAL + memtable**: Ensures durability while keeping writes fast and sequential; replay keeps recovery simple.
- **Sorted SSTables**: Immutable files make reads predictable and compaction straightforward; the on-disk format is intentionally simple and easy to inspect.
- **In-memory index per SSTable**: Trades a bit of memory for very fast point lookups without needing an on-disk tree.
- **Size-based compaction**: Keeps the number of SSTables bounded with minimal logic, suitable for a first iteration.
- **Go map memtable**: Simplifies the implementation and lets us focus on persistence and format choices before optimizing with a skiplist.

## Notes

- This is a teaching/learning baseline; expect to revisit on-disk formats, indexing, and compaction strategy as features grow.
