# Chapter 2

## Changes

- Replaced the memtable map with a skiplist backed by an arena allocator.
- Memtable flush now streams entries in sorted order from the skiplist.

## Why This Design

- **Sorted structure**: Skiplist keeps keys ordered, so flushes no longer need an explicit sort step.
- **Lower GC pressure**: Arena allocation reduces per-entry heap allocations by storing key/value bytes in reusable blocks.
- **Predictable reads**: Skiplist lookups are logarithmic and align with the LSM design used by LevelDB.
- **Better cache locality**: Arena-stored bytes improve locality and reduce pointer churn.

## Notes

- The arena is a simple block allocator; it prioritizes simplicity over reuse or compaction.
- Entry overwrites can leave unused bytes in the arena, which is acceptable for this iteration.
