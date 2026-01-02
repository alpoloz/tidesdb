# Chapter 4

Leveled compaction organizes SSTables into tiers that get progressively more compact. New flushes land in L0. When a level grows past its limit, a subset of its tables is merged into the next level, rather than collapsing everything at once.

The compaction picker now chooses a table from the current level and computes its key range. Only tables in the next level that overlap that range are pulled into the merge. That keeps compactions focused, reduces write amplification, and preserves the leveled property where deeper levels become more stable and less overlapping.

Reads still consult newer levels first, but the overlap-aware merges make it cheaper to keep older levels compact. This is a foundational version of leveled compaction: it does not yet track file sizes or use a scoring system, but it moves the system away from full-level merges and toward more surgical compactions.
