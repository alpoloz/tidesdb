# Chapter 3

SSTables now carry Bloom filters to avoid unnecessary disk reads. A Bloom filter is a compact probabilistic structure that can quickly answer "this key is definitely not here" while allowing a small false-positive rate. In an LSM, that matters because a negative lookup can otherwise probe multiple tables and blocks.

The file ends with a small table footer that points to a metaindex block. The metaindex is a tiny directory mapping names to block ranges; today it records both the Bloom block and the index block locations. This makes the format extensible without hard-coding offsets.

Blocks are the second half of the story. Instead of a single long run of records, each SSTable now groups records into fixed-size blocks, each prefixed with its length. The index block lists the first key of each block and its offset, so a lookup can jump directly to the block that might contain the key and scan a much smaller window.

Together, the metaindex-driven Bloom filter and block layout reduce read amplification in two directions: filters prune whole tables, and block indices narrow the search within a table. The result is a format that stays simple but scales better as more SSTables accumulate.
