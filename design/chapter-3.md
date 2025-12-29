# Chapter 3

SSTables now carry Bloom filters to avoid unnecessary disk reads. A Bloom filter is a compact probabilistic structure that can quickly answer “this key is definitely not here” while allowing a small false-positive rate. In an LSM, that matters because a negative lookup can otherwise probe multiple tables and blocks.

The filter is built as SSTables are written. Every key is hashed into a bitset, and the bitset is stored alongside the table in a small `.bloom` file. On reads, the lookup checks the Bloom filter first; if the filter says “no,” the table is skipped entirely. If it says “maybe,” the search continues into the block index and data blocks.

Blocks are the second half of the story. Instead of a single long run of records, each SSTable now groups records into fixed-size blocks, each prefixed with its length. The table index points to the first key of each block, so a lookup can jump directly to the block that might contain the key and scan a much smaller window.

Together, Bloom filters and blocks reduce read amplification in two directions: filters prune whole tables, and block indices narrow the search within a table. The result is a format that stays simple but scales better as more SSTables accumulate.
