# Next Steps

- Add a versioned footer/header to SSTables to support format evolution.
- Explore compaction strategies (size-tiered, leveled, or hybrid).
- Add transactions and consistent read views (snapshots).
- Add write batches for atomic multi-key writes.
- Introduce sequence numbers and an internal key format (user key + sequence + type).
- Add a manifest/version set to track table metadata atomically.
- Add block restart points with prefix compression for on-disk keys.
- Compress data blocks (snappy/lz4).
- Add checksums/CRC for blocks and record validation on reads.
- Embed bloom filters in a metaindex block rather than a footer-only approach.
- Add iterators and a merging iterator for range scans.
- Add a table cache with LRU eviction and open-file reuse.
- Add background flush/compaction workers.
- Add file locking and repair tooling.
- Add configurable cache sizes, bloom bits, and block size.
- Add optional WAL sync batching.
