# tidesdb

Simple, persistent key-value store powered by an LSM-tree architecture. Designed as a compact, readable implementation with WAL + memtable + immutable SSTables with compaction.

## Architecture

This implementation mirrors the core LSM flow while keeping the code intentionally small and readable.

1. **WAL (write-ahead log)**: Every `Put` and `Delete` is appended to `wal.log` (with `fsync`) before touching memory. During flush, `wal.log` is rotated (`wal_*.log`) and replay on startup loads all WAL files in order.
2. **Memtable**: In-memory skiplist (ordered by internal key: user key + sequence + kind) backed by an arena allocator for key/value bytes. This preserves sorted order for flush and supports snapshot reads.
3. **SSTables**: Immutable sorted tables on disk (`sst_L<level>_<id>.dat`). Each file embeds block data, block index, bloom filter, and metaindex/footer; index and bloom are loaded in memory for point lookups.
4. **Compaction**: Leveled, overlap-aware compaction. A picker chooses input table(s) from one level plus overlapping tables from the next level, then merges and prunes old versions/tombstones with snapshot awareness.
5. **Manifest**: A durable append-only `MANIFEST` tracks table add/remove events so startup can rebuild level state without relying on directory scans.

Data lookup order is memtable -> pending flush batches -> SSTables (newest to oldest within each level).

## Features

- Durable writes via WAL + `fsync`
- Memtable flush to sorted SSTables
- Immutable SSTables with in-memory index for fast point reads
- Basic size-based compaction

## Samples

### Basic usage

```go
package main

import (
	"fmt"
	"log"

	"tidesdb"
	"go.uber.org/zap"
)

func main() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = logger.Sync()
	}()

	db, err := tidesdb.Open(logger, "data", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := db.Put("hello", []byte("world")); err != nil {
		log.Fatal(err)
	}

	value, err := db.Get("hello")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(value))

	if err := db.Delete("hello"); err != nil {
		log.Fatal(err)
	}
}
```

### Snapshot read

```go
snap := db.NewSnapshot()
defer snap.Release()

_ = db.Put("key", []byte("v1"))
_ = db.Put("key", []byte("v2"))

value, err := snap.Get("key")
if err != nil {
	log.Fatal(err)
}
fmt.Println(string(value))
```

### Custom options

```go
logger, err := zap.NewDevelopment()
if err != nil {
	log.Fatal(err)
}
defer func() {
	_ = logger.Sync()
}()

opts := &tidesdb.Options{
	MemtableMaxBytes: 1 << 20,
	MaxSSTables:      2,
}
db, err := tidesdb.Open(logger, "data", opts)
if err != nil {
	log.Fatal(err)
}
defer db.Close()
```
