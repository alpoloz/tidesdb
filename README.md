# tidesdb

Simple, persistent key-value store powered by an LSM-tree architecture. Designed as a compact, readable implementation with WAL + memtable + immutable SSTables with compaction.

## Architecture

This implementation mirrors the core LSM flow while keeping the code intentionally small and readable.

1. **WAL (write-ahead log)**: Every `Put` and `Delete` is appended to `wal.log` before touching memory. On restart, the WAL is replayed to rebuild the memtable.
2. **Memtable**: In-memory map storing the latest key state (value or tombstone). When it grows beyond a threshold, it is flushed to disk.
3. **SSTables**: Immutable sorted tables on disk (`sst_*.dat`) with a simple in-memory index (`sst_*.idx`) mapping keys to offsets for point lookups.
4. **Compaction**: When the number of SSTables exceeds the configured limit, tables are merged into one new SSTable, preserving the newest value and discarding tombstones.

Data lookup order is memtable -> newest SSTable -> oldest SSTable.

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
