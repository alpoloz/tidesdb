package tidesdb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"go.uber.org/zap"
)

const (
	defaultMemtableMaxBytes = 4 << 20
	defaultMaxSSTables      = 4
)

var ErrNotFound = errors.New("key not found")

type entry struct {
	value     []byte
	tombstone bool
}

type Options struct {
	MemtableMaxBytes int
	MaxSSTables      int
}

type DB struct {
	mu sync.RWMutex

	path      string
	opts      Options
	memtable  *memtable
	memBytes  int
	wal       *wal
	sstables  []*sstable
	nextSSTID uint64
	logger    *zap.Logger
}

func Open(path string, logger *zap.Logger, opts *Options) (*DB, error) {
	if path == "" {
		return nil, errors.New("path required")
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	options := Options{
		MemtableMaxBytes: defaultMemtableMaxBytes,
		MaxSSTables:      defaultMaxSSTables,
	}
	if opts != nil {
		if opts.MemtableMaxBytes > 0 {
			options.MemtableMaxBytes = opts.MemtableMaxBytes
		}
		if opts.MaxSSTables > 0 {
			options.MaxSSTables = opts.MaxSSTables
		}
	}

	if err := os.MkdirAll(path, 0o755); err != nil {
		return nil, err
	}

	w, err := openWAL(filepath.Join(path, "wal.log"), logger)
	if err != nil {
		return nil, err
	}

	db := &DB{
		path:     path,
		opts:     options,
		memtable: newMemtable(),
		wal:      w,
		logger:   logger,
	}

	if err := db.loadSSTables(); err != nil {
		_ = w.Close()
		return nil, err
	}
	if err := db.replayWAL(); err != nil {
		_ = w.Close()
		return nil, err
	}

	db.logger.Info("db opened",
		zap.String("path", path),
		zap.Int("sstables", len(db.sstables)),
		zap.Int("memtable_entries", db.memtable.len()),
	)

	return db, nil
}

func (db *DB) Put(key string, value []byte) error {
	if key == "" {
		return errors.New("key required")
	}
	valCopy := append([]byte(nil), value...)

	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.wal.appendRecord(walOpPut, key, valCopy); err != nil {
		return err
	}
	db.insertMemtable(key, entry{value: valCopy})

	if db.memBytes >= db.opts.MemtableMaxBytes {
		return db.flushLocked()
	}
	return nil
}

func (db *DB) Delete(key string) error {
	if key == "" {
		return errors.New("key required")
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.wal.appendRecord(walOpDelete, key, nil); err != nil {
		return err
	}
	db.insertMemtable(key, entry{tombstone: true})

	if db.memBytes >= db.opts.MemtableMaxBytes {
		return db.flushLocked()
	}
	return nil
}

func (db *DB) Get(key string) ([]byte, error) {
	if key == "" {
		return nil, errors.New("key required")
	}

	db.mu.RLock()
	if ent, ok := db.memtable.get(key); ok {
		if ent.tombstone {
			db.mu.RUnlock()
			return nil, ErrNotFound
		}
		val := append([]byte(nil), ent.value...)
		db.mu.RUnlock()
		return val, nil
	}
	sstables := append([]*sstable(nil), db.sstables...)
	db.mu.RUnlock()

	for i := len(sstables) - 1; i >= 0; i-- {
		ent, ok, err := sstables[i].get(key)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		if ent.tombstone {
			return nil, ErrNotFound
		}
		return append([]byte(nil), ent.value...), nil
	}

	return nil, ErrNotFound
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.wal.Close()
}

func (db *DB) insertMemtable(key string, ent entry) {
	db.memtable.set(key, ent)
	db.memBytes = db.memtable.bytesUsed()
}

func (db *DB) flushLocked() error {
	if db.memtable.len() == 0 {
		return nil
	}

	entries := db.memtable.entries()

	id := db.nextSSTID
	db.nextSSTID++

	db.logger.Info("flushing memtable",
		zap.Uint64("sstable_id", id),
		zap.Int("entries", len(entries)),
		zap.Int("mem_bytes", db.memBytes),
	)

	sst, err := createSSTable(db.path, id, entries, db.logger)
	if err != nil {
		return err
	}
	if err := db.wal.reset(); err != nil {
		return err
	}

	db.sstables = append(db.sstables, sst)
	db.memtable = newMemtable()
	db.memBytes = 0

	if len(db.sstables) > db.opts.MaxSSTables {
		if err := db.compactLocked(); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) compactLocked() error {
	if len(db.sstables) < 2 {
		return nil
	}

	db.logger.Info("compaction started",
		zap.Int("sstables", len(db.sstables)),
	)

	merged, err := mergeSSTables(db.path, db.nextSSTID, db.sstables, db.logger)
	if err != nil {
		return err
	}
	for _, sst := range db.sstables {
		if err := sst.remove(); err != nil {
			return err
		}
	}
	db.sstables = []*sstable{merged}
	db.nextSSTID++

	db.logger.Info("compaction finished",
		zap.Uint64("sstable_id", merged.id),
	)

	return nil
}

func (db *DB) loadSSTables() error {
	entries, err := os.ReadDir(db.path)
	if err != nil {
		return err
	}

	var ids []uint64
	for _, entry := range entries {
		id, ok := parseSSTableID(entry.Name())
		if ok {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	for _, id := range ids {
		sst, err := loadSSTable(db.path, id, db.logger)
		if err != nil {
			return err
		}
		db.sstables = append(db.sstables, sst)
		db.nextSSTID = max(db.nextSSTID, id+1)
	}
	if len(ids) > 0 {
		db.logger.Info("sstables loaded", zap.Int("count", len(ids)))
	}
	return nil
}

func (db *DB) replayWAL() error {
	records, err := db.wal.readAll()
	if err != nil {
		return err
	}
	if len(records) > 0 {
		db.logger.Info("wal replay",
			zap.Int("records", len(records)),
		)
	}
	for _, rec := range records {
		switch rec.op {
		case walOpPut:
			db.insertMemtable(rec.key, entry{value: rec.value})
		case walOpDelete:
			db.insertMemtable(rec.key, entry{tombstone: true})
		default:
			return fmt.Errorf("unknown wal op %d", rec.op)
		}
	}
	return nil
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
