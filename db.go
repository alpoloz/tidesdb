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
	defaultMaxLevels        = 3
)

var ErrNotFound = errors.New("key not found")

type entry struct {
	value     []byte
	tombstone bool
}

type Options struct {
	MemtableMaxBytes int
	MaxSSTables      int
	MaxLevels        int
}

type DB struct {
	mu sync.RWMutex

	path      string
	opts      Options
	memtable  *memtable
	memBytes  int
	wal       *wal
	walID     uint64
	sstables  [][]*sstable
	nextSSTID uint64
	seq       uint64
	logger    *zap.Logger
	bg        *bgWorker
	walMgr    *walManager
	levels    *levelManager
	meta      *metaStore
	compactor *compactor
	manifest  *manifestStore
}

func Open(logger *zap.Logger, path string, opts *Options) (*DB, error) {
	if path == "" {
		return nil, errors.New("path required")
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	options := Options{
		MemtableMaxBytes: defaultMemtableMaxBytes,
		MaxSSTables:      defaultMaxSSTables,
		MaxLevels:        defaultMaxLevels,
	}
	if opts != nil {
		if opts.MemtableMaxBytes > 0 {
			options.MemtableMaxBytes = opts.MemtableMaxBytes
		}
		if opts.MaxSSTables > 0 {
			options.MaxSSTables = opts.MaxSSTables
		}
		if opts.MaxLevels > 0 {
			options.MaxLevels = opts.MaxLevels
		}
	}

	if err := os.MkdirAll(path, 0o755); err != nil {
		return nil, err
	}

	w, err := openWAL(logger, filepath.Join(path, "wal.log"))
	if err != nil {
		return nil, err
	}

	manifest, err := newManifestStore(path)
	if err != nil {
		_ = w.Close()
		return nil, err
	}

	db := &DB{
		path:     path,
		opts:     options,
		memtable: newMemtable(),
		wal:      w,
		logger:   logger,
		sstables: make([][]*sstable, options.MaxLevels),
		manifest: manifest,
	}

	db.meta = newMetaStore(path)
	db.levels = newLevelManager(db)
	db.compactor = newCompactor(db)
	db.walMgr = newWALManager(db)
	db.bg = newBackgroundWorker(db)
	db.bg.Start()

	if err := db.meta.LoadSeq(db); err != nil {
		_ = w.Close()
		return nil, err
	}
	if err := db.walMgr.LoadID(); err != nil {
		_ = w.Close()
		return nil, err
	}
	if err := db.loadSSTables(); err != nil {
		_ = w.Close()
		_ = manifest.Close()
		return nil, err
	}
	if err := db.walMgr.Replay(); err != nil {
		_ = w.Close()
		_ = manifest.Close()
		return nil, err
	}

	db.logger.Info("db opened",
		zap.String("path", path),
		zap.Int("sstables", db.levels.Total()),
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

	seq := db.seq + 1
	if err := db.wal.appendRecord(walOpPut, seq, key, valCopy); err != nil {
		return err
	}
	db.seq = seq
	if err := db.meta.StoreSeq(db); err != nil {
		return err
	}
	db.insertMemtable(key, entry{value: valCopy}, seq, valueKindPut)

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

	seq := db.seq + 1
	if err := db.wal.appendRecord(walOpDelete, seq, key, nil); err != nil {
		return err
	}
	db.seq = seq
	if err := db.meta.StoreSeq(db); err != nil {
		return err
	}
	db.insertMemtable(key, entry{tombstone: true}, seq, valueKindDelete)

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
	sstables := db.levels.Snapshot()
	db.mu.RUnlock()

	for _, level := range sstables {
		for i := len(level) - 1; i >= 0; i-- {
			ent, ok, err := level[i].get(key)
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
	}

	return nil, ErrNotFound
}

func (db *DB) Close() error {
	db.mu.Lock()
	if db.memtable.len() > 0 {
		if err := db.flushLocked(); err != nil {
			db.mu.Unlock()
			return err
		}
	}
	db.mu.Unlock()

	if db.bg != nil {
		db.bg.Stop()
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.wal.Close(); err != nil {
		return err
	}
	if db.manifest != nil {
		return db.manifest.Close()
	}
	return nil
}

func (db *DB) insertMemtable(key string, ent entry, seq uint64, kind valueKind) {
	internalKey := encodeInternalKey(key, seq, kind)
	db.memtable.set(internalKey, ent)
	db.memBytes = db.memtable.bytesUsed()
}

func (db *DB) flushLocked() error {
	if db.memtable.len() == 0 {
		return nil
	}

	entries := db.memtable.entries()
	walPath := db.wal.path
	rotatedPath := filepath.Join(db.path, fmt.Sprintf("wal_%06d.log", db.walID))
	db.walID++

	db.logger.Info("flushing memtable",
		zap.Int("entries", len(entries)),
		zap.Int("mem_bytes", db.memBytes),
	)

	if err := db.wal.Close(); err != nil {
		return err
	}
	if err := os.Rename(walPath, rotatedPath); err != nil {
		return err
	}
	w, err := openWAL(db.logger, filepath.Join(db.path, "wal.log"))
	if err != nil {
		return err
	}
	db.wal = w

	db.memtable = newMemtable()
	db.memBytes = 0

	db.bg.EnqueueFlush(entries, rotatedPath)

	if len(db.sstables[0]) > db.opts.MaxSSTables {
		db.bg.EnqueueCompaction(0)
	}

	return nil
}

func (db *DB) loadSSTables() error {
	levels, err := db.manifest.Load()
	if err != nil {
		return err
	}

	if len(levels) == 0 {
		levels, err = db.scanSSTables()
		if err != nil {
			return err
		}
		for level, ids := range levels {
			for _, id := range ids {
				if err := db.manifest.AppendAdd(level, id); err != nil {
					return err
				}
			}
		}
	}

	for level, ids := range levels {
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		db.levels.Ensure(level)
		for _, id := range ids {
			sst, err := loadSSTable(db.logger, db.path, level, id)
			if err != nil {
				return err
			}
			db.sstables[level] = append(db.sstables[level], sst)
			db.nextSSTID = max(db.nextSSTID, id+1)
		}
	}
	if len(levels) > 0 {
		db.logger.Info("sstables loaded", zap.Int("count", db.levels.Total()))
	}
	return nil
}

func (db *DB) scanSSTables() (map[int][]uint64, error) {
	entries, err := os.ReadDir(db.path)
	if err != nil {
		return nil, err
	}
	levels := map[int][]uint64{}
	for _, entry := range entries {
		level, id, ok := parseSSTableName(entry.Name())
		if ok {
			levels[level] = append(levels[level], id)
		}
	}
	return levels, nil
}
