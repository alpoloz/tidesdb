package tidesdb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
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
	bgTasks   chan any
	bgWG      sync.WaitGroup
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

	db := &DB{
		path:     path,
		opts:     options,
		memtable: newMemtable(),
		wal:      w,
		logger:   logger,
		sstables: make([][]*sstable, options.MaxLevels),
		bgTasks:  make(chan any, 16),
	}

	db.bgWG.Add(1)
	go db.runBackground()

	if err := db.loadSeq(); err != nil {
		_ = w.Close()
		return nil, err
	}
	if err := db.loadWALID(); err != nil {
		_ = w.Close()
		return nil, err
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
		zap.Int("sstables", db.totalSSTables()),
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
	if err := db.storeSeq(); err != nil {
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
	if err := db.storeSeq(); err != nil {
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
	sstables := db.snapshotSSTables()
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
	if db.bgTasks != nil {
		close(db.bgTasks)
		db.bgTasks = nil
	}
	db.mu.Unlock()

	db.bgWG.Wait()
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.wal.Close()
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

	db.enqueueTask(flushTask{entries: entries, walPath: rotatedPath})

	if len(db.sstables[0]) > db.opts.MaxSSTables {
		db.enqueueTask(compactionTask{level: 0})
	}

	return nil
}

func (db *DB) loadSSTables() error {
	entries, err := os.ReadDir(db.path)
	if err != nil {
		return err
	}

	byLevel := map[int][]uint64{}
	for _, entry := range entries {
		level, id, ok := parseSSTableName(entry.Name())
		if ok {
			byLevel[level] = append(byLevel[level], id)
		}
	}

	for level, ids := range byLevel {
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		db.ensureLevel(level)
		for _, id := range ids {
			sst, err := loadSSTable(db.logger, db.path, level, id)
			if err != nil {
				return err
			}
			db.sstables[level] = append(db.sstables[level], sst)
			db.nextSSTID = max(db.nextSSTID, id+1)
		}
	}
	if len(byLevel) > 0 {
		db.logger.Info("sstables loaded", zap.Int("count", db.totalSSTables()))
	}
	return nil
}

func (db *DB) replayWAL() error {
	walFiles, err := db.listWALFiles()
	if err != nil {
		return err
	}

	var records []walRecord
	for _, path := range walFiles {
		recs, err := readWALRecords(path)
		if err != nil {
			return err
		}
		records = append(records, recs...)
	}
	if len(records) > 0 {
		db.logger.Info("wal replay",
			zap.Int("records", len(records)),
		)
	}
	for _, rec := range records {
		switch rec.op {
		case walOpPut:
			db.insertMemtable(rec.key, entry{value: rec.value}, rec.seq, valueKindPut)
		case walOpDelete:
			db.insertMemtable(rec.key, entry{tombstone: true}, rec.seq, valueKindDelete)
		default:
			return fmt.Errorf("unknown wal op %d", rec.op)
		}
		if rec.seq > db.seq {
			db.seq = rec.seq
		}
	}
	if err := db.storeSeq(); err != nil {
		return err
	}
	return nil
}

func (db *DB) ensureLevel(level int) {
	if level < len(db.sstables) {
		return
	}
	needed := level + 1
	for len(db.sstables) < needed {
		db.sstables = append(db.sstables, nil)
	}
}

func (db *DB) totalSSTables() int {
	total := 0
	for _, level := range db.sstables {
		total += len(level)
	}
	return total
}

func (db *DB) snapshotSSTables() [][]*sstable {
	result := make([][]*sstable, len(db.sstables))
	for i := range db.sstables {
		result[i] = append([]*sstable(nil), db.sstables[i]...)
	}
	return result
}

type flushTask struct {
	entries []sstEntry
	walPath string
}

type compactionTask struct {
	level int
}

func (db *DB) enqueueTask(task any) {
	if db.bgTasks == nil {
		return
	}
	db.bgTasks <- task
}

func (db *DB) runBackground() {
	defer db.bgWG.Done()
	for task := range db.bgTasks {
		switch t := task.(type) {
		case flushTask:
			db.processFlush(t)
		case compactionTask:
			db.processCompaction(t.level)
		}
	}
}

func (db *DB) processFlush(task flushTask) {
	db.mu.Lock()
	id := db.nextSSTID
	db.nextSSTID++
	db.mu.Unlock()

	sst, err := createSSTable(db.logger, db.path, 0, id, task.entries)
	if err != nil {
		db.logger.Error("flush failed", zap.Error(err))
		return
	}

	db.mu.Lock()
	db.sstables[0] = append(db.sstables[0], sst)
	needCompact := len(db.sstables[0]) > db.opts.MaxSSTables
	db.mu.Unlock()

	if err := os.Remove(task.walPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		db.logger.Warn("wal cleanup failed", zap.String("path", task.walPath), zap.Error(err))
	}
	if needCompact {
		db.enqueueTask(compactionTask{level: 0})
	}
}

func (db *DB) processCompaction(level int) {
	db.mu.Lock()
	if level >= len(db.sstables) {
		db.mu.Unlock()
		return
	}
	db.ensureLevel(level + 1)
	pick := db.pickCompactionInputs(level)
	if len(pick) == 0 {
		db.mu.Unlock()
		return
	}
	minKey, maxKey := tableRange(pick)
	overlaps := db.overlappingTables(level+1, minKey, maxKey)
	tables := append([]*sstable(nil), pick...)
	tables = append(tables, overlaps...)
	id := db.nextSSTID
	db.nextSSTID++
	db.mu.Unlock()

	if len(tables) == 0 {
		return
	}

	db.logger.Info("compaction started",
		zap.Int("level", level),
		zap.Int("next_level", level+1),
		zap.Int("inputs", len(pick)),
		zap.Int("overlaps", len(overlaps)),
	)

	merged, err := mergeSSTables(db.logger, db.path, level+1, id, tables)
	if err != nil {
		db.logger.Error("compaction failed", zap.Error(err))
		return
	}

	db.mu.Lock()
	db.sstables[level] = removeTables(db.sstables[level], pick)
	db.sstables[level+1] = removeTables(db.sstables[level+1], overlaps)
	db.sstables[level+1] = append(db.sstables[level+1], merged)
	db.mu.Unlock()

	for _, sst := range tables {
		if err := sst.remove(); err != nil {
			db.logger.Warn("sstable cleanup failed", zap.Error(err))
		}
	}

	db.logger.Info("compaction finished",
		zap.Int("level", level),
		zap.Int("next_level", level+1),
		zap.Uint64("sstable_id", merged.id),
	)
}

func (db *DB) listWALFiles() ([]string, error) {
	entries, err := os.ReadDir(db.path)
	if err != nil {
		return nil, err
	}
	var walFiles []string
	for _, entry := range entries {
		name := entry.Name()
		if name == "wal.log" || strings.HasPrefix(name, "wal_") && strings.HasSuffix(name, ".log") {
			walFiles = append(walFiles, filepath.Join(db.path, name))
		}
	}
	sort.Strings(walFiles)
	return walFiles, nil
}

func (db *DB) loadWALID() error {
	entries, err := os.ReadDir(db.path)
	if err != nil {
		return err
	}
	var maxID uint64
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, "wal_") && strings.HasSuffix(name, ".log") {
			base := strings.TrimSuffix(strings.TrimPrefix(name, "wal_"), ".log")
			id, err := strconv.ParseUint(base, 10, 64)
			if err == nil && id > maxID {
				maxID = id
			}
		}
	}
	db.walID = maxID + 1
	return nil
}

func (db *DB) loadSeq() error {
	path := filepath.Join(db.path, "seq.meta")
	seq, err := readSeqMeta(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	db.seq = seq
	return nil
}

func (db *DB) storeSeq() error {
	path := filepath.Join(db.path, "seq.meta")
	return writeSeqMeta(path, db.seq)
}

func (db *DB) pickCompactionInputs(level int) []*sstable {
	if len(db.sstables[level]) == 0 {
		return nil
	}
	return []*sstable{db.sstables[level][0]}
}

func (db *DB) overlappingTables(level int, minKey, maxKey string) []*sstable {
	if minKey == "" && maxKey == "" {
		return nil
	}
	var overlaps []*sstable
	for _, sst := range db.sstables[level] {
		if rangesOverlap(minKey, maxKey, sst.minKey, sst.maxKey) {
			overlaps = append(overlaps, sst)
		}
	}
	return overlaps
}

func rangesOverlap(minA, maxA, minB, maxB string) bool {
	if minA == "" && maxA == "" {
		return false
	}
	if minB == "" && maxB == "" {
		return false
	}
	if maxA < minB || maxB < minA {
		return false
	}
	return true
}

func tableRange(tables []*sstable) (string, string) {
	if len(tables) == 0 {
		return "", ""
	}
	minKey := ""
	maxKey := ""
	for _, sst := range tables {
		if sst.minKey == "" && sst.maxKey == "" {
			continue
		}
		if minKey == "" || sst.minKey < minKey {
			minKey = sst.minKey
		}
		if maxKey == "" || sst.maxKey > maxKey {
			maxKey = sst.maxKey
		}
	}
	return minKey, maxKey
}

func removeTables(all []*sstable, remove []*sstable) []*sstable {
	if len(remove) == 0 {
		return all
	}
	removeSet := make(map[*sstable]struct{}, len(remove))
	for _, sst := range remove {
		removeSet[sst] = struct{}{}
	}
	out := make([]*sstable, 0, len(all))
	for _, sst := range all {
		if _, ok := removeSet[sst]; ok {
			continue
		}
		out = append(out, sst)
	}
	return out
}
