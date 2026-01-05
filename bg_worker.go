package tidesdb

import (
	"errors"
	"os"
	"sync"

	"go.uber.org/zap"
)

type flushTask struct {
	entries []sstEntry
	walPath string
}

type compactionTask struct {
	level int
}

type bgWorker struct {
	db    *DB
	tasks chan any
	wg    sync.WaitGroup
}

func newBackgroundWorker(db *DB) *bgWorker {
	return &bgWorker{db: db, tasks: make(chan any, 16)}
}

func (bg *bgWorker) Start() {
	bg.wg.Add(1)
	go bg.run()
}

func (bg *bgWorker) Stop() {
	close(bg.tasks)
	bg.wg.Wait()
}

func (bg *bgWorker) EnqueueFlush(entries []sstEntry, walPath string) {
	bg.tasks <- flushTask{entries: entries, walPath: walPath}
}

func (bg *bgWorker) EnqueueCompaction(level int) {
	bg.tasks <- compactionTask{level: level}
}

func (bg *bgWorker) run() {
	defer bg.wg.Done()
	for task := range bg.tasks {
		switch t := task.(type) {
		case flushTask:
			bg.processFlush(t)
		case compactionTask:
			bg.processCompaction(t.level)
		}
	}
}

func (bg *bgWorker) processFlush(task flushTask) {
	bg.db.mu.Lock()
	id := bg.db.nextSSTID
	bg.db.nextSSTID++
	bg.db.mu.Unlock()

	sst, err := createSSTable(bg.db.logger, bg.db.path, 0, id, task.entries)
	if err != nil {
		bg.db.logger.Error("flush failed", zap.Error(err))
		return
	}
	if err := bg.db.manifest.AppendAdd(0, id); err != nil {
		bg.db.logger.Error("manifest append failed", zap.Error(err))
		return
	}

	bg.db.mu.Lock()
	bg.db.sstables[0] = append(bg.db.sstables[0], sst)
	needCompact := len(bg.db.sstables[0]) > bg.db.opts.MaxSSTables
	bg.db.mu.Unlock()

	if err := os.Remove(task.walPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		bg.db.logger.Warn("wal cleanup failed", zap.String("path", task.walPath), zap.Error(err))
	}
	if needCompact {
		bg.EnqueueCompaction(0)
	}
}

func (bg *bgWorker) processCompaction(level int) {
	bg.db.mu.Lock()
	if level >= len(bg.db.sstables) {
		bg.db.mu.Unlock()
		return
	}
	bg.db.levels.Ensure(level + 1)
	pick := bg.db.compactor.PickInputs(level)
	if len(pick) == 0 {
		bg.db.mu.Unlock()
		return
	}
	minKey, maxKey := bg.db.compactor.TableRange(pick)
	overlaps := bg.db.compactor.Overlaps(level+1, minKey, maxKey)
	tables := append([]*sstable(nil), pick...)
	tables = append(tables, overlaps...)
	id := bg.db.nextSSTID
	bg.db.nextSSTID++
	bg.db.mu.Unlock()

	if len(tables) == 0 {
		return
	}

	bg.db.logger.Info("compaction started",
		zap.Int("level", level),
		zap.Int("next_level", level+1),
		zap.Int("inputs", len(pick)),
		zap.Int("overlaps", len(overlaps)),
	)

	minSeq, hasMin := bg.db.snapshots.MinSeq()
	if !hasMin {
		minSeq = 0
	}
	merged, err := mergeSSTables(bg.db.logger, bg.db.path, level+1, id, tables, minSeq, hasMin)
	if err != nil {
		bg.db.logger.Error("compaction failed", zap.Error(err))
		return
	}
	if err := bg.db.manifest.AppendAdd(level+1, id); err != nil {
		bg.db.logger.Error("manifest append failed", zap.Error(err))
		return
	}

	bg.db.mu.Lock()
	bg.db.sstables[level] = bg.db.compactor.RemoveTables(bg.db.sstables[level], pick)
	bg.db.sstables[level+1] = bg.db.compactor.RemoveTables(bg.db.sstables[level+1], overlaps)
	bg.db.sstables[level+1] = append(bg.db.sstables[level+1], merged)
	bg.db.mu.Unlock()

	for _, sst := range tables {
		if err := sst.remove(); err != nil {
			bg.db.logger.Warn("sstable cleanup failed", zap.Error(err))
		}
		if err := bg.db.manifest.AppendRemove(sst.level, sst.id); err != nil {
			bg.db.logger.Warn("manifest remove failed", zap.Error(err))
		}
	}

	bg.db.logger.Info("compaction finished",
		zap.Int("level", level),
		zap.Int("next_level", level+1),
		zap.Uint64("sstable_id", merged.id),
	)
}
