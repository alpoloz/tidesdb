package tidesdb

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"go.uber.org/zap"
)

type walManager struct {
	db *DB
}

func newWALManager(db *DB) *walManager {
	return &walManager{db: db}
}

func (wm *walManager) Replay() error {
	walFiles, err := wm.listWALFiles()
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
		wm.db.logger.Info("wal replay",
			zap.Int("records", len(records)),
		)
	}
	for _, rec := range records {
		switch rec.op {
		case walOpPut:
			wm.db.insertMemtable(rec.key, entry{value: rec.value}, rec.seq, valueKindPut)
		case walOpDelete:
			wm.db.insertMemtable(rec.key, entry{tombstone: true}, rec.seq, valueKindDelete)
		default:
			return fmt.Errorf("unknown wal op %d", rec.op)
		}
		if rec.seq > wm.db.seq {
			wm.db.seq = rec.seq
		}
	}
	if err := wm.db.meta.StoreSeq(wm.db); err != nil {
		return err
	}
	return nil
}

func (wm *walManager) listWALFiles() ([]string, error) {
	entries, err := os.ReadDir(wm.db.path)
	if err != nil {
		return nil, err
	}
	var walFiles []string
	for _, entry := range entries {
		name := entry.Name()
		if name == "wal.log" || strings.HasPrefix(name, "wal_") && strings.HasSuffix(name, ".log") {
			walFiles = append(walFiles, filepath.Join(wm.db.path, name))
		}
	}
	sort.Strings(walFiles)
	return walFiles, nil
}

func (wm *walManager) LoadID() error {
	entries, err := os.ReadDir(wm.db.path)
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
	wm.db.walID = maxID + 1
	return nil
}
