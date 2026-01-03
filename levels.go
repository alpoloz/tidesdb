package tidesdb

type levelManager struct {
	db *DB
}

func newLevelManager(db *DB) *levelManager {
	return &levelManager{db: db}
}

func (lm *levelManager) Ensure(level int) {
	if level < len(lm.db.sstables) {
		return
	}
	needed := level + 1
	for len(lm.db.sstables) < needed {
		lm.db.sstables = append(lm.db.sstables, nil)
	}
}

func (lm *levelManager) Total() int {
	total := 0
	for _, level := range lm.db.sstables {
		total += len(level)
	}
	return total
}

func (lm *levelManager) Snapshot() [][]*sstable {
	result := make([][]*sstable, len(lm.db.sstables))
	for i := range lm.db.sstables {
		result[i] = append([]*sstable(nil), lm.db.sstables[i]...)
	}
	return result
}
