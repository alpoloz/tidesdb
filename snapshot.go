package tidesdb

type Snapshot struct {
	db  *DB
	seq uint64
}

func (db *DB) NewSnapshot() *Snapshot {
	db.mu.RLock()
	seq := db.seq
	db.mu.RUnlock()
	if db.snapshots == nil {
		return &Snapshot{db: db, seq: seq}
	}
	return db.snapshots.New(db, seq)
}

func (s *Snapshot) Get(key string) ([]byte, error) {
	if s == nil || s.db == nil {
		return nil, ErrNotFound
	}
	return s.db.GetAt(key, s.seq)
}

func (s *Snapshot) Sequence() uint64 {
	if s == nil {
		return 0
	}
	return s.seq
}

func (s *Snapshot) Release() {
	if s == nil || s.db == nil || s.db.snapshots == nil {
		return
	}
	s.db.snapshots.Release(s)
}
