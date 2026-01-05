package tidesdb

import "sync"

type snapshotManager struct {
	mu      sync.Mutex
	snaps   map[*Snapshot]uint64
	minSeq  uint64
	hasMins bool
}

func newSnapshotManager() *snapshotManager {
	return &snapshotManager{snaps: make(map[*Snapshot]uint64)}
}

func (sm *snapshotManager) New(db *DB, seq uint64) *Snapshot {
	snap := &Snapshot{db: db, seq: seq}
	sm.mu.Lock()
	sm.snaps[snap] = seq
	if !sm.hasMins || seq < sm.minSeq {
		sm.minSeq = seq
		sm.hasMins = true
	}
	sm.mu.Unlock()
	return snap
}

func (sm *snapshotManager) Release(snap *Snapshot) {
	if snap == nil {
		return
	}
	sm.mu.Lock()
	if _, ok := sm.snaps[snap]; !ok {
		sm.mu.Unlock()
		return
	}
	delete(sm.snaps, snap)
	if len(sm.snaps) == 0 {
		sm.hasMins = false
		sm.minSeq = 0
		sm.mu.Unlock()
		return
	}
	min := uint64(^uint64(0))
	for _, seq := range sm.snaps {
		if seq < min {
			min = seq
		}
	}
	sm.minSeq = min
	sm.hasMins = true
	sm.mu.Unlock()
}

func (sm *snapshotManager) MinSeq() (uint64, bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if !sm.hasMins {
		return 0, false
	}
	return sm.minSeq, true
}
