package tidesdb

type compactor struct {
	db *DB
}

func newCompactor(db *DB) *compactor {
	return &compactor{db: db}
}

func (c *compactor) PickInputs(level int) []*sstable {
	if len(c.db.sstables[level]) == 0 {
		return nil
	}
	return []*sstable{c.db.sstables[level][0]}
}

func (c *compactor) Overlaps(level int, minKey, maxKey string) []*sstable {
	if minKey == "" && maxKey == "" {
		return nil
	}
	var overlaps []*sstable
	for _, sst := range c.db.sstables[level] {
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

func (c *compactor) TableRange(tables []*sstable) (string, string) {
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

func (c *compactor) RemoveTables(all []*sstable, remove []*sstable) []*sstable {
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
