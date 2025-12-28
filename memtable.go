package tidesdb

import (
	"bytes"
)

const (
	defaultArenaBlockSize = 4 << 10
	defaultSkiplistHeight = 12
)

type memtable struct {
	list  *skiplist
	arena *arena
}

func newMemtable() *memtable {
	return &memtable{
		list:  newSkiplist(defaultSkiplistHeight),
		arena: newArena(defaultArenaBlockSize),
	}
}

func (m *memtable) set(key string, ent entry) {
	keyBytes := m.arena.putBytes([]byte(key))
	valueBytes := []byte(nil)
	if len(ent.value) > 0 {
		valueBytes = m.arena.putBytes(ent.value)
	}
	m.list.insertOrUpdate(keyBytes, valueBytes, ent.tombstone)
}

func (m *memtable) get(key string) (entry, bool) {
	keyBytes := []byte(key)
	node := m.list.find(keyBytes)
	if node == nil {
		return entry{}, false
	}
	return entry{value: node.value, tombstone: node.tombstone}, true
}

func (m *memtable) len() int {
	return m.list.len()
}

func (m *memtable) bytesUsed() int {
	return m.arena.used()
}

func (m *memtable) entries() []sstEntry {
	entries := make([]sstEntry, 0, m.list.len())
	for node := m.list.head.next[0]; node != nil; node = node.next[0] {
		entries = append(entries, sstEntry{
			key:   string(node.key),
			entry: entry{value: node.value, tombstone: node.tombstone},
		})
	}
	return entries
}

type arena struct {
	blocks    [][]byte
	blockSize int
	offset    int
	usedBytes int
}

func newArena(blockSize int) *arena {
	if blockSize <= 0 {
		blockSize = defaultArenaBlockSize
	}
	return &arena{blockSize: blockSize}
}

func (a *arena) alloc(n int) []byte {
	if n <= 0 {
		return nil
	}
	if len(a.blocks) == 0 || a.offset+n > len(a.blocks[len(a.blocks)-1]) {
		size := a.blockSize
		if n > size {
			size = n
		}
		a.blocks = append(a.blocks, make([]byte, size))
		a.offset = 0
	}
	block := a.blocks[len(a.blocks)-1]
	start := a.offset
	a.offset += n
	a.usedBytes += n
	return block[start:a.offset:a.offset]
}

func (a *arena) putBytes(data []byte) []byte {
	buf := a.alloc(len(data))
	copy(buf, data)
	return buf
}

func (a *arena) used() int {
	return a.usedBytes
}

type skiplist struct {
	head      *skiplistNode
	maxHeight int
	height    int
	rnd       uint32
	length    int
}

type skiplistNode struct {
	key       []byte
	value     []byte
	tombstone bool
	next      []*skiplistNode
}

func newSkiplist(maxHeight int) *skiplist {
	if maxHeight <= 0 {
		maxHeight = defaultSkiplistHeight
	}
	head := &skiplistNode{next: make([]*skiplistNode, maxHeight)}
	return &skiplist{head: head, maxHeight: maxHeight, height: 1, rnd: 0xdeadbeef}
}

func (s *skiplist) len() int {
	return s.length
}

func (s *skiplist) find(key []byte) *skiplistNode {
	x := s.head
	for level := s.height - 1; level >= 0; level-- {
		for next := x.next[level]; next != nil && bytes.Compare(next.key, key) < 0; next = x.next[level] {
			x = next
		}
	}
	x = x.next[0]
	if x != nil && bytes.Equal(x.key, key) {
		return x
	}
	return nil
}

func (s *skiplist) insertOrUpdate(key []byte, value []byte, tombstone bool) {
	update := make([]*skiplistNode, s.maxHeight)
	x := s.head
	for level := s.height - 1; level >= 0; level-- {
		for next := x.next[level]; next != nil && bytes.Compare(next.key, key) < 0; next = x.next[level] {
			x = next
		}
		update[level] = x
	}
	x = x.next[0]
	if x != nil && bytes.Equal(x.key, key) {
		x.value = value
		x.tombstone = tombstone
		return
	}

	nodeHeight := s.randomHeight()
	if nodeHeight > s.height {
		for level := s.height; level < nodeHeight; level++ {
			update[level] = s.head
		}
		s.height = nodeHeight
	}

	node := &skiplistNode{
		key:       key,
		value:     value,
		tombstone: tombstone,
		next:      make([]*skiplistNode, nodeHeight),
	}
	for level := 0; level < nodeHeight; level++ {
		node.next[level] = update[level].next[level]
		update[level].next[level] = node
	}
	s.length++
}

func (s *skiplist) randomHeight() int {
	height := 1
	for height < s.maxHeight && s.rand()%4 == 0 {
		height++
	}
	return height
}

func (s *skiplist) rand() uint32 {
	s.rnd = s.rnd*1664525 + 1013904223
	return s.rnd
}
