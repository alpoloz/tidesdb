package tidesdb

import (
	"encoding/binary"
	"hash/fnv"
	"math"
)

type bloomFilter struct {
	m    uint32
	k    uint32
	bits []byte
}

func newBloomFilter(n int, bitsPerKey int) *bloomFilter {
	if n <= 0 {
		n = 1
	}
	if bitsPerKey <= 0 {
		bitsPerKey = 10
	}
	m := uint32(n * bitsPerKey)
	if m < 8 {
		m = 8
	}
	k := uint32(math.Round(math.Ln2 * float64(bitsPerKey)))
	if k < 1 {
		k = 1
	}
	bytes := make([]byte, (m+7)/8)
	return &bloomFilter{m: m, k: k, bits: bytes}
}

func (b *bloomFilter) add(key string) {
	h1, h2 := hashKey(key)
	for i := uint32(0); i < b.k; i++ {
		pos := (h1 + uint64(i)*h2) % uint64(b.m)
		idx := pos / 8
		bit := uint8(1 << (pos % 8))
		b.bits[idx] |= bit
	}
}

func (b *bloomFilter) mayContain(key string) bool {
	h1, h2 := hashKey(key)
	for i := uint32(0); i < b.k; i++ {
		pos := (h1 + uint64(i)*h2) % uint64(b.m)
		idx := pos / 8
		bit := uint8(1 << (pos % 8))
		if b.bits[idx]&bit == 0 {
			return false
		}
	}
	return true
}

func (b *bloomFilter) encode() []byte {
	buf := make([]byte, 8+len(b.bits))
	binary.LittleEndian.PutUint32(buf[0:4], b.m)
	binary.LittleEndian.PutUint32(buf[4:8], b.k)
	copy(buf[8:], b.bits)
	return buf
}

func decodeBloomFilter(data []byte) *bloomFilter {
	if len(data) < 8 {
		return nil
	}
	m := binary.LittleEndian.Uint32(data[0:4])
	k := binary.LittleEndian.Uint32(data[4:8])
	bits := append([]byte(nil), data[8:]...)
	if m == 0 || k == 0 || len(bits) == 0 {
		return nil
	}
	return &bloomFilter{m: m, k: k, bits: bits}
}

func hashKey(key string) (uint64, uint64) {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	h1 := h.Sum64()
	h2 := (h1 >> 33) ^ (h1 << 11) ^ 0x9e3779b97f4a7c15
	if h2 == 0 {
		h2 = 0x27d4eb2d
	}
	return h1, h2
}
