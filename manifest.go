package tidesdb

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

const (
	manifestAdd    byte = 1
	manifestRemove byte = 2
)

type manifestStore struct {
	path string
	mu   sync.Mutex
	file *os.File
}

func newManifestStore(dir string) (*manifestStore, error) {
	path := filepath.Join(dir, "MANIFEST")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &manifestStore{path: path, file: file}, nil
}

func (ms *manifestStore) Close() error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if ms.file == nil {
		return nil
	}
	return ms.file.Close()
}

func (ms *manifestStore) Load() (map[int][]uint64, error) {
	file, err := os.Open(ms.path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	levels := make(map[int][]uint64)
	for {
		op, err := reader.ReadByte()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		var level uint32
		var id uint64
		if err := binary.Read(reader, binary.LittleEndian, &level); err != nil {
			if errors.Is(err, io.EOF) {
				return nil, io.ErrUnexpectedEOF
			}
			return nil, err
		}
		if err := binary.Read(reader, binary.LittleEndian, &id); err != nil {
			if errors.Is(err, io.EOF) {
				return nil, io.ErrUnexpectedEOF
			}
			return nil, err
		}
		lvl := int(level)
		switch op {
		case manifestAdd:
			levels[lvl] = append(levels[lvl], id)
		case manifestRemove:
			levels[lvl] = removeID(levels[lvl], id)
		default:
			return nil, errors.New("unknown manifest op")
		}
	}
	for level := range levels {
		sort.Slice(levels[level], func(i, j int) bool { return levels[level][i] < levels[level][j] })
	}
	return levels, nil
}

func (ms *manifestStore) AppendAdd(level int, id uint64) error {
	return ms.appendRecord(manifestAdd, level, id)
}

func (ms *manifestStore) AppendRemove(level int, id uint64) error {
	return ms.appendRecord(manifestRemove, level, id)
}

func (ms *manifestStore) appendRecord(op byte, level int, id uint64) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if ms.file == nil {
		return errors.New("manifest closed")
	}
	if _, err := ms.file.Write([]byte{op}); err != nil {
		return err
	}
	if err := binary.Write(ms.file, binary.LittleEndian, uint32(level)); err != nil {
		return err
	}
	if err := binary.Write(ms.file, binary.LittleEndian, id); err != nil {
		return err
	}
	return ms.file.Sync()
}

func removeID(ids []uint64, id uint64) []uint64 {
	out := ids[:0]
	for _, v := range ids {
		if v == id {
			continue
		}
		out = append(out, v)
	}
	return out
}
