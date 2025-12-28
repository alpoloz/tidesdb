package tidesdb

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"go.uber.org/zap"
)

type sstable struct {
	id        uint64
	dataPath  string
	indexPath string
	index     map[string]int64
	logger    *zap.Logger
}

type sstEntry struct {
	key   string
	entry entry
}

func createSSTable(dir string, id uint64, entries []sstEntry, logger *zap.Logger) (*sstable, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	dataPath := filepath.Join(dir, sstDataName(id))
	indexPath := filepath.Join(dir, sstIndexName(id))

	dataFile, err := os.Create(dataPath)
	if err != nil {
		return nil, err
	}
	defer dataFile.Close()
	indexFile, err := os.Create(indexPath)
	if err != nil {
		return nil, err
	}
	defer indexFile.Close()

	dataWriter := bufio.NewWriter(dataFile)
	indexWriter := bufio.NewWriter(indexFile)

	index := make(map[string]int64, len(entries))
	var offset int64
	for _, item := range entries {
		recordSize := recordEncodedSize(item.key, item.entry)
		if err := writeRecord(dataWriter, item.key, item.entry); err != nil {
			return nil, err
		}
		if err := writeIndexEntry(indexWriter, item.key, offset); err != nil {
			return nil, err
		}
		index[item.key] = offset
		offset += recordSize
	}

	if err := dataWriter.Flush(); err != nil {
		return nil, err
	}
	if err := indexWriter.Flush(); err != nil {
		return nil, err
	}
	if err := dataFile.Sync(); err != nil {
		return nil, err
	}
	if err := indexFile.Sync(); err != nil {
		return nil, err
	}

	logger.Info("sstable created",
		zap.Uint64("sstable_id", id),
		zap.Int("entries", len(entries)),
	)
	return &sstable{id: id, dataPath: dataPath, indexPath: indexPath, index: index, logger: logger}, nil
}

func loadSSTable(dir string, id uint64, logger *zap.Logger) (*sstable, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	dataPath := filepath.Join(dir, sstDataName(id))
	indexPath := filepath.Join(dir, sstIndexName(id))
	index, err := readIndex(indexPath)
	if err != nil {
		return nil, err
	}
	logger.Info("sstable loaded",
		zap.Uint64("sstable_id", id),
	)
	return &sstable{id: id, dataPath: dataPath, indexPath: indexPath, index: index, logger: logger}, nil
}

func (s *sstable) get(key string) (entry, bool, error) {
	offset, ok := s.index[key]
	if !ok {
		return entry{}, false, nil
	}
	file, err := os.Open(s.dataPath)
	if err != nil {
		return entry{}, false, err
	}
	defer file.Close()
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return entry{}, false, err
	}
	ent, _, err := readRecord(file)
	if err != nil {
		return entry{}, false, err
	}
	return ent, true, nil
}

func (s *sstable) remove() error {
	if err := os.Remove(s.dataPath); err != nil {
		return err
	}
	return os.Remove(s.indexPath)
}

func mergeSSTables(dir string, id uint64, tables []*sstable, logger *zap.Logger) (*sstable, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	type tableKey struct {
		key string
		idx int
	}

	keysMap := make(map[string]int)
	for i, sst := range tables {
		for key := range sst.index {
			if existing, ok := keysMap[key]; ok {
				if existing < i {
					keysMap[key] = i
				}
				continue
			}
			keysMap[key] = i
		}
	}

	var keyList []tableKey
	for key, idx := range keysMap {
		keyList = append(keyList, tableKey{key: key, idx: idx})
	}
	sort.Slice(keyList, func(i, j int) bool { return keyList[i].key < keyList[j].key })

	entries := make([]sstEntry, 0, len(keyList))
	for _, item := range keyList {
		ent, ok, err := tables[item.idx].get(item.key)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, errors.New("missing key during compaction")
		}
		if ent.tombstone {
			continue
		}
		entries = append(entries, sstEntry{key: item.key, entry: ent})
	}

	logger.Info("sstable merge",
		zap.Uint64("sstable_id", id),
		zap.Int("sources", len(tables)),
		zap.Int("entries", len(entries)),
	)
	return createSSTable(dir, id, entries, logger)
}

func parseSSTableID(name string) (uint64, bool) {
	if strings.HasPrefix(name, "sst_") && strings.HasSuffix(name, ".dat") {
		base := strings.TrimSuffix(strings.TrimPrefix(name, "sst_"), ".dat")
		id, err := strconv.ParseUint(base, 10, 64)
		if err == nil {
			return id, true
		}
	}
	return 0, false
}

func sstDataName(id uint64) string {
	return fmt.Sprintf("sst_%06d.dat", id)
}

func sstIndexName(id uint64) string {
	return fmt.Sprintf("sst_%06d.idx", id)
}

func writeRecord(w io.Writer, key string, ent entry) error {
	keyLen := uint32(len(key))
	valLen := uint32(len(ent.value))
	if err := binary.Write(w, binary.LittleEndian, keyLen); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, valLen); err != nil {
		return err
	}
	var tombstone byte
	if ent.tombstone {
		tombstone = 1
	}
	if err := binary.Write(w, binary.LittleEndian, tombstone); err != nil {
		return err
	}
	if _, err := io.WriteString(w, key); err != nil {
		return err
	}
	if valLen > 0 {
		if _, err := w.Write(ent.value); err != nil {
			return err
		}
	}
	return nil
}

func readRecord(r io.Reader) (entry, int64, error) {
	var keyLen uint32
	var valLen uint32
	if err := binary.Read(r, binary.LittleEndian, &keyLen); err != nil {
		return entry{}, 0, err
	}
	if err := binary.Read(r, binary.LittleEndian, &valLen); err != nil {
		return entry{}, 0, err
	}
	var tombstoneByte byte
	if err := binary.Read(r, binary.LittleEndian, &tombstoneByte); err != nil {
		return entry{}, 0, err
	}
	key := make([]byte, keyLen)
	if _, err := io.ReadFull(r, key); err != nil {
		return entry{}, 0, err
	}
	value := make([]byte, valLen)
	if valLen > 0 {
		if _, err := io.ReadFull(r, value); err != nil {
			return entry{}, 0, err
		}
	}
	ent := entry{value: value, tombstone: tombstoneByte == 1}
	return ent, recordEncodedSize(string(key), ent), nil
}

func writeIndexEntry(w io.Writer, key string, offset int64) error {
	keyLen := uint32(len(key))
	if err := binary.Write(w, binary.LittleEndian, keyLen); err != nil {
		return err
	}
	if _, err := io.WriteString(w, key); err != nil {
		return err
	}
	return binary.Write(w, binary.LittleEndian, offset)
}

func readIndex(path string) (map[string]int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	index := make(map[string]int64)
	for {
		var keyLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &keyLen); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		key := make([]byte, keyLen)
		if _, err := io.ReadFull(reader, key); err != nil {
			return nil, err
		}
		var offset int64
		if err := binary.Read(reader, binary.LittleEndian, &offset); err != nil {
			return nil, err
		}
		index[string(key)] = offset
	}
	return index, nil
}

func recordEncodedSize(key string, ent entry) int64 {
	return int64(4 + 4 + 1 + len(key) + len(ent.value))
}
