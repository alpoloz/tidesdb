package tidesdb

import (
	"bufio"
	"bytes"
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

const (
	sstableBlockSize = 4 << 10
	bloomFooterMagic = "TDBBLOOM"
	bloomFooterSize  = 8 + 8 + 4
)

type sstable struct {
	id        uint64
	level     int
	dataPath  string
	indexPath string
	index     []blockIndexEntry
	bloom     *bloomFilter
	logger    *zap.Logger
}

type sstEntry struct {
	key   string
	entry entry
}

type blockIndexEntry struct {
	firstKey string
	offset   int64
}

func createSSTable(logger *zap.Logger, dir string, level int, id uint64, entries []sstEntry) (*sstable, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	dataPath := filepath.Join(dir, sstDataName(level, id))
	indexPath := filepath.Join(dir, sstIndexName(level, id))

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

	index := make([]blockIndexEntry, 0, len(entries))
	filter := newBloomFilter(len(entries), 10)
	var offset int64
	var blockBuf bytes.Buffer
	blockFirstKey := ""

	flushBlock := func() error {
		if blockBuf.Len() == 0 {
			return nil
		}
		if err := writeIndexEntry(indexWriter, blockFirstKey, offset); err != nil {
			return err
		}
		index = append(index, blockIndexEntry{firstKey: blockFirstKey, offset: offset})
		if err := writeBlock(dataWriter, blockBuf.Bytes()); err != nil {
			return err
		}
		offset += int64(4 + blockBuf.Len())
		blockBuf.Reset()
		return nil
	}

	for _, item := range entries {
		recordSize := recordEncodedSize(item.key, item.entry)
		if blockBuf.Len() == 0 {
			blockFirstKey = item.key
		}
		if blockBuf.Len() > 0 && blockBuf.Len()+int(recordSize) > sstableBlockSize {
			if err := flushBlock(); err != nil {
				return nil, err
			}
			blockFirstKey = item.key
		}
		if err := writeRecord(&blockBuf, item.key, item.entry); err != nil {
			return nil, err
		}
		filter.add(item.key)
	}
	if err := flushBlock(); err != nil {
		return nil, err
	}

	bloomOffset := uint64(offset)
	bloomData := filter.encode()
	if len(bloomData) > 0 {
		if _, err := dataWriter.Write(bloomData); err != nil {
			return nil, err
		}
	}
	if err := writeBloomFooter(dataWriter, bloomOffset, uint32(len(bloomData))); err != nil {
		return nil, err
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
	return &sstable{
		id:        id,
		level:     level,
		dataPath:  dataPath,
		indexPath: indexPath,
		index:     index,
		bloom:     filter,
		logger:    logger,
	}, nil
}

func loadSSTable(logger *zap.Logger, dir string, level int, id uint64) (*sstable, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	dataPath := filepath.Join(dir, sstDataName(level, id))
	indexPath := filepath.Join(dir, sstIndexName(level, id))
	index, err := readIndex(indexPath)
	if err != nil {
		return nil, err
	}
	filter, err := readEmbeddedBloom(dataPath)
	if err != nil {
		return nil, err
	}
	if filter == nil {
		filter, err = readBloom(filepath.Join(dir, sstBloomName(level, id)))
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	logger.Info("sstable loaded",
		zap.Uint64("sstable_id", id),
	)
	return &sstable{
		id:        id,
		level:     level,
		dataPath:  dataPath,
		indexPath: indexPath,
		index:     index,
		bloom:     filter,
		logger:    logger,
	}, nil
}

func (s *sstable) get(key string) (entry, bool, error) {
	if s.bloom != nil && !s.bloom.mayContain(key) {
		return entry{}, false, nil
	}
	if len(s.index) == 0 {
		return entry{}, false, nil
	}
	pos := sort.Search(len(s.index), func(i int) bool {
		return s.index[i].firstKey > key
	})
	blockIdx := pos - 1
	if blockIdx < 0 {
		return entry{}, false, nil
	}
	offset := s.index[blockIdx].offset
	file, err := os.Open(s.dataPath)
	if err != nil {
		return entry{}, false, err
	}
	defer file.Close()
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return entry{}, false, err
	}
	var blockLen uint32
	if err := binary.Read(file, binary.LittleEndian, &blockLen); err != nil {
		return entry{}, false, err
	}
	block := make([]byte, blockLen)
	if _, err := io.ReadFull(file, block); err != nil {
		return entry{}, false, err
	}
	reader := bytes.NewReader(block)
	for reader.Len() > 0 {
		recKey, ent, _, err := readRecord(reader)
		if err != nil {
			return entry{}, false, err
		}
		cmp := strings.Compare(recKey, key)
		if cmp == 0 {
			return ent, true, nil
		}
		if cmp > 0 {
			return entry{}, false, nil
		}
	}
	return entry{}, false, nil
}

func (s *sstable) remove() error {
	if err := os.Remove(s.dataPath); err != nil {
		return err
	}
	if err := os.Remove(s.indexPath); err != nil {
		return err
	}
	bloomPath := filepath.Join(filepath.Dir(s.dataPath), sstBloomName(s.level, s.id))
	if err := os.Remove(bloomPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func mergeSSTables(logger *zap.Logger, dir string, level int, id uint64, tables []*sstable) (*sstable, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	type tableKey struct {
		key string
		idx int
	}

	keysMap := make(map[string]int)
	for i, sst := range tables {
		for _, block := range sst.index {
			blockKeys, err := sst.readBlockKeys(block.offset)
			if err != nil {
				return nil, err
			}
			for _, key := range blockKeys {
				if existing, ok := keysMap[key]; ok {
					if existing < i {
						keysMap[key] = i
					}
					continue
				}
				keysMap[key] = i
			}
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
	return createSSTable(logger, dir, level, id, entries)
}

func parseSSTableName(name string) (int, uint64, bool, bool) {
	if !strings.HasSuffix(name, ".dat") {
		return 0, 0, false, false
	}
	base := strings.TrimSuffix(name, ".dat")
	if strings.HasPrefix(base, "sst_L") {
		parts := strings.Split(strings.TrimPrefix(base, "sst_L"), "_")
		if len(parts) != 2 {
			return 0, 0, false, false
		}
		level, err := strconv.Atoi(parts[0])
		if err != nil {
			return 0, 0, false, false
		}
		id, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			return 0, 0, false, false
		}
		return level, id, true, false
	}
	if strings.HasPrefix(base, "sst_") {
		id, err := strconv.ParseUint(strings.TrimPrefix(base, "sst_"), 10, 64)
		if err == nil {
			return 0, id, true, true
		}
	}
	return 0, 0, false, false
}

func sstDataName(level int, id uint64) string {
	return fmt.Sprintf("sst_L%d_%06d.dat", level, id)
}

func sstIndexName(level int, id uint64) string {
	return fmt.Sprintf("sst_L%d_%06d.idx", level, id)
}

func sstBloomName(level int, id uint64) string {
	return fmt.Sprintf("sst_L%d_%06d.bloom", level, id)
}

func sstLegacyDataName(id uint64) string {
	return fmt.Sprintf("sst_%06d.dat", id)
}

func sstLegacyIndexName(id uint64) string {
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

func readRecord(r io.Reader) (string, entry, int64, error) {
	var keyLen uint32
	var valLen uint32
	if err := binary.Read(r, binary.LittleEndian, &keyLen); err != nil {
		return "", entry{}, 0, err
	}
	if err := binary.Read(r, binary.LittleEndian, &valLen); err != nil {
		return "", entry{}, 0, err
	}
	var tombstoneByte byte
	if err := binary.Read(r, binary.LittleEndian, &tombstoneByte); err != nil {
		return "", entry{}, 0, err
	}
	key := make([]byte, keyLen)
	if _, err := io.ReadFull(r, key); err != nil {
		return "", entry{}, 0, err
	}
	value := make([]byte, valLen)
	if valLen > 0 {
		if _, err := io.ReadFull(r, value); err != nil {
			return "", entry{}, 0, err
		}
	}
	ent := entry{value: value, tombstone: tombstoneByte == 1}
	keyStr := string(key)
	return keyStr, ent, recordEncodedSize(keyStr, ent), nil
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

func readIndex(path string) ([]blockIndexEntry, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	index := make([]blockIndexEntry, 0)
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
		index = append(index, blockIndexEntry{firstKey: string(key), offset: offset})
	}
	return index, nil
}

func readBloom(path string) (*bloomFilter, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	filter := decodeBloomFilter(data)
	if filter == nil {
		return nil, errors.New("invalid bloom filter")
	}
	return filter, nil
}

func readEmbeddedBloom(path string) (*bloomFilter, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() < bloomFooterSize {
		return nil, nil
	}
	if _, err := file.Seek(info.Size()-bloomFooterSize, io.SeekStart); err != nil {
		return nil, err
	}

	footer := make([]byte, bloomFooterSize)
	if _, err := io.ReadFull(file, footer); err != nil {
		return nil, err
	}
	if string(footer[:8]) != bloomFooterMagic {
		return nil, nil
	}
	offset := binary.LittleEndian.Uint64(footer[8:16])
	length := binary.LittleEndian.Uint32(footer[16:20])
	if offset > uint64(info.Size()) {
		return nil, errors.New("invalid bloom offset")
	}
	if uint64(length) > uint64(info.Size())-offset-uint64(bloomFooterSize) {
		return nil, errors.New("invalid bloom length")
	}
	if _, err := file.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, err
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(file, data); err != nil {
		return nil, err
	}
	filter := decodeBloomFilter(data)
	if filter == nil {
		return nil, nil
	}
	return filter, nil
}

func writeBloomFooter(w io.Writer, offset uint64, length uint32) error {
	if _, err := w.Write([]byte(bloomFooterMagic)); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, offset); err != nil {
		return err
	}
	return binary.Write(w, binary.LittleEndian, length)
}

func migrateLegacySSTable(logger *zap.Logger, dir string, id uint64) (*sstable, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	legacyDataPath := filepath.Join(dir, sstLegacyDataName(id))
	file, err := os.Open(legacyDataPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	entries := make([]sstEntry, 0)
	for {
		key, ent, _, err := readRecord(reader)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		entries = append(entries, sstEntry{key: key, entry: ent})
	}

	sst, err := createSSTable(logger, dir, 0, id, entries)
	if err != nil {
		return nil, err
	}
	if err := os.Remove(legacyDataPath); err != nil {
		return nil, err
	}
	legacyIndexPath := filepath.Join(dir, sstLegacyIndexName(id))
	if err := os.Remove(legacyIndexPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	logger.Info("legacy sstable migrated",
		zap.Uint64("sstable_id", id),
		zap.Int("entries", len(entries)),
	)
	return sst, nil
}

func writeBlock(w io.Writer, data []byte) error {
	if err := binary.Write(w, binary.LittleEndian, uint32(len(data))); err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	_, err := w.Write(data)
	return err
}

func (s *sstable) readBlockKeys(offset int64) ([]string, error) {
	file, err := os.Open(s.dataPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}
	var blockLen uint32
	if err := binary.Read(file, binary.LittleEndian, &blockLen); err != nil {
		return nil, err
	}
	block := make([]byte, blockLen)
	if _, err := io.ReadFull(file, block); err != nil {
		return nil, err
	}
	reader := bytes.NewReader(block)
	keys := make([]string, 0)
	for reader.Len() > 0 {
		recKey, _, _, err := readRecord(reader)
		if err != nil {
			return nil, err
		}
		keys = append(keys, recKey)
	}
	return keys, nil
}

func recordEncodedSize(key string, ent entry) int64 {
	return int64(4 + 4 + 1 + len(key) + len(ent.value))
}
