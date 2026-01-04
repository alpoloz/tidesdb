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
	tableFooterMagic = "TDBMETA0"
	tableFooterSize  = 8 + 8 + 4
)

type sstable struct {
	id       uint64
	level    int
	dataPath string
	index    []blockIndexEntry
	minKey   string
	maxKey   string
	bloom    *bloomFilter
	logger   *zap.Logger
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
	sst := &sstable{
		id:       id,
		level:    level,
		dataPath: dataPath,
		logger:   logger,
	}

	dataFile, err := os.Create(dataPath)
	if err != nil {
		return nil, err
	}
	defer dataFile.Close()
	dataWriter := bufio.NewWriter(dataFile)

	index := make([]blockIndexEntry, 0, len(entries))
	filter := newBloomFilter(len(entries), 10)
	var offset int64
	var blockBuf bytes.Buffer
	var indexBuf bytes.Buffer
	blockFirstKey := ""

	flushBlock := func() error {
		if blockBuf.Len() == 0 {
			return nil
		}
		if err := sst.writeIndexEntry(&indexBuf, blockFirstKey, offset); err != nil {
			return err
		}
		index = append(index, blockIndexEntry{firstKey: blockFirstKey, offset: offset})
		if err := sst.writeBlock(dataWriter, blockBuf.Bytes()); err != nil {
			return err
		}
		offset += int64(4 + blockBuf.Len())
		blockBuf.Reset()
		return nil
	}

	for _, item := range entries {
		recordSize := sst.recordEncodedSize(item.key, item.entry)
		if blockBuf.Len() == 0 {
			blockFirstKey = item.key
		}
		if blockBuf.Len() > 0 && blockBuf.Len()+int(recordSize) > sstableBlockSize {
			if err := flushBlock(); err != nil {
				return nil, err
			}
			blockFirstKey = item.key
		}
		if err := sst.writeRecord(&blockBuf, item.key, item.entry); err != nil {
			return nil, err
		}
		if userKey, _, _, ok := decodeInternalKey([]byte(item.key)); ok {
			filter.add(userKey)
		}
	}
	if err := flushBlock(); err != nil {
		return nil, err
	}

	indexOffset := uint64(offset)
	indexData := indexBuf.Bytes()
	if len(indexData) > 0 {
		if _, err := dataWriter.Write(indexData); err != nil {
			return nil, err
		}
	}
	bloomOffset := indexOffset + uint64(len(indexData))
	bloomData := filter.encode()
	if len(bloomData) > 0 {
		if _, err := dataWriter.Write(bloomData); err != nil {
			return nil, err
		}
	}
	metaOffset := bloomOffset + uint64(len(bloomData))
	metaData, err := sst.buildMetaindex(indexOffset, uint32(len(indexData)), bloomOffset, uint32(len(bloomData)))
	if err != nil {
		return nil, err
	}
	if _, err := dataWriter.Write(metaData); err != nil {
		return nil, err
	}
	if err := sst.writeTableFooter(dataWriter, metaOffset, uint32(len(metaData))); err != nil {
		return nil, err
	}

	if err := dataWriter.Flush(); err != nil {
		return nil, err
	}
	if err := dataFile.Sync(); err != nil {
		return nil, err
	}

	logger.Info("sstable created",
		zap.Uint64("sstable_id", id),
		zap.Int("entries", len(entries)),
	)
	sst.index = index
	sst.bloom = filter
	if len(entries) > 0 {
		if userKey, _, _, ok := decodeInternalKey([]byte(entries[0].key)); ok {
			sst.minKey = userKey
		}
		if userKey, _, _, ok := decodeInternalKey([]byte(entries[len(entries)-1].key)); ok {
			sst.maxKey = userKey
		}
	}
	return sst, nil
}

func loadSSTable(logger *zap.Logger, dir string, level int, id uint64) (*sstable, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	dataPath := filepath.Join(dir, sstDataName(level, id))
	sst := &sstable{
		id:       id,
		level:    level,
		dataPath: dataPath,
		logger:   logger,
	}
	index, filter, err := sst.readEmbeddedIndexAndBloom()
	if err != nil {
		return nil, err
	}
	logger.Info("sstable loaded",
		zap.Uint64("sstable_id", id),
	)
	sst.index = index
	sst.bloom = filter
	if err := sst.computeKeyRange(); err != nil {
		return nil, err
	}
	return sst, nil
}

func (s *sstable) get(key string) (entry, bool, error) {
	return s.getAt(key, ^uint64(0))
}

func (s *sstable) getAt(key string, seq uint64) (entry, bool, error) {
	if s.bloom != nil && !s.bloom.mayContain(key) {
		return entry{}, false, nil
	}
	if len(s.index) == 0 {
		return entry{}, false, nil
	}
	seekKey := encodeInternalKey(key, seq, valueKindPut)
	pos := sort.Search(len(s.index), func(i int) bool {
		return compareInternalKeyStrings(s.index[i].firstKey, string(seekKey)) > 0
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
		recKey, ent, _, err := s.readRecord(reader)
		if err != nil {
			return entry{}, false, err
		}
		userKey, recSeq, _, ok := decodeInternalKey([]byte(recKey))
		if !ok {
			continue
		}
		cmp := strings.Compare(userKey, key)
		if cmp == 0 {
			if recSeq <= seq {
				return ent, true, nil
			}
			continue
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
	return nil
}

func mergeSSTables(logger *zap.Logger, dir string, level int, id uint64, tables []*sstable) (*sstable, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	entries := make([]sstEntry, 0)
	for _, sst := range tables {
		tableEntries, err := sst.readAllEntries()
		if err != nil {
			return nil, err
		}
		entries = append(entries, tableEntries...)
	}
	sort.Slice(entries, func(i, j int) bool {
		return compareInternalKeyStrings(entries[i].key, entries[j].key) < 0
	})

	logger.Info("sstable merge",
		zap.Uint64("sstable_id", id),
		zap.Int("sources", len(tables)),
		zap.Int("entries", len(entries)),
	)
	return createSSTable(logger, dir, level, id, entries)
}

func parseSSTableName(name string) (int, uint64, bool) {
	if !strings.HasSuffix(name, ".dat") {
		return 0, 0, false
	}
	base := strings.TrimSuffix(name, ".dat")
	if strings.HasPrefix(base, "sst_L") {
		parts := strings.Split(strings.TrimPrefix(base, "sst_L"), "_")
		if len(parts) != 2 {
			return 0, 0, false
		}
		level, err := strconv.Atoi(parts[0])
		if err != nil {
			return 0, 0, false
		}
		id, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			return 0, 0, false
		}
		return level, id, true
	}
	return 0, 0, false
}

func sstDataName(level int, id uint64) string {
	return fmt.Sprintf("sst_L%d_%06d.dat", level, id)
}

func (s *sstable) writeRecord(w io.Writer, key string, ent entry) error {
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

func (s *sstable) readRecord(r io.Reader) (string, entry, int64, error) {
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
	return keyStr, ent, s.recordEncodedSize(keyStr, ent), nil
}

func (s *sstable) writeIndexEntry(w io.Writer, key string, offset int64) error {
	keyLen := uint32(len(key))
	if err := binary.Write(w, binary.LittleEndian, keyLen); err != nil {
		return err
	}
	if _, err := io.WriteString(w, key); err != nil {
		return err
	}
	return binary.Write(w, binary.LittleEndian, offset)
}

func (s *sstable) readIndex(reader *bufio.Reader) ([]blockIndexEntry, error) {
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

func (s *sstable) readEmbeddedIndexAndBloom() ([]blockIndexEntry, *bloomFilter, error) {
	file, err := os.Open(s.dataPath)
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil, nil, err
	}
	if info.Size() < tableFooterSize {
		return nil, nil, nil
	}
	if _, err := file.Seek(info.Size()-tableFooterSize, io.SeekStart); err != nil {
		return nil, nil, err
	}

	footer := make([]byte, tableFooterSize)
	if _, err := io.ReadFull(file, footer); err != nil {
		return nil, nil, err
	}
	if string(footer[:8]) != tableFooterMagic {
		return nil, nil, nil
	}
	metaOffset := binary.LittleEndian.Uint64(footer[8:16])
	metaLength := binary.LittleEndian.Uint32(footer[16:20])
	if metaOffset > uint64(info.Size()) {
		return nil, nil, errors.New("invalid metaindex offset")
	}
	if uint64(metaLength) > uint64(info.Size())-metaOffset-uint64(tableFooterSize) {
		return nil, nil, errors.New("invalid metaindex length")
	}
	if _, err := file.Seek(int64(metaOffset), io.SeekStart); err != nil {
		return nil, nil, err
	}
	meta := make([]byte, metaLength)
	if _, err := io.ReadFull(file, meta); err != nil {
		return nil, nil, err
	}
	indexOffset, indexLength, ok, err := s.parseMetaindex(meta, "index")
	if err != nil {
		return nil, nil, err
	}
	if !ok {
		return nil, nil, errors.New("missing index meta")
	}
	if indexOffset+uint64(indexLength) > uint64(info.Size()) {
		return nil, nil, errors.New("invalid index range")
	}
	if _, err := file.Seek(int64(indexOffset), io.SeekStart); err != nil {
		return nil, nil, err
	}
	indexReader := bufio.NewReader(io.LimitReader(file, int64(indexLength)))
	index, err := s.readIndex(indexReader)
	if err != nil {
		return nil, nil, err
	}

	bloomOffset, bloomLength, ok, err := s.parseMetaindex(meta, "bloom")
	if err != nil || !ok {
		return index, nil, err
	}
	if bloomOffset+uint64(bloomLength) > uint64(info.Size()) {
		return nil, nil, errors.New("invalid bloom range")
	}
	if _, err := file.Seek(int64(bloomOffset), io.SeekStart); err != nil {
		return nil, nil, err
	}
	data := make([]byte, bloomLength)
	if _, err := io.ReadFull(file, data); err != nil {
		return nil, nil, err
	}
	filter := decodeBloomFilter(data)
	if filter == nil {
		return index, nil, nil
	}
	return index, filter, nil
}

func (s *sstable) computeKeyRange() error {
	if len(s.index) == 0 {
		return nil
	}
	if userKey, _, _, ok := decodeInternalKey([]byte(s.index[0].firstKey)); ok {
		s.minKey = userKey
	}
	last := s.index[len(s.index)-1]
	keys, err := s.readBlockKeys(last.offset)
	if err != nil {
		return err
	}
	if len(keys) > 0 {
		if userKey, _, _, ok := decodeInternalKey([]byte(keys[len(keys)-1])); ok {
			s.maxKey = userKey
		}
	}
	return nil
}

func (s *sstable) writeTableFooter(w io.Writer, offset uint64, length uint32) error {
	if _, err := w.Write([]byte(tableFooterMagic)); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, offset); err != nil {
		return err
	}
	return binary.Write(w, binary.LittleEndian, length)
}

func (s *sstable) buildMetaindex(indexOffset uint64, indexLength uint32, bloomOffset uint64, bloomLength uint32) ([]byte, error) {
	var buf bytes.Buffer
	if err := s.writeMetaEntry(&buf, "index", indexOffset, indexLength); err != nil {
		return nil, err
	}
	if err := s.writeMetaEntry(&buf, "bloom", bloomOffset, bloomLength); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (s *sstable) parseMetaindex(meta []byte, name string) (uint64, uint32, bool, error) {
	reader := bytes.NewReader(meta)
	for reader.Len() > 0 {
		var nameLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &nameLen); err != nil {
			return 0, 0, false, err
		}
		if nameLen == 0 || nameLen > uint32(reader.Len()) {
			return 0, 0, false, errors.New("invalid metaindex name length")
		}
		nameBytes := make([]byte, nameLen)
		if _, err := io.ReadFull(reader, nameBytes); err != nil {
			return 0, 0, false, err
		}
		var offset uint64
		var length uint32
		if err := binary.Read(reader, binary.LittleEndian, &offset); err != nil {
			return 0, 0, false, err
		}
		if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
			return 0, 0, false, err
		}
		if string(nameBytes) == name {
			return offset, length, true, nil
		}
	}
	return 0, 0, false, nil
}

func (s *sstable) writeMetaEntry(w io.Writer, name string, offset uint64, length uint32) error {
	nameBytes := []byte(name)
	if err := binary.Write(w, binary.LittleEndian, uint32(len(nameBytes))); err != nil {
		return err
	}
	if _, err := w.Write(nameBytes); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, offset); err != nil {
		return err
	}
	return binary.Write(w, binary.LittleEndian, length)
}

func (s *sstable) writeBlock(w io.Writer, data []byte) error {
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
		recKey, _, _, err := s.readRecord(reader)
		if err != nil {
			return nil, err
		}
		keys = append(keys, recKey)
	}
	return keys, nil
}

func (s *sstable) readAllEntries() ([]sstEntry, error) {
	file, err := os.Open(s.dataPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	entries := make([]sstEntry, 0)
	for _, block := range s.index {
		if _, err := file.Seek(block.offset, io.SeekStart); err != nil {
			return nil, err
		}
		var blockLen uint32
		if err := binary.Read(file, binary.LittleEndian, &blockLen); err != nil {
			return nil, err
		}
		blockData := make([]byte, blockLen)
		if _, err := io.ReadFull(file, blockData); err != nil {
			return nil, err
		}
		reader := bytes.NewReader(blockData)
		for reader.Len() > 0 {
			recKey, ent, _, err := s.readRecord(reader)
			if err != nil {
				return nil, err
			}
			entries = append(entries, sstEntry{key: recKey, entry: ent})
		}
	}
	return entries, nil
}

func (s *sstable) recordEncodedSize(key string, ent entry) int64 {
	return int64(4 + 4 + 1 + len(key) + len(ent.value))
}
