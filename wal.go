package tidesdb

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"

	"go.uber.org/zap"
)

type walOp byte

const (
	walOpPut walOp = iota + 1
	walOpDelete
)

type walRecord struct {
	op    walOp
	seq   uint64
	key   string
	value []byte
}

type wal struct {
	file   *os.File
	buf    *bufio.Writer
	logger *zap.Logger
	path   string
}

func openWAL(logger *zap.Logger, path string) (*wal, error) {
	if logger == nil {
		logger = zap.NewNop()
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		_ = file.Close()
		return nil, err
	}
	logger.Info("wal opened", zap.String("path", path))
	return &wal{file: file, buf: bufio.NewWriter(file), logger: logger, path: path}, nil
}

func (w *wal) appendRecord(op walOp, seq uint64, key string, value []byte) error {
	if err := binary.Write(w.buf, binary.LittleEndian, op); err != nil {
		return err
	}
	if err := binary.Write(w.buf, binary.LittleEndian, seq); err != nil {
		return err
	}
	keyLen := uint32(len(key))
	valLen := uint32(len(value))
	if err := binary.Write(w.buf, binary.LittleEndian, keyLen); err != nil {
		return err
	}
	if err := binary.Write(w.buf, binary.LittleEndian, valLen); err != nil {
		return err
	}
	if _, err := w.buf.WriteString(key); err != nil {
		return err
	}
	if valLen > 0 {
		if _, err := w.buf.Write(value); err != nil {
			return err
		}
	}
	if err := w.buf.Flush(); err != nil {
		return err
	}
	return w.file.Sync()
}

func (w *wal) readAll() ([]walRecord, error) {
	if err := w.buf.Flush(); err != nil {
		return nil, err
	}
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	defer func() {
		_, _ = w.file.Seek(0, io.SeekEnd)
	}()

	reader := bufio.NewReader(w.file)
	var records []walRecord
	for {
		opByte, err := reader.ReadByte()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		var seq uint64
		if err := binary.Read(reader, binary.LittleEndian, &seq); err != nil {
			return nil, err
		}
		var keyLen uint32
		var valLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &keyLen); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.LittleEndian, &valLen); err != nil {
			return nil, err
		}
		key := make([]byte, keyLen)
		if _, err := io.ReadFull(reader, key); err != nil {
			return nil, err
		}
		value := make([]byte, valLen)
		if valLen > 0 {
			if _, err := io.ReadFull(reader, value); err != nil {
				return nil, err
			}
		}
		records = append(records, walRecord{op: walOp(opByte), seq: seq, key: string(key), value: value})
	}
	return records, nil
}

func (w *wal) reset() error {
	if err := w.buf.Flush(); err != nil {
		return err
	}
	if err := w.file.Truncate(0); err != nil {
		return err
	}
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	return nil
}

func (w *wal) Close() error {
	if w == nil {
		return nil
	}
	if err := w.buf.Flush(); err != nil {
		_ = w.file.Close()
		return err
	}
	return w.file.Close()
}

func readWALRecords(path string) ([]walRecord, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	var records []walRecord
	for {
		opByte, err := reader.ReadByte()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		var seq uint64
		if err := binary.Read(reader, binary.LittleEndian, &seq); err != nil {
			return nil, err
		}
		var keyLen uint32
		var valLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &keyLen); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.LittleEndian, &valLen); err != nil {
			return nil, err
		}
		key := make([]byte, keyLen)
		if _, err := io.ReadFull(reader, key); err != nil {
			return nil, err
		}
		value := make([]byte, valLen)
		if valLen > 0 {
			if _, err := io.ReadFull(reader, value); err != nil {
				return nil, err
			}
		}
		records = append(records, walRecord{op: walOp(opByte), seq: seq, key: string(key), value: value})
	}
	return records, nil
}
