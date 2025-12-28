package tidesdb

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
)

type walOp byte

const (
	walOpPut walOp = iota + 1
	walOpDelete
)

type walRecord struct {
	op    walOp
	key   string
	value []byte
}

type wal struct {
	file *os.File
	buf  *bufio.Writer
}

func openWAL(path string) (*wal, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		_ = file.Close()
		return nil, err
	}
	return &wal{file: file, buf: bufio.NewWriter(file)}, nil
}

func (w *wal) appendRecord(op walOp, key string, value []byte) error {
	if err := binary.Write(w.buf, binary.LittleEndian, op); err != nil {
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
		records = append(records, walRecord{op: walOp(opByte), key: string(key), value: value})
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
