package tidesdb

import (
	"bytes"
	"encoding/binary"
	"os"
)

type valueKind byte

const (
	valueKindDelete valueKind = 0
	valueKindPut    valueKind = 1
)

const internalKeySuffix = 8 + 1

func encodeInternalKey(userKey string, seq uint64, kind valueKind) []byte {
	buf := make([]byte, len(userKey)+internalKeySuffix)
	copy(buf, userKey)
	invSeq := ^seq
	binary.BigEndian.PutUint64(buf[len(userKey):len(userKey)+8], invSeq)
	buf[len(buf)-1] = byte(kind)
	return buf
}

func decodeInternalKey(internal []byte) (string, uint64, valueKind, bool) {
	if len(internal) < internalKeySuffix {
		return "", 0, 0, false
	}
	userKey := string(internal[:len(internal)-internalKeySuffix])
	invSeq := binary.BigEndian.Uint64(internal[len(internal)-internalKeySuffix : len(internal)-1])
	seq := ^invSeq
	kind := valueKind(internal[len(internal)-1])
	return userKey, seq, kind, true
}

func compareInternalKeys(a, b []byte) int {
	return bytes.Compare(a, b)
}

func compareInternalKeyStrings(a, b string) int {
	return compareInternalKeys([]byte(a), []byte(b))
}

func readSeqMeta(path string) (uint64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	if len(data) < 8 {
		return 0, nil
	}
	return binary.LittleEndian.Uint64(data[:8]), nil
}

func writeSeqMeta(path string, seq uint64) error {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, seq)
	return os.WriteFile(path, buf, 0o644)
}
