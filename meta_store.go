package tidesdb

import (
	"errors"
	"os"
	"path/filepath"
)

type metaStore struct {
	path string
}

func newMetaStore(path string) *metaStore {
	return &metaStore{path: path}
}

func (ms *metaStore) LoadSeq(db *DB) error {
	path := filepath.Join(ms.path, "seq.meta")
	seq, err := readSeqMeta(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	db.seq = seq
	return nil
}

func (ms *metaStore) StoreSeq(db *DB) error {
	path := filepath.Join(ms.path, "seq.meta")
	return writeSeqMeta(path, db.seq)
}
