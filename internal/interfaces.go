package internal

import (
	"io"
)

type WALEntry struct {
	Key   string
	Value []byte
}

type Memtable interface {
	Set(KVPair)
	Get(string) Position
	Contains(string) bool
	Items() []KVPair
	Size() uint32
}

// DataManager is responsible for managing pair values
type DataManager interface {
	Store([]byte) (Position, error)
	Retrieve(Position) ([]byte, error)
	Compact() error
	Close() error
}

type WAL interface {
	Append(WALEntry) error
	Retrieve() ([]WALEntry, error)
	Clear() error
	Close() error
}

type WriteSeekCloser interface {
	io.Writer
	io.Seeker
	io.Closer
}
