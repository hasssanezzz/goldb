package shared

import (
	"io"
)

// type Memtable interface {
// 	Set(KVPair)
// 	Get(key string) IndexNode // TODO: change key type of []byte
// 	Contains(key string) bool
// 	Items() []KVPair
// }

type WriteSeekCloser interface {
	io.Writer
	io.Seeker
	io.Closer
}
