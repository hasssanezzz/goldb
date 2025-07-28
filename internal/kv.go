package internal

import (
	"encoding/binary"

	"github.com/hasssanezzz/goldb/shared"
)

type KVPair struct {
	Key   string
	Value Position
}

func (p KVPair) Encode() []byte {
	buffer := make([]byte, 0, shared.KeySize+shared.UintSize*2)

	buffer = append(buffer, shared.KeyToBytes(p.Key)...)
	binary.LittleEndian.AppendUint32(buffer, p.Value.Offset)
	binary.LittleEndian.AppendUint32(buffer, p.Value.Size)

	return buffer
}

type Pairs []KVPair

func (a Pairs) Len() int           { return len(a) }
func (a Pairs) Less(i, j int) bool { return a[i].Key < a[j].Key }
func (a Pairs) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
