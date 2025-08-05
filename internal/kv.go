package internal

import (
	"encoding/binary"

	"github.com/hasssanezzz/goldb/shared"
)

type Position struct {
	Offset uint32
	Size   uint32
}

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
