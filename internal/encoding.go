package internal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/hasssanezzz/goldb/shared"
)

func (tm *TableMetadata) Serialize() []byte {
	buffer := bytes.NewBuffer(nil)

	isLevelAsByte := byte(0x00)
	if tm.IsLevel {
		isLevelAsByte = byte(0xFF)
	}

	binary.Write(buffer, binary.LittleEndian, isLevelAsByte)
	binary.Write(buffer, binary.LittleEndian, tm.Serial)
	binary.Write(buffer, binary.LittleEndian, tm.Size)
	binary.Write(buffer, binary.LittleEndian, tm.FilterSize)
	buffer.Write(shared.KeyToBytes(tm.MinKey))
	buffer.Write(shared.KeyToBytes(tm.MaxKey))

	return buffer.Bytes()
}

func (tm *TableMetadata) Deserialize(r io.Reader) error {
	uintBuffer := make([]byte, shared.UintSize)
	keyBuffer := make([]byte, shared.KeySize)

	// read isLevel
	isLevelBuffer := make([]byte, 1)
	_, err := r.Read(isLevelBuffer)
	if err != nil {
		return fmt.Errorf("failed to deserialize metadata: %v", err)
	}
	tm.IsLevel = isLevelBuffer[0] == 0xFF

	// read serial
	_, err = r.Read(uintBuffer)
	if err != nil {
		return fmt.Errorf("failed to deserialize serial: %v", err)
	}
	tm.Serial = binary.LittleEndian.Uint32(uintBuffer)

	// read table size
	_, err = r.Read(uintBuffer)
	if err != nil {
		return fmt.Errorf("failed to deserialize table size: %v", err)
	}
	tm.Size = binary.LittleEndian.Uint32(uintBuffer)

	// read filter size
	_, err = r.Read(uintBuffer)
	if err != nil {
		return fmt.Errorf("failed to deserialize filter size: %v", err)
	}
	tm.FilterSize = binary.LittleEndian.Uint32(uintBuffer)

	// read min key
	_, err = r.Read(keyBuffer)
	if err != nil {
		return fmt.Errorf("failed to deserialize min key: %v", err)
	}
	tm.MinKey = shared.TrimPaddedKey(string(keyBuffer))

	// read max key
	_, err = r.Read(keyBuffer)
	if err != nil {
		return fmt.Errorf("failed to deserialize max key: %v", err)
	}
	tm.MaxKey = shared.TrimPaddedKey(string(keyBuffer))

	return nil
}

func serializePairs(pairs []KVPair) []byte {
	buffer := bytes.NewBuffer(nil)

	// Write pairs
	for _, pair := range pairs {
		buffer.Write(shared.KeyToBytes(pair.Key))
		binary.Write(buffer, binary.LittleEndian, pair.Value.Offset)
		binary.Write(buffer, binary.LittleEndian, pair.Value.Size)
	}

	return buffer.Bytes()
}
