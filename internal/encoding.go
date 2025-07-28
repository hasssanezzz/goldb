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
	buffer.Write(shared.KeyToBytes(tm.MinKey))
	buffer.Write(shared.KeyToBytes(tm.MaxKey))

	return buffer.Bytes()
}

func (tm *TableMetadata) Deserialize(r io.Reader) error {
	result := TableMetadata{}

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
		return fmt.Errorf("failed to deserialize metadata: %v", err)
	}
	tm.Serial = binary.LittleEndian.Uint32(uintBuffer)

	// read pair count
	_, err = r.Read(uintBuffer)
	if err != nil {
		return fmt.Errorf("failed to deserialize metadata: %v", err)
	}
	tm.Size = binary.LittleEndian.Uint32(uintBuffer)

	// read min key
	_, err = r.Read(keyBuffer)
	if err != nil {
		return fmt.Errorf("failed to deserialize metadata: %v", err)
	}
	tm.MinKey = shared.TrimPaddedKey(string(keyBuffer))

	// read max key
	_, err = r.Read(keyBuffer)
	if err != nil {
		return fmt.Errorf("failed to deserialize metadata: %v", err)
	}
	tm.MaxKey = shared.TrimPaddedKey(string(keyBuffer))

	tm = &result
	return nil
}

func serializePairs(pairs []KVPair, metadata *TableMetadata) []byte {
	serializedMetadata := metadata.Serialize()
	buffer := bytes.NewBuffer(serializedMetadata)

	// Write pairs
	for _, pair := range pairs {
		buffer.Write(shared.KeyToBytes(pair.Key))
		binary.Write(buffer, binary.LittleEndian, pair.Value.Offset)
		binary.Write(buffer, binary.LittleEndian, pair.Value.Size)
	}

	return buffer.Bytes()
}
