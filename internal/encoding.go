package internal

import (
	"bytes"
	"encoding/binary"

	"github.com/hasssanezzz/goldb/shared"
)

func (metadata *TableMetadata) Serialize(config *shared.EngineConfig) ([]byte, error) {
	buffer := bytes.NewBuffer(nil)

	isLevelAsByte := byte(0x00)
	if metadata.IsLevel {
		isLevelAsByte = byte(0xFF)
	}

	binary.Write(buffer, binary.LittleEndian, isLevelAsByte)
	binary.Write(buffer, binary.LittleEndian, metadata.Serial)
	binary.Write(buffer, binary.LittleEndian, metadata.Size)

	// Writing min key
	keyAsBytes, err := shared.KeyToBytes(metadata.MinKey, config.KeySize)
	if err != nil {
		return nil, err
	}
	buffer.Write(keyAsBytes)

	// Writing max key
	keyAsBytes, err = shared.KeyToBytes(metadata.MaxKey, config.KeySize)
	if err != nil {
		return nil, err
	}
	buffer.Write(keyAsBytes)

	return buffer.Bytes(), nil
}

func serializePairs(config *shared.EngineConfig, pairs []KVPair, metadata *TableMetadata) ([]byte, error) {
	serializedMetadata, err := metadata.Serialize(config)
	if err != nil {
		return nil, err
	}

	buffer := bytes.NewBuffer(serializedMetadata)

	// Write pairs
	for _, pair := range pairs {
		keyAsBytes, err := shared.KeyToBytes(pair.Key, config.KeySize)
		if err != nil {
			return nil, err
		}

		buffer.Write(keyAsBytes) // Key is fixed length
		binary.Write(buffer, binary.LittleEndian, pair.Value.Offset)
		binary.Write(buffer, binary.LittleEndian, pair.Value.Size)
	}

	return buffer.Bytes(), nil
}
