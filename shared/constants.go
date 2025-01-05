package shared

import "fmt"

const KeyByteLength = 256
const MetadataSize = KeyByteLength*2 + 4*2
const KVPairSize = KeyByteLength + 4*2
const MemtableSizeThreshold = 500 // for now
const SSTableExpectedSize = MetadataSize + MemtableSizeThreshold*KVPairSize
const SSTableNamePrefix = "sst_"
const LevelFileNamePrefix = "lvl_"
const MaxSSTableCount = 10 // for now

type ErrKeyTooLong struct{ Key string }

func (e *ErrKeyTooLong) Error() string {
	return fmt.Sprintf("key %q must be less than %d", e.Key, KeyByteLength)
}

type ErrKeyNotFound struct{ Key string }

func (e *ErrKeyNotFound) Error() string {
	return fmt.Sprintf("key %q can not be found", e.Key)
}

type ErrKeyRemoved struct{ Key string }

func (e *ErrKeyRemoved) Error() string {
	return fmt.Sprintf("key %q is deleted", e.Key)
}
