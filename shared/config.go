package shared

const UintSize = 4

var DefaultConfig = EngineConfig{
	KeySize:               KeySize,
	MemtableSizeThreshold: 1000,
	CompactionThreshold:   10,
	SSTableNamePrefix:     "sst_",
	LevelFileNamePrefix:   "lvl_",
}

// EngineConfig defines the configuration parameters for the Goldb database engine.
// It allows customization of key sizes, memtable thresholds, file naming conventions, and compaction behavior.
type EngineConfig struct {
	KeySize               uint32 // Maximum size of a key in bytes.
	MemtableSizeThreshold uint32 // Maximum number of key-value pairs the memtable can hold before flushing to disk.
	CompactionThreshold   uint32 // Number of SSTables that if exceeded will trigger compaction.
	SSTableNamePrefix     string // Prefix for SSTable file names.
	LevelFileNamePrefix   string // Prefix for level file names.
	Homepath              string // Source directory
}

func NewEngineConfig() *EngineConfig {
	return &EngineConfig{
		KeySize:               DefaultConfig.KeySize,
		MemtableSizeThreshold: DefaultConfig.MemtableSizeThreshold,
		SSTableNamePrefix:     DefaultConfig.SSTableNamePrefix,
		LevelFileNamePrefix:   DefaultConfig.LevelFileNamePrefix,
		CompactionThreshold:   DefaultConfig.CompactionThreshold,
	}
}

func (ec *EngineConfig) WithKeySize(value uint32) *EngineConfig {
	ec.KeySize = value
	return ec
}

func (ec *EngineConfig) WithMemtableSizeThreshold(value uint32) *EngineConfig {
	ec.MemtableSizeThreshold = value
	return ec
}

func (ec *EngineConfig) WithCompactionThreshold(value uint32) *EngineConfig {
	ec.CompactionThreshold = value
	return ec
}

func (ec *EngineConfig) WithSSTableNamePrefix(value string) *EngineConfig {
	ec.SSTableNamePrefix = value
	return ec
}

func (ec *EngineConfig) WithLevelFileNamePrefix(value string) *EngineConfig {
	ec.LevelFileNamePrefix = value
	return ec
}

// GetMetadataSize calculates the size of the metadata section in an SSTable.
// The metadata includes the serial number, pair count, min key, and max key.
// Returns the total size in bytes.
func (ec *EngineConfig) GetMetadataSize() uint32 {
	// TODO: this is very wrong, if the metadata struct changes this will not be reflected
	return ec.KeySize*2 + UintSize*3 + 1
}

// GetKVPairSize calculates the size of a key-value pair in an SSTable.
// Each pair consists of a key, an offset, and a size.
// Returns the total size in bytes.
func (ec *EngineConfig) GetKVPairSize() uint32 {
	return ec.KeySize + UintSize*2 // "<key><offset><size>"
}

// GetSSTableExpectedSize calculates the expected size of an SSTable based on the configuration.
// It accounts for the metadata section and the key-value pairs stored in the SSTable.
// Returns the total size in bytes.
func (ec *EngineConfig) GetSSTableExpectedSize() uint32 {
	return ec.GetMetadataSize() + ec.MemtableSizeThreshold*ec.GetKVPairSize()
}
