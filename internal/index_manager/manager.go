// Package index_manager manages the indexing and organization of SSTables and levels.
// It provides functionality for key lookups, flushing the memtable to disk as SSTables,
// and performing compaction to merge SSTables into levels.
//
// The package is inspired by LSM-tree principles, ensuring efficient write and read
// operations for key-value storage systems.
package index_manager

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/hasssanezzz/goldb/internal/memtable"
	"github.com/hasssanezzz/goldb/internal/shared"
)

// IndexManager handles the indexing of keys across the memtable, SSTables, and levels.
// It ensures that keys are efficiently located and manages the compaction process.
type IndexManager struct {
	Config     *shared.EngineConfig
	Memtable   *memtable.Table // In-memory AVL tree for temporary storage.
	currSerial int             // Current serial number for SSTables.
	lvlSerial  int             // Current serial number for levels.
	sstables   []*SSTable      // List of SSTables on disk.
	levels     []*SSTable      // List of levels (merged SSTables).
}

// New initializes a new IndexManager with the given homepath.
// It reads existing SSTables and levels from disk and prepares the memtable for writes.
// Returns an error if the directory cannot be accessed or if SSTables cannot be parsed.
func New(config *shared.EngineConfig) (*IndexManager, error) {
	im := &IndexManager{
		Config:     config,
		Memtable:   memtable.New(),
		currSerial: 1, // starting from one to reserve number zero
		lvlSerial:  1, // level 0 for SSTables only
	}

	err := im.ReadTables()
	if err != nil {
		return nil, err
	}

	err = im.ReadLevels()
	if err != nil {
		return nil, err
	}

	return im, nil
}

// ReadTables reads all SSTables from the directory and parses them into memory.
// It updates the list of SSTables and the current serial number.
// Returns an error if any SSTable cannot be parsed.
func (im *IndexManager) ReadTables() error {
	files, err := os.ReadDir(im.Config.Homepath)
	if err != nil {
		return err
	}

	for _, file := range files {
		name := file.Name()
		if !(len(name) > len(im.Config.SSTableNamePrefix) && name[:len(im.Config.SSTableNamePrefix)] == im.Config.SSTableNamePrefix) {
			continue
		}

		serial, err := strconv.Atoi(name[len(im.Config.SSTableNamePrefix):])
		if err != nil {
			continue
		}

		err = im.parseSSTable(serial, false)
		if err != nil {
			return fmt.Errorf("index manager can not parse sstable %d: %v", serial, err)
		}
		im.currSerial++
	}

	if len(im.sstables) > 1 {
		im.sortSSTablesBySerial()
	}

	return nil
}

// ReadLevels reads all levels from the directory and parses them into memory.
// It updates the list of levels and the level serial number.
// Returns an error if any level cannot be parsed.
func (im *IndexManager) ReadLevels() error {
	files, err := os.ReadDir(im.Config.Homepath)
	if err != nil {
		return err
	}

	for _, file := range files {
		name := file.Name()
		if !(len(name) > len(im.Config.LevelFileNamePrefix) && name[:len(im.Config.LevelFileNamePrefix)] == im.Config.LevelFileNamePrefix) {
			continue
		}

		serial, err := strconv.Atoi(name[len(im.Config.LevelFileNamePrefix):])
		if err != nil {
			continue
		}

		err = im.parseSSTable(serial, true)
		if err != nil {
			return fmt.Errorf("index manager can not parse level %d: %v", serial, err)
		}
		im.lvlSerial++
	}

	if len(im.sstables) > 1 {
		im.sortSSTablesBySerial()
	}

	return nil
}

// Get retrieves the IndexNode for the given key.
// It searches the memtable, SSTables, and levels in order of recency.
// Returns ErrKeyNotFound if the key does not exist.
func (im *IndexManager) Get(key string) (memtable.IndexNode, error) {

	// 1. search in the memtable
	if im.Memtable.Contains(key) {
		indexNode := im.Memtable.Get(key)
		if indexNode.Size == 0 {
			return memtable.IndexNode{}, &shared.ErrKeyNotFound{Key: key}
		}
		return indexNode, nil
	}

	// 2. search in the SSTables
	for _, table := range im.sstables {
		if table.Meta.MinKey > key || table.Meta.MaxKey < key {
			continue
		}

		result, err := table.BSearch(key)
		if err != nil {
			if _, ok := err.(*shared.ErrKeyRemoved); ok {
				return memtable.IndexNode{}, &shared.ErrKeyNotFound{Key: key}
			}
			if _, ok := err.(*shared.ErrKeyNotFound); !ok {
				return memtable.IndexNode{}, fmt.Errorf("index manager can not read key %q from sstable %d: %v", key, table.Meta.Serial, err)
			}
			continue
		}

		return result, nil
	}

	// 3. search in the levels
	for _, table := range im.levels {
		if table.Meta.MinKey > key || table.Meta.MaxKey < key {
			continue
		}

		result, err := table.BSearch(key)
		if err != nil {
			if _, ok := err.(*shared.ErrKeyRemoved); ok {
				return memtable.IndexNode{}, &shared.ErrKeyNotFound{Key: key}
			}
			if _, ok := err.(*shared.ErrKeyNotFound); !ok {
				return memtable.IndexNode{}, fmt.Errorf("index manager can not read key %q from sstable %d: %v", key, table.Meta.Serial, err)
			}
			continue
		}

		return result, nil
	}

	return memtable.IndexNode{}, &shared.ErrKeyNotFound{Key: key}
}

// Delete marks the given key as deleted in the memtable.
// The key will be removed during the next flush or compaction.
func (im *IndexManager) Delete(key string) {
	im.Memtable.Set(key, memtable.IndexNode{})
}

// Flush writes the contents of the memtable to disk as a new SSTable.
// It resets the memtable and updates the list of SSTables.
// Returns an error if the SSTable cannot be created or written.
func (im *IndexManager) Flush() error {
	path := filepath.Join(im.Config.Homepath, fmt.Sprintf(im.Config.SSTableNamePrefix+"%d", im.currSerial))
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	pairs := im.Memtable.Items()
	metadata := TableMetadata{
		Path:   path,
		Size:   uint32(len(pairs)),
		Serial: uint32(im.currSerial),
		MinKey: pairs[0].Key,
		MaxKey: pairs[len(pairs)-1].Key,
	}

	err = im.serializePairs(file, pairs, &metadata)
	if err != nil {
		return fmt.Errorf("index manager can not flush sstable %d: %v", im.currSerial, err)
	}

	// reset the memtable after successfully serializing it
	im.Memtable = memtable.New()

	newSSTable := NewSSTable(path, im.currSerial, im.Config)
	newSSTable.Meta = metadata
	newSSTable.Open()

	im.sstables = append(im.sstables, newSSTable)
	im.sortSSTablesBySerial()
	im.currSerial++

	log.Printf("index manager: flushed the memtable successfully, created new table %d", im.currSerial-1)

	return nil
}

// Keys returns a list of all keys in the database.
// It includes keys from the memtable, SSTables, and levels.
// Returns an error if any SSTable or level cannot be read.
func (im *IndexManager) Keys() ([]string, error) {
	// TODO get keys from levels
	final := map[string]struct{}{}

	for _, table := range im.sstables {
		keys, err := table.Keys()
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			final[key] = struct{}{}
		}
	}

	memtablePairs := im.Memtable.Items()
	for _, pair := range memtablePairs {
		final[pair.Key] = struct{}{}
	}

	results := []string{}
	for key := range final {
		results = append(results, key)
	}

	return results, nil
}

// Close closes all open SSTables and levels.
func (im *IndexManager) Close() error {
	for _, table := range im.sstables {
		if err := table.Close(); err != nil {
			return err
		}
	}

	for _, level := range im.levels {
		if err := level.Close(); err != nil {
			return err
		}
	}

	return nil
}

// CompactionCheck checks if the number of SSTables exceeds the threshold.
// If so, it triggers compaction to merge SSTables into a single level.
// Returns an error if compaction fails.
func (im *IndexManager) CompactionCheck() error {
	if len(im.sstables) <= int(im.Config.CompactionThreshold) {
		return nil
	}

	return im.createLevel()
}

// createLevel merges all SSTables into a single level and deletes the original SSTables.
// Returns an error if the level cannot be created or written.
func (im *IndexManager) createLevel() error {
	path := filepath.Join(im.Config.Homepath, fmt.Sprintf(im.Config.LevelFileNamePrefix+"%d", im.lvlSerial))
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	allPairs, err := im.getAllUniquePairs()
	if err != nil {
		return err
	}

	metadata := TableMetadata{
		Path:   path,
		Size:   uint32(len(allPairs)),
		Serial: uint32(im.lvlSerial),
		MinKey: allPairs[0].Key,
		MaxKey: allPairs[len(allPairs)-1].Key,
	}

	err = im.serializePairs(file, allPairs, &metadata)
	if err != nil {
		return fmt.Errorf("index manager can not flush sstable %d: %v", im.currSerial, err)
	}

	// create a new level
	level := NewSSTable(path, im.lvlSerial, im.Config)
	level.Meta = metadata
	level.Open()
	im.lvlSerial++
	im.levels = append(im.levels, level)

	// delete all sstables (danger)
	for _, table := range im.sstables {
		table.Close() // TODO handle closing errors
		err := os.Remove(table.Meta.Path)
		if err != nil {
			log.Printf("failed to remove sstable %d: %v", table.Meta.Serial, err)
			continue
		}
	}
	im.sstables = []*SSTable{}
	im.sortSSTablesBySerial()

	return nil
}

// getAllUniquePairs retrieves all unique key-value pairs from SSTables.
// It removes duplicates and deleted keys.
// Returns an error if any SSTable cannot be read.
func (im *IndexManager) getAllUniquePairs() ([]memtable.KVPair, error) {
	mp := map[string]*memtable.KVPair{}
	for _, table := range im.sstables {
		pairs, err := table.KVPairs()
		if err != nil {
			return nil, fmt.Errorf("compaction failed to read pairs of table %d: %v", table.Meta.Serial, err)
		}
		for _, pair := range pairs {
			// TODO urgent - check deleted keys
			if pair.Value.Size == 0 {
				continue
			}
			if _, ok := mp[pair.Key]; ok {
				continue
			}
			mp[pair.Key] = &pair
		}
	}

	pairs := make([]memtable.KVPair, len(mp))
	i := 0
	for _, pair := range mp {
		pairs[i] = *pair
	}

	sort.Sort(memtable.KVPairSlice(pairs))

	return pairs, nil
}

// parseSSTable reads an SSTable or level from disk and adds it to the list of SSTables.
// Returns an error if the SSTable cannot be parsed.
func (im *IndexManager) parseSSTable(serial int, isLevel bool) error {
	prefix := im.Config.SSTableNamePrefix
	if isLevel {
		prefix = im.Config.LevelFileNamePrefix
	}

	table := NewSSTable(filepath.Join(im.Config.Homepath, fmt.Sprintf("%s%d", prefix, serial)), serial, im.Config)
	err := table.Open()
	if err != nil {
		return err
	}
	im.sstables = append(im.sstables, table)

	readMsg := "table"
	if isLevel {
		readMsg = "level"
	}

	log.Printf("index manager: read %s %d with %d pairs\n", readMsg, table.Meta.Serial, table.Meta.Size)
	return nil
}

// serializePairs writes key-value pairs to disk in the SSTable format.
// Returns an error if the pairs cannot be written.
func (im *IndexManager) serializePairs(w io.Writer, pairs []memtable.KVPair, metadata *TableMetadata) error {
	// SSTable serial number
	err := binary.Write(w, binary.LittleEndian, metadata.Serial)
	if err != nil {
		return err
	}
	// pair count
	err = binary.Write(w, binary.LittleEndian, metadata.Size)
	if err != nil {
		return err
	}
	// write min and max keys
	keyAsBytes, err := shared.KeyToBytes(metadata.MinKey, im.Config.KeySize)
	if err != nil {
		return err
	}
	_, err = w.Write(keyAsBytes)
	if err != nil {
		return err
	}
	keyAsBytes, err = shared.KeyToBytes(metadata.MaxKey, im.Config.KeySize)
	if err != nil {
		return err
	}
	_, err = w.Write(keyAsBytes)
	if err != nil {
		return err
	}

	// write pairs
	for _, pair := range pairs {
		keyAsBytes, err := shared.KeyToBytes(pair.Key, im.Config.KeySize)
		if err != nil {
			return err
		}
		_, err = w.Write(keyAsBytes)
		if err != nil {
			return err
		}
		err = binary.Write(w, binary.LittleEndian, pair.Value.Offset)
		if err != nil {
			return err
		}
		err = binary.Write(w, binary.LittleEndian, pair.Value.Size)
		if err != nil {
			return err
		}
	}

	return nil
}

// sortSSTablesBySerial sorts the list of SSTables and levels by their serial numbers in descending order.
func (im *IndexManager) sortSSTablesBySerial() {
	sort.Slice(im.sstables, func(i, j int) bool {
		return im.sstables[i].Meta.Serial > im.sstables[j].Meta.Serial
	})

	sort.Slice(im.levels, func(i, j int) bool {
		return im.levels[i].Meta.Serial > im.levels[j].Meta.Serial
	})
}
