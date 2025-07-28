package internal

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/hasssanezzz/goldb/shared"
)

// IndexManager handles the indexing of keys across the memtable, SSTables, and levels.
// It ensures that keys are efficiently located and manages the compaction process.
type IndexManager struct {
	memtable   Memtable
	config     *shared.EngineConfig
	currSerial int        // Current serial number for SSTables.
	lvlSerial  int        // Current serial number for levels.
	sstables   []*SSTable // List of SSTables on disk.
	levels     []*SSTable // List of levels (merged SSTables).
}

// NewIndexManager initializes a new IndexManager with the given homepath.
// It reads existing SSTables and levels from disk and prepares the memtable for writes.
// Returns an error if the directory cannot be accessed or if SSTables cannot be parsed.
func NewIndexManager(config *shared.EngineConfig) (*IndexManager, error) {
	im := &IndexManager{
		memtable:   NewAVLMemtable(),
		config:     config,
		currSerial: 1, // starting from one to reserve number zero
		lvlSerial:  1, // level 0 for SSTables only
	}

	if err := im.parseHomeDir(); err != nil {
		return nil, err
	}

	return im, nil
}

// Get retrieves the IndexNode for the given key.
// It searches the memtable, SSTables, and levels in order of recency.
// Returns ErrKeyNotFound if the key does not exist.
func (im *IndexManager) Get(key string) (Position, error) {
	// 1. search in the memtable
	if im.memtable.Contains(key) {
		indexNode := im.memtable.Get(key)
		if indexNode.Size == 0 {
			return Position{}, &shared.ErrKeyNotFound{Key: key}
		}
		return indexNode, nil
	}

	// 2. search in the SSTables
	for _, table := range im.sstables {
		if table.metadata.MinKey > key || table.metadata.MaxKey < key {
			continue
		}

		result, err := table.BSearch(key)
		if err != nil {
			if _, ok := err.(*shared.ErrKeyRemoved); ok {
				return Position{}, &shared.ErrKeyNotFound{Key: key}
			}
			if _, ok := err.(*shared.ErrKeyNotFound); !ok {
				return Position{}, fmt.Errorf("index manager can not read key %q from sstable %d: %v", key, table.metadata.Serial, err)
			}
			continue
		}

		return result, nil
	}

	// 3. search in the levels
	for _, table := range im.levels {
		if table.metadata.MinKey > key || table.metadata.MaxKey < key {
			continue
		}

		result, err := table.BSearch(key)
		if err != nil {
			if _, ok := err.(*shared.ErrKeyRemoved); ok {
				return Position{}, &shared.ErrKeyNotFound{Key: key}
			}
			if _, ok := err.(*shared.ErrKeyNotFound); !ok {
				return Position{}, fmt.Errorf("index manager can not read key %q from sstable %d: %v", key, table.metadata.Serial, err)
			}
			continue
		}

		return result, nil
	}

	return Position{}, &shared.ErrKeyNotFound{Key: key}
}

// Delete marks the given key as deleted in the memtable.
// The key will be removed during the next flush or compaction.
func (im *IndexManager) Delete(key string) {
	im.memtable.Set(KVPair{Key: key})
}

// Flush writes the contents of the memtable to disk as a new SSTable.
// It resets the memtable and updates the list of SSTables.
// Returns an error if the SSTable cannot be created or written.
func (im *IndexManager) Flush() error {
	path := filepath.Join(im.config.Homepath, fmt.Sprintf(im.config.SSTableNamePrefix+"%d", im.currSerial))
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	// Initialize the new table's metadata
	pairs := im.memtable.Items()
	metadata := TableMetadata{
		Path:    path,
		IsLevel: false,
		Size:    uint32(len(pairs)),
		Serial:  uint32(im.currSerial),
		MinKey:  pairs[0].Key,
		MaxKey:  pairs[len(pairs)-1].Key,
	}

	// Write the serialized pairs
	if _, err := file.Write(serializePairs(pairs, &metadata)); err != nil {
		return fmt.Errorf("index manager failed to write serialized pairs %d: %v", im.currSerial, err)
	}

	// Reset the memtable after successfully serializing it
	im.memtable = NewAVLMemtable()

	// Create a logical SSTable after successfully creating the physical one
	newSSTable, err := NewSSTable(metadata, im.config)
	if err != nil {
		return err
	}
	im.sstables = append(im.sstables, newSSTable)
	im.sortTablesBySerial()
	im.currSerial++

	// REMOVE
	log.Printf("index manager: flushed the memtable successfully, created new table %d with %d pairs", im.currSerial-1, len(pairs))

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

	memtablePairs := im.memtable.Items()
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
	if len(im.sstables) <= int(im.config.CompactionThreshold) {
		return nil
	}

	return im.createLevel()
}

func (im *IndexManager) readTable(filename string) error {
	// 1. create a new sstable
	fullPath := filepath.Join(im.config.Homepath, filename)
	table, err := NewSSTable(TableMetadata{Path: fullPath}, im.config)
	if err != nil {
		return fmt.Errorf("index manager can not parse table %q: %v", filename, err)
	}

	// 2. add the table to the list
	if table.metadata.IsLevel {
		im.levels = append(im.levels, table)
		im.lvlSerial = max(im.lvlSerial, int(table.metadata.Serial))
	} else {
		im.sstables = append(im.sstables, table)
		im.currSerial = max(im.currSerial, int(table.metadata.Serial))
	}

	// 3. sort the tables
	im.sortTablesBySerial()

	// 4. do some logging
	log.Printf("index manager: read %s %d with %d pairs\n", filename, table.metadata.Serial, table.metadata.Size)

	return nil
}

// createLevel merges all SSTables into a single level and deletes the original SSTables.
// Returns an error if the level cannot be created or written.
func (im *IndexManager) createLevel() error {
	path := filepath.Join(im.config.Homepath, fmt.Sprintf(im.config.LevelFileNamePrefix+"%d", im.lvlSerial))
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	allPairs, err := im.allItemsFromSSTables()
	if err != nil {
		return err
	}

	metadata := TableMetadata{
		Path:    path,
		IsLevel: true,
		Size:    uint32(len(allPairs)),
		Serial:  uint32(im.lvlSerial),
		MinKey:  allPairs[0].Key,
		MaxKey:  allPairs[len(allPairs)-1].Key,
	}

	if _, err := file.Write(serializePairs(allPairs, &metadata)); err != nil {
		return fmt.Errorf("index manager can not flush sstable %d: %v", im.currSerial, err)
	}

	// Create a new level
	level, err := NewSSTable(metadata, im.config)
	if err != nil {
		return err
	}

	im.lvlSerial++
	im.levels = append(im.levels, level)

	// Delete all sstables (danger)
	for _, table := range im.sstables {
		table.Close() // TODO handle closing errors
		err := os.Remove(table.metadata.Path)
		if err != nil {
			log.Printf("failed to remove sstable %d: %v", table.metadata.Serial, err)
			continue
		}
	}

	im.sstables = []*SSTable{}
	im.sortTablesBySerial()

	return nil
}

// allItemsFromSSTables retrieves all unique key-value pairs from SSTables.
// It removes duplicates and deleted keys.
// Returns an error if any SSTable cannot be read.
func (im *IndexManager) allItemsFromSSTables() ([]KVPair, error) {
	mp := map[string]*KVPair{}
	for _, table := range im.sstables {
		items, err := table.Items()
		if err != nil {
			return nil, fmt.Errorf("allPairsFromSSTables failed to read pairs of table %d: %v", table.metadata.Serial, err)
		}
		for _, pair := range items {
			// TODO urgent - check deleted keys
			// if pair.Value.Size == 0 {
			// 	continue
			// }
			if _, ok := mp[pair.Key]; ok {
				continue
			}
			mp[pair.Key] = &pair
		}
	}

	pairs := make([]KVPair, len(mp))
	i := 0
	for _, pair := range mp {
		pairs[i] = *pair
	}

	sort.Sort(Pairs(pairs))

	return pairs, nil
}

// sortTablesBySerial sorts the list of SSTables and levels by their serial numbers in descending order.
func (im *IndexManager) sortTablesBySerial() {
	sort.Slice(im.sstables, func(i, j int) bool {
		return im.sstables[i].metadata.Serial > im.sstables[j].metadata.Serial
	})

	sort.Slice(im.levels, func(i, j int) bool {
		return im.levels[i].metadata.Serial > im.levels[j].metadata.Serial
	})
}

func (im *IndexManager) parseHomeDir() error {
	files, err := os.ReadDir(im.config.Homepath)
	if err != nil {
		return err
	}

	for _, file := range files {
		name := file.Name()

		if strings.HasPrefix(name, im.config.SSTableNamePrefix) || strings.HasPrefix(name, im.config.LevelFileNamePrefix) {
			err := im.readTable(name)
			if err != nil {
				log.Printf("index manager: failed to parse file %q: %v\n", name, err)
			}
		}
	}

	return nil
}
