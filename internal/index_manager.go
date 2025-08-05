package internal

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

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
	wal        WAL

	mu             sync.RWMutex
	flushRequested chan struct{}
}

// NewIndexManager initializes a new IndexManager with the given homepath.
// It reads existing SSTables and levels from disk and prepares the memtable for writes.
// Returns an error if the directory cannot be accessed or if SSTables cannot be parsed.
func NewIndexManager(config *shared.EngineConfig, wal WAL) (*IndexManager, error) {
	im := &IndexManager{
		memtable:       NewAVLMemtable(),
		config:         config,
		currSerial:     1, // starting from one to reserve number zero
		lvlSerial:      1, // level 0 for SSTables only
		wal:            wal,
		flushRequested: make(chan struct{}),
	}

	if err := im.parseHomeDir(); err != nil {
		return nil, err
	}

	go im.backgroundFlusher()

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

	// Acquire read lock for accessing sstables/levels
	im.mu.RLock()
	defer im.mu.RUnlock()

	// 2. Search in the SSTables
	for _, table := range im.sstables {
		result, err := table.Search(key)
		if err != nil {
			var errKeyRemoved *shared.ErrKeyRemoved
			if errors.As(err, &errKeyRemoved) {
				return Position{}, &shared.ErrKeyNotFound{Key: key}
			}
			continue
		}

		return result, nil
	}

	// 3. Search in the levels
	for _, table := range im.levels {
		if table.metadata.MinKey > key || table.metadata.MaxKey < key {
			continue
		}

		result, err := table.Search(key)
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

func (im *IndexManager) Set(pair KVPair) {
	im.memtable.Set(pair)
}

// Keys returns a list of all keys in the database.
// It includes keys from the memtable, SSTables, and levels.
// Returns an error if any SSTable or level cannot be read.
func (im *IndexManager) Keys() ([]string, error) {
	im.mu.RLock()
	defer im.mu.RUnlock()

	// Use a map to store unique keys
	final := make(map[string]struct{})
	var finalMu sync.Mutex // Protects access to 'final'
	var wg sync.WaitGroup  // Waits for all goroutines to finish
	var firstError error   // Captures the first error encountered
	var errMu sync.Mutex   // Protects access to 'firstError'

	tables := append(im.sstables, im.levels...) // Combine SSTables and Levels

	for _, table := range tables {
		wg.Add(1)
		go func(t *SSTable) {
			defer wg.Done()
			keys, err := t.Keys()
			if err != nil {
				errMu.Lock()
				if firstError == nil {
					firstError = err
				}
				errMu.Unlock()
				return
			}
			finalMu.Lock()
			for _, key := range keys {
				final[key] = struct{}{}
			}
			finalMu.Unlock()
		}(table)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	errMu.Lock()
	if firstError != nil {
		errMu.Unlock()
		return nil, firstError
	}
	errMu.Unlock()

	// Add keys from the memtable (in-memory, likely fast, can be sequential)
	memtablePairs := im.memtable.Items()
	for _, pair := range memtablePairs {
		if pair.Value.Size == 0 {
			delete(final, pair.Key)
			continue
		}
		final[pair.Key] = struct{}{}
	}

	// Convert the map keys to a slice for the final result
	results := make([]string, 0, len(final))
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

func (im *IndexManager) backgroundFlusher() {
	for range im.flushRequested {
		im.mu.Lock()

		if err := im.flush(); err != nil {
			if im.config.Debug {
				log.Printf("IndexManager background flush failed: %v", err)
			}
		} else {
			if im.config.Debug {
				log.Printf("IndexManager background flush completed successfully.")
			}
		}
		im.mu.Unlock()
	}
}

// flush writes the contents of the memtable to disk as a new SSTable.
// It resets the memtable and updates the list of SSTables.
// Returns an error if the SSTable cannot be created or written.
func (im *IndexManager) flush() error {
	// Get all memtable items
	pairs := im.memtable.Items()

	// Initialize the new table's metadata
	metadata := TableMetadata{
		Path:    filepath.Join(im.config.Homepath, fmt.Sprintf(im.config.SSTableNamePrefix+"%d", im.currSerial)),
		IsLevel: false,
		Size:    uint32(len(pairs)),
		Serial:  uint32(im.currSerial),
		MinKey:  pairs[0].Key,
		MaxKey:  pairs[len(pairs)-1].Key,
	}

	// Create a new SSTable after successfully creating the physical one
	newSSTable, err := serializeSSTable(metadata, im.config, pairs)
	if err != nil {
		return fmt.Errorf("IndexManager.readTable failed to serialize table %q: %v", metadata.Path, err)
	}

	im.sstables = append(im.sstables, newSSTable)
	im.sortTablesBySerial()
	im.currSerial++

	// Reset the memtable after successfully serializing it
	im.memtable.Reset()

	log.Printf("IndexManager flushed new SSTable %d with %d pairs", im.currSerial-1, len(pairs))

	// TEMP disabling table compaction
	// return im.compactionCheck()
	return nil
}

// compactionCheck checks if the number of SSTables exceeds the threshold.
// If so, it triggers compaction to merge SSTables into a single level.
// Returns an error if compaction fails.
func (im *IndexManager) compactionCheck() error {
	if len(im.sstables) <= int(im.config.CompactionThreshold) {
		return nil
	}

	return im.createLevel()
}

func (im *IndexManager) readTable(filename string) error {
	// 1. create a new sstable
	fullPath := filepath.Join(im.config.Homepath, filename)
	table, err := deserializeSSTable(TableMetadata{Path: fullPath}, im.config)
	if err != nil {
		return fmt.Errorf("IndexManager.readTable failed to deserialize table %q: %v", filename, err)
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
	if im.config.Debug {
		log.Printf("index manager: read %s %d with %d pairs\n", filename, table.metadata.Serial, table.metadata.Size)
	}

	return nil
}

// createLevel merges all SSTables into a single level and deletes the original SSTables.
// Returns an error if the level cannot be created or written.
func (im *IndexManager) createLevel() error {
	allPairs, err := im.allItemsFromSSTables()
	if err != nil {
		return err
	}

	// Initialize the new table's metadata
	metadata := TableMetadata{
		Path:    filepath.Join(im.config.Homepath, fmt.Sprintf(im.config.LevelFileNamePrefix+"%d", im.lvlSerial)),
		IsLevel: true,
		Size:    uint32(len(allPairs)),
		Serial:  uint32(im.lvlSerial),
		MinKey:  allPairs[0].Key,
		MaxKey:  allPairs[len(allPairs)-1].Key,
	}

	// Create a new level
	level, err := serializeSSTable(metadata, im.config, allPairs)
	if err != nil {
		return fmt.Errorf("IndexManager.createLevel failed to create new level: %v", err)
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
	im.mu.Lock()
	defer im.mu.Unlock()

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
