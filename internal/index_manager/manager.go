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

type IndexManager struct {
	Memtable   *memtable.Table
	path       string
	currSerial int
	lvlSerial  int
	sstables   []*SSTable
	levels     []*SSTable // a level is just a big sstable
}

func New(homepath string) (*IndexManager, error) {
	im := &IndexManager{
		Memtable:   memtable.New(),
		path:       homepath,
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

func (im *IndexManager) ReadTables() error {
	files, err := os.ReadDir(im.path)
	if err != nil {
		return err
	}

	for _, file := range files {
		name := file.Name()
		if !(len(name) > 4 && name[:4] == shared.SSTableNamePrefix) {
			continue
		}

		serial, err := strconv.Atoi(name[4:])
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

func (im *IndexManager) ReadLevels() error {
	files, err := os.ReadDir(im.path)
	if err != nil {
		return err
	}

	for _, file := range files {
		name := file.Name()
		if !(len(name) > 4 && name[:4] == shared.LevelFileNamePrefix) {
			continue
		}

		serial, err := strconv.Atoi(name[4:])
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

func (im *IndexManager) Delete(key string) {
	im.Memtable.Set(key, memtable.IndexNode{})
}

func (im *IndexManager) Flush() error {
	path := filepath.Join(im.path, fmt.Sprintf(shared.SSTableNamePrefix+"%d", im.currSerial))
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

	newSSTable := NewSSTable(path, im.currSerial)
	newSSTable.Meta = metadata
	newSSTable.Open()

	im.sstables = append(im.sstables, newSSTable)
	im.sortSSTablesBySerial()
	im.currSerial++

	log.Printf("index manager: flushed the memtable successfully, created new table %d", im.currSerial-1)

	return nil
}

func (im *IndexManager) Keys(pattern string) ([]string, error) {
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

func (im *IndexManager) Close() {
	for _, table := range im.sstables {
		table.Close()
	}
}

func (im *IndexManager) CompactionCheck() error {
	if len(im.sstables) <= shared.MaxSSTableCount {
		return nil
	}

	return im.createLevel()
}

func (im *IndexManager) createLevel() error {
	path := filepath.Join(im.path, fmt.Sprintf(shared.LevelFileNamePrefix+"%d", im.lvlSerial))
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
	level := NewSSTable(path, im.lvlSerial)
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

func (im *IndexManager) getAllUniquePairs() ([]memtable.KVPair, error) {
	mp := map[string]*memtable.KVPair{}
	for _, table := range im.sstables {
		pairs, err := table.KVPairs()
		if err != nil {
			return nil, fmt.Errorf("compaction failed to read pairs of table %d: %v", table.Meta.Serial, err)
		}
		for _, pair := range pairs {
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

func (im *IndexManager) parseSSTable(serial int, isLevel bool) error {
	prefix := shared.SSTableNamePrefix
	if isLevel {
		prefix = shared.LevelFileNamePrefix
	}

	table := NewSSTable(filepath.Join(im.path, fmt.Sprintf("%s%d", prefix, serial)), serial)
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
	keyAsBytes, err := shared.KeyToBytes(metadata.MinKey)
	if err != nil {
		return err
	}
	_, err = w.Write(keyAsBytes)
	if err != nil {
		return err
	}
	keyAsBytes, err = shared.KeyToBytes(metadata.MaxKey)
	if err != nil {
		return err
	}
	_, err = w.Write(keyAsBytes)
	if err != nil {
		return err
	}

	// write pairs
	for _, pair := range pairs {
		keyAsBytes, err := shared.KeyToBytes(pair.Key)
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

func (im *IndexManager) sortSSTablesBySerial() {
	sort.Slice(im.sstables, func(i, j int) bool {
		return im.sstables[i].Meta.Serial > im.sstables[j].Meta.Serial
	})

	sort.Slice(im.levels, func(i, j int) bool {
		return im.levels[i].Meta.Serial > im.levels[j].Meta.Serial
	})
}
