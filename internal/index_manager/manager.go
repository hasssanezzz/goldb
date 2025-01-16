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
	sstables   []*SSTable
	levels     []*SSTable
}

func New(homepath string) (*IndexManager, error) {
	im := &IndexManager{
		Memtable:   memtable.New(),
		path:       homepath,
		currSerial: 1, // starting from one to reserve number zero
	}

	err := im.ReadTables()
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

		err = im.parseSSTable(serial)
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

		err = im.parseSSTable(serial)
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

func (im *IndexManager) Get(key string) (memtable.IndexNode, error) {
	if im.Memtable.Contains(key) {
		indexNode := im.Memtable.Get(key)
		if indexNode.Size == 0 {
			return memtable.IndexNode{}, &shared.ErrKeyNotFound{Key: key}
		}
		return indexNode, nil
	}

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

	meta, err := im.serializeTree(file, uint32(im.currSerial), path)
	if err != nil {
		return fmt.Errorf("index manager can not flush sstable %d: %v", im.currSerial, err)
	}

	// reset the memtable after successfully serializing it
	im.Memtable = memtable.New()

	newSSTable := NewSSTable(path, im.currSerial)
	newSSTable.Meta = meta
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

	// ...

	return nil
}

func (im *IndexManager) parseSSTable(serial int) error {
	table := NewSSTable(filepath.Join(im.path, fmt.Sprintf("%s%d", shared.SSTableNamePrefix, serial)), serial)
	err := table.Open()
	if err != nil {
		return err
	}
	im.sstables = append(im.sstables, table)
	log.Printf("index manager: read table %d with %d pairs\n", table.Meta.Serial, table.Meta.Size)
	return nil
}

func (im *IndexManager) serializeTree(w io.Writer, serial uint32, path string) (TableMetadata, error) {
	pairs := im.Memtable.Items()
	metadata := TableMetadata{
		Path:   path,
		Size:   uint32(len(pairs)),
		Serial: serial,
		MinKey: pairs[0].Key,
		MaxKey: pairs[len(pairs)-1].Key,
	}

	// SSTable serial number
	err := binary.Write(w, binary.LittleEndian, serial)
	if err != nil {
		return TableMetadata{}, err
	}
	// pair count
	err = binary.Write(w, binary.LittleEndian, metadata.Size)
	if err != nil {
		return TableMetadata{}, err
	}
	// write min and max keys
	keyAsBytes, err := shared.KeyToBytes(metadata.MinKey)
	if err != nil {
		return TableMetadata{}, err
	}
	_, err = w.Write(keyAsBytes)
	if err != nil {
		return TableMetadata{}, err
	}
	keyAsBytes, err = shared.KeyToBytes(metadata.MaxKey)
	if err != nil {
		return TableMetadata{}, err
	}
	_, err = w.Write(keyAsBytes)
	if err != nil {
		return TableMetadata{}, err
	}

	// write pairs
	for _, pair := range pairs {
		keyAsBytes, err := shared.KeyToBytes(pair.Key)
		if err != nil {
			return TableMetadata{}, err
		}
		_, err = w.Write(keyAsBytes)
		if err != nil {
			return TableMetadata{}, err
		}
		err = binary.Write(w, binary.LittleEndian, pair.Value.Offset)
		if err != nil {
			return TableMetadata{}, err
		}
		err = binary.Write(w, binary.LittleEndian, pair.Value.Size)
		if err != nil {
			return TableMetadata{}, err
		}
	}

	return metadata, nil
}

func (im *IndexManager) sortSSTablesBySerial() {
	sort.Slice(im.sstables, func(i, j int) bool {
		return im.sstables[i].Meta.Serial > im.sstables[j].Meta.Serial
	})
}
