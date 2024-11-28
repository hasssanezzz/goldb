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
	"strings"

	"github.com/hasssanezzz/goldb-engine/memtable"
)

const SSTableNamePrefix = "sst_"

func keyToBytes(key string) []byte {
	// TODO fix the 0X00 string thing
	keyByteLength := len([]byte(key))
	paddedKey := key + strings.Repeat(string(0x0), 256-keyByteLength)
	results := []byte(paddedKey)
	if len(results) != 256 {
		log.Panicf("key %q can not be padded", key)
	}
	return results
}

type IndexManager struct {
	Memtable   *memtable.Table
	path       string
	currSerial int
	sstables   []*SSTable
}

func New(homepath string) (*IndexManager, error) {
	im := &IndexManager{
		path:     homepath,
		Memtable: memtable.New(),
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
		if !(len(name) > 4 && name[:4] == SSTableNamePrefix) {
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
	for _, pair := range im.Memtable.Items() {
		fmt.Println(pair)
	}
	if im.Memtable.Contains(key) {
		indexNode := im.Memtable.Get(key)
		if indexNode.Size == 0 {
			return memtable.IndexNode{}, &ErrKeyNotFound{key}
		}
		return indexNode, nil
	}

	for _, table := range im.sstables {
		println("serial:", table.Meta.Serial)
		result, err := table.BSearch(key)
		if err != nil {
			if _, ok := err.(*ErrKeyRemoved); ok {
				return memtable.IndexNode{}, &ErrKeyNotFound{key}
			}
			if _, ok := err.(*ErrKeyNotFound); !ok {
				return memtable.IndexNode{}, fmt.Errorf("index manager can not read key %q from sstable %d: %v", key, table.Meta.Serial, err)
			}
			continue
		}

		return result, nil
	}

	return memtable.IndexNode{}, &ErrKeyNotFound{key}
}

func (im *IndexManager) Delete(key string) {
	im.Memtable.Set(key, memtable.IndexNode{})
}

func (im *IndexManager) Flush() error {
	path := filepath.Join(im.path, fmt.Sprintf(SSTableNamePrefix+"%d", im.currSerial))
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

	log.Printf("index manager: flushed a tree successfully, created new table %d", im.currSerial-1)

	return nil
}

func (im *IndexManager) Close() {
	for _, table := range im.sstables {
		table.Close()
	}
}

// private functions

func (im *IndexManager) parseSSTable(serial int) error {
	table := NewSSTable(filepath.Join(im.path, fmt.Sprintf("%s%d", SSTableNamePrefix, serial)), serial)
	err := table.Open()
	if err != nil {
		return err
	}
	im.sstables = append(im.sstables, table)
	log.Printf("index manager: read table %d with %d pairs\n", table.Meta.Serial, table.Meta.Size)
	return nil
}

func (im *IndexManager) serializeTree(w io.Writer, serial uint32, path string) (SSTableMetadata, error) {
	pairs := im.Memtable.Items()
	metadata := SSTableMetadata{
		Path:   path,
		Size:   uint32(len(pairs)),
		Serial: serial,
		MinKey: pairs[0].Key,
		MaxKey: pairs[len(pairs)-1].Key,
	}

	// SSTable serial number
	err := binary.Write(w, binary.LittleEndian, serial)
	if err != nil {
		return SSTableMetadata{}, err
	}
	// pair count
	err = binary.Write(w, binary.LittleEndian, metadata.Size)
	if err != nil {
		return SSTableMetadata{}, err
	}
	// write min and max keys
	_, err = w.Write(keyToBytes(metadata.MinKey))
	if err != nil {
		return SSTableMetadata{}, err
	}
	_, err = w.Write(keyToBytes(metadata.MaxKey))
	if err != nil {
		return SSTableMetadata{}, err
	}

	// write pairs
	for _, pair := range pairs {
		if pair.Value.Size == 0 {
			println("found deleted value")
		}
		_, err = w.Write(keyToBytes(pair.Key))
		if err != nil {
			return SSTableMetadata{}, err
		}
		err = binary.Write(w, binary.LittleEndian, pair.Value.Offset)
		if err != nil {
			return SSTableMetadata{}, err
		}
		err = binary.Write(w, binary.LittleEndian, pair.Value.Size)
		if err != nil {
			return SSTableMetadata{}, err
		}
	}

	return metadata, nil
}

func (im *IndexManager) sortSSTablesBySerial() {
	sort.Slice(im.sstables, func(i, j int) bool {
		return im.sstables[i].Meta.Serial > im.sstables[j].Meta.Serial
	})
}
