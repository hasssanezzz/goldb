package index_manager

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/hasssanezzz/goldb-engine/memtable"
)

const MetadataSize = 256*2 + 4*2 // two keys + two number
const KVPairSize = 256 + 4*2

type SSTableMetadata struct {
	Path   string
	Serial uint32
	Size   uint32
	MinKey string
	MaxKey string
}

type SSTable struct {
	Meta SSTableMetadata
	file *os.File
}

func NewSSTable(path string, serial int) *SSTable {
	table := &SSTable{}
	table.Meta.Path = path
	table.Meta.Serial = uint32(serial)
	return table
}

func (s *SSTable) Open() error {
	file, err := os.Open(s.Meta.Path)
	if err != nil {
		return fmt.Errorf("can not open sstable %q: %v", s.Meta.Path, err)
	}
	s.file = file
	s.ParseMetadata()
	return nil
}

func (s *SSTable) ParseMetadata() error {
	buf := make([]byte, 4)
	key := make([]byte, 256)

	// read serial
	_, err := s.file.Read(buf)
	if err != nil {
		return fmt.Errorf("can not read metadata from sstable %q: %v", s.Meta.Path, err)
	}
	s.Meta.Serial = binary.LittleEndian.Uint32(buf)

	// read pair count
	_, err = s.file.Read(buf)
	if err != nil {
		return fmt.Errorf("can not read metadata from sstable %q: %v", s.Meta.Path, err)
	}
	s.Meta.Size = binary.LittleEndian.Uint32(buf)

	// read min key
	_, err = s.file.Read(key)
	if err != nil {
		return fmt.Errorf("can not read metadata from sstable %q: %v", s.Meta.Path, err)
	}
	s.Meta.MinKey = string(key)

	// read max key
	_, err = s.file.Read(key)
	if err != nil {
		return fmt.Errorf("can not read metadata from sstable %q: %v", s.Meta.Path, err)
	}
	s.Meta.MaxKey = string(key)

	return nil
}

func (s *SSTable) BSearch(key string) (memtable.IndexNode, error) {
	key = string(keyToBytes(key)) // I'm just a cs student
	left, right := 0, int(s.Meta.Size-1)
	for left <= right {
		mid := left + (right-left)/2
		pair, err := s.nthKey(mid)
		if err != nil {
			return memtable.IndexNode{}, fmt.Errorf("sstable %q can not perform bsearch gettting the %dth key: %v", s.Meta.Path, mid, err)
		}

		if pair.Key < key {
			left = mid + 1
		} else if pair.Key > key {
			right = mid - 1
		} else {
			if pair.Value.Size == 0 {
				return memtable.IndexNode{}, &ErrKeyRemoved{Key: key}
			} else {
				return pair.Value, nil
			}
		}
	}

	return memtable.IndexNode{}, &ErrKeyNotFound{Key: key}
}

func (s *SSTable) Close() error {
	return s.file.Close()
}

func (s *SSTable) nthKey(n int) (memtable.KVPair, error) {
	position := int64(MetadataSize + n*KVPairSize)
	// fmt.Printf("trying to access the %dth item pos: %d\n", n, position)
	_, err := s.file.Seek(position, io.SeekStart)
	if err != nil {
		return memtable.KVPair{}, fmt.Errorf("sstable %q can not seek position %d: %v", s.Meta.Path, position, err)
	}

	keyBuffer := make([]byte, 256)
	numberBuffer := make([]byte, 4)

	// read key string
	_, err = s.file.Read(keyBuffer)
	if err != nil {
		return memtable.KVPair{}, err
	}

	_, err = s.file.Read(numberBuffer)
	if err != nil {
		return memtable.KVPair{}, err
	}
	offset := binary.LittleEndian.Uint32(numberBuffer)

	_, err = s.file.Read(numberBuffer)
	if err != nil {
		return memtable.KVPair{}, err
	}
	size := binary.LittleEndian.Uint32(numberBuffer)

	return memtable.KVPair{
		Key: string(keyBuffer),
		Value: memtable.IndexNode{
			Offset: offset,
			Size:   size,
		},
	}, nil
}
